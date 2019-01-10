/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package historycouchdb

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"

	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/kvledger/history/historydb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	coreledgerutil "github.com/hyperledger/fabric/core/ledger/util"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/hyperledger/fabric/protos/common"
	putils "github.com/hyperledger/fabric/protos/utils"
	"github.com/pkg/errors"
)

const (
	// Suffix for our CouchDB database' name.
	// Avoids naming conflicts with other databases that may reside in the same CouchDB instance.
	dbNameSuffix = "history"
	// ID of the block height CouchDB doc
	heightDocIdKey = "height"
	// Key of the block-height document that will hold the last committed block's number
	heightDocBlockNumKey = "block_number"
	// Key of the block-height document that will hold the number of transactions in the last committed block
	heightDocTrxNumKey = "trx_number"
	// Name of CouchDB index for the writeset we will be storing
	writesetIndexName = "writeset-index"
	// Name of CouchDB index for sorting the writesets
	sortIndexName = "writeset-sort-index"
	// Name of design document for our CouchDB index
	writesetIndexDesignDoc = "historydb"
)

var logger = flogging.MustGetLogger("historycouchdb")

// Returns the CouchDB definition provided in the peer's yaml config.
func GetProductionCouchDBDefinition() *couchdb.CouchDBDef {
	return couchdb.GetCouchDBDefinition()
}

// Implementation of the historydb.HistoryDBProvider interface
type historyDBProvider struct {
	couchDBInstance *couchdb.CouchInstance
}

// Implementation of the historydb.HistoryDB interface
type historyDB struct {
	// CouchDB client
	couchDB *couchdb.CouchDatabase
	// CouchDB document revision for the savepoint
	savepointRev string
}

// Model for write-sets metadata.
type writeSet struct {
	// Namespace of the key written to
	Namespace string
	// Key
	Key string
	// Block number in which the transaction is recorded
	BlockNum uint64
	// Transaction number within the block
	TrxNum uint64
}

// Array of writeSets
type writeSets []writeSet

// Export this writeSet as a CouchDB doc.
func (ws writeSet) asCouchDoc() (*couchdb.CouchDoc, error) {
	bytes, err := json.Marshal(ws)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to marshal writeSet with values: %+v", ws)
	}
	return &couchdb.CouchDoc{JSONValue: bytes}, nil
}

// Export these writeSets as CouchDB docs.
func (sets writeSets) asCouchDbDocs() ([]*couchdb.CouchDoc, error) {
	var docs []*couchdb.CouchDoc
	for _, write := range sets {
		doc, err := write.asCouchDoc()
		if err != nil {
			return nil, errors.Wrapf(err, "failed to export this writeSet as a CouchDB doc: %+v", write)
		}
		docs = append(docs, doc)
	}
	return docs, nil
}

// NewHistoryDBProvider instantiates historyDBProvider
// For normal uses the caller should provide the CouchDB definition from GetProductionCouchDBDefinition()
func NewHistoryDBProvider(couchDBDef *couchdb.CouchDBDef) (historydb.HistoryDBProvider, error) {
	logger.Debugf("constructing CouchDB historyDB storage provider")
	couchInstance, err := couchdb.CreateCouchInstance(couchDBDef.URL, couchDBDef.Username, couchDBDef.Password,
		couchDBDef.MaxRetries, couchDBDef.MaxRetriesOnStartup, couchDBDef.RequestTimeout, couchDBDef.CreateGlobalChangesDB)
	if err != nil {
		return nil, errors.WithMessage(err, "obtaining CouchDB HistoryDB provider failed")
	}
	return &historyDBProvider{couchDBInstance: couchInstance}, nil
}

// GetDBHandle gets the handle to a named database
func (provider *historyDBProvider) GetDBHandle(dbName string) (historydb.HistoryDB, error) {
	database, err := createCouchDatabase(provider.couchDBInstance, couchdb.ConstructBlockchainDBName(dbName, dbNameSuffix))
	if err != nil {
		return nil, errors.WithMessage(err, "obtaining handle on CouchDB HistoryDB failed")
	}
	_, rev, err := database.ReadDoc(heightDocIdKey)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get CouchDB document revision for savepoint with _id [%s]", heightDocIdKey)
	}
	logger.Debugf("CouchDB document revision for savepoint: %s", rev)
	return &historyDB{couchDB: database, savepointRev: rev}, nil
}

func createCouchDatabase(couchInstance *couchdb.CouchInstance, dbName string) (*couchdb.CouchDatabase, error) {
	if ledgerconfig.IsCommitter() {
		return createCouchDatabaseCommitter(couchInstance, dbName)
	}

	return createCouchDatabaseEndorser(couchInstance, dbName)
}

func createCouchDatabaseCommitter(couchInstance *couchdb.CouchInstance, dbName string) (*couchdb.CouchDatabase, error) {
	db, err := couchdb.CreateCouchDatabase(couchInstance, dbName)
	if err != nil {
		return nil, err
	}

	err = createIndexes(db)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func createCouchDatabaseEndorser(couchInstance *couchdb.CouchInstance, dbName string) (*couchdb.CouchDatabase, error) {
	db, err := couchdb.NewCouchDatabase(couchInstance, dbName)
	if err != nil {
		return nil, err
	}

	dbExists, err := db.ExistsWithRetry()
	if err != nil {
		return nil, err
	}

	if !dbExists {
		return nil, errors.Errorf("DB not found: [%s]", db.DBName)
	}

	indexExists, err := db.IndexDesignDocExistsWithRetry(writesetIndexDesignDoc)
	if err != nil {
		return nil, err
	}
	if !indexExists {
		return nil, errors.Errorf("DB index not found: [%s]", db.DBName)
	}

	return db, nil
}

// Close closes the underlying db
func (provider *historyDBProvider) Close() {
	// do nothing
}

// Creates indexes if they don't exist already.
func createIndexes(couchDB *couchdb.CouchDatabase) error {
	writesetIdx := fmt.Sprintf(`
		{
			"index": {
				"fields": ["Namespace", "Key"]
			},
			"name": "%s",
			"ddoc": "%s",
			"type": "json"
		}`, writesetIndexName, writesetIndexDesignDoc)
	sortIdx := fmt.Sprintf(`
		{
			"index": {
				"fields": ["BlockNum", "TrxNum"]
			},
			"name": "%s",
			"ddoc": "%s",
			"type": "json"
		}
	`, sortIndexName, writesetIndexDesignDoc)
	for _, index := range []string{writesetIdx, sortIdx} {
		// CreateIndex() does not return an error when the index already exists. Instead,
		// CreateIndexResponse.Result will have the value "exists".
		// Therefore we assume that CouchDB is safely handling any race conditions with the
		// creation of this index even if this API does not return an error.
		if _, err := couchDB.CreateIndex(index); err != nil {
			return errors.Wrapf(err, "failed to create this CouchDB index for HistoryDB: [%s]", index)
		}
	}
	return nil
}

// NewHistoryQueryExecutor implements method in HistoryDB interface
func (historyDB *historyDB) NewHistoryQueryExecutor(blockStore blkstorage.BlockStore) (ledger.HistoryQueryExecutor, error) {
	return &queryExecutor{
		blockStore: blockStore,
		couchDB:    historyDB.couchDB,
	}, nil
}

// Commit implements method in HistoryDB interface
func (historyDB *historyDB) Commit(block *common.Block) error {
	savepoint, err := historyDB.GetLastSavepoint()
	if err != nil {
		return err
	}
	// Only save blocks with greater height than the last savepoint
	if savepoint == nil || savepoint.BlockNum < block.GetHeader().GetNumber() {
		// We're only interested in writes from valid and endorsed transactions
		writes, err := getWriteSetsFromEndorsedTrxs(historyDB.couchDB.DBName, block)
		if err != nil {
			return err
		}
		docs, err := writes.asCouchDbDocs()
		if err != nil {
			return errors.Wrapf(err, "failed to convert write-sets to CouchDB documents")
		}
		// Create CouchDB doc savepoint
		heightDoc, err := newSavepointDoc(newHeight(block), historyDB.savepointRev)
		if err != nil {
			return errors.Wrapf(err, "failed to construct a new savepoint for historycouchdb")
		}
		docs = append(docs, heightDoc)
		// Save write-sets + savepoint to CouchDB
		results, err := historyDB.couchDB.CommitDocuments(docs)
		if err != nil {
			return errors.Wrapf(err, "failed to save history batch to CouchDB")
		}

		savepointRev, ok := results[heightDocIdKey]
		if !ok {
			return errors.New("save point revision was not found")
		}
		historyDB.savepointRev = savepointRev

		logger.Debugf(
			"Channel [%s]: Updates committed to history database for blockNo [%v]",
			historyDB.couchDB.DBName, block.Header.Number,
		)
	}
	return nil
}

// GetBlockNumFromSavepoint implements method in HistoryDB interface
func (historyDB *historyDB) GetLastSavepoint() (*version.Height, error) {
	doc, _, err := historyDB.couchDB.ReadDoc(heightDocIdKey)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get last savepoint with id [%s] from CouchDB", heightDocIdKey)
	}
	if doc == nil {
		return nil, nil
	}
	docMap := make(map[string]string)
	if err := json.Unmarshal(doc.JSONValue, &docMap); err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal savepoint with id [%s] from CouchDB", heightDocIdKey)
	}
	blockNum, err := strconv.ParseUint(docMap[heightDocBlockNumKey], 10, 64)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse block number from CouchDB savepoint")
	}
	trxNum, err := strconv.ParseUint(docMap[heightDocTrxNumKey], 10, 64)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse trx number from CouchDB savepoint")
	}
	return version.NewHeight(blockNum, trxNum), nil
}

// ShouldRecover implements method in interface historydb.HistoryDB
func (historyDB *historyDB) ShouldRecover(lastAvailableBlock uint64) (bool, uint64, error) {
	if !ledgerconfig.IsHistoryDBEnabled() {
		return false, 0, nil
	}
	savepoint, err := historyDB.GetLastSavepoint()
	if err != nil {
		return false, 0, err
	}
	if savepoint == nil {
		return true, 0, nil
	}
	return savepoint.BlockNum != lastAvailableBlock, savepoint.BlockNum + 1, nil
}

// CommitLostBlock implements method in interface historydb.HistoryDB
func (historyDB *historyDB) CommitLostBlock(blockAndPvtdata *ledger.BlockAndPvtData) error {
	return historyDB.Commit(blockAndPvtdata.Block)
}

// Returns a new CouchDB savepoint document for the new savepoint.
// The document's revision will be set only if 'rev' is not empty.
func newSavepointDoc(height *version.Height, rev string) (*couchdb.CouchDoc, error) {
	doc := make(map[string]interface{})
	doc["_id"] = heightDocIdKey
	if rev != "" {
		doc["_rev"] = rev
	}
	doc[heightDocBlockNumKey] = fmt.Sprintf("%d", height.BlockNum)
	doc[heightDocTrxNumKey] = fmt.Sprintf("%d", height.TxNum)
	bytes, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}
	return &couchdb.CouchDoc{JSONValue: bytes}, nil
}

// Returns the block's height.
func newHeight(block *common.Block) *version.Height {
	return version.NewHeight(
		block.Header.Number,
		uint64(len(block.Data.Data)),
	)
}

// Returns write-sets from valid and endorsed transactions.
func getWriteSetsFromEndorsedTrxs(channel string, block *common.Block) (writeSets, error) {
	writes := writeSets{}
	var tranNo uint64
	logger.Debugf(
		"Channel [%s]: Updating history database for blockNo [%v] with [%d] transactions",
		channel, block.Header.Number, len(block.Data.Data),
	)
	// Get the invalidation byte array for the block
	txsFilter := coreledgerutil.TxValidationFlags(
		block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER],
	)
	for _, envBytes := range block.Data.Data {
		// Skip invalid transactions
		if txsFilter.IsInvalid(int(tranNo)) {
			logger.Debugf(
				"Channel [%s]: Skipping history write for invalid transaction number %d",
				channel, tranNo,
			)
			tranNo++
			continue
		}
		envelope, err := putils.GetEnvelopeFromBlock(envBytes)
		if err != nil {
			return nil, err
		}
		payload, err := putils.GetPayload(envelope)
		if err != nil {
			return nil, err
		}
		header, err := putils.UnmarshalChannelHeader(payload.Header.ChannelHeader)
		if err != nil {
			return nil, err
		}
		if common.HeaderType(header.Type) == common.HeaderType_ENDORSER_TRANSACTION {
			action, err := putils.GetActionFromEnvelope(envBytes)
			if err != nil {
				return nil, err
			}
			txRWSet := &rwsetutil.TxRwSet{}
			if err = txRWSet.FromProtoBytes(action.Results); err != nil {
				return nil, err
			}
			for _, nsRWSet := range txRWSet.NsRwSets {
				for _, kvWrite := range nsRWSet.KvRwSet.Writes {
					writes = append(writes, writeSet{nsRWSet.NameSpace, kvWrite.Key, block.Header.Number, tranNo})
				}
			}

		} else {
			logger.Debugf("Skipping transaction [%d] since it is not an endorsement transaction\n", tranNo)
		}
		tranNo++
	}
	return writes, nil
}
