/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package historycouchdb

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

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
)

var logger = flogging.MustGetLogger("historycouchdb")

// Returns the CouchDB definition provided in the peer's yaml config.
func GetProductionCouchDBDefinition() *couchdb.CouchDBDef {
	return couchdb.GetCouchDBDefinition()
}

// historyDBProvider implements interface historydb.HistoryDBProvider
type historyDBProvider struct {
	couchDBInstance *couchdb.CouchInstance
}

// NewHistoryDBProvider instantiates historyDBProvider
// For normal uses the caller should provide the CouchDB definition from GetProductionCouchDBDefinition()
func NewHistoryDBProvider(couchDBDef *couchdb.CouchDBDef) (historydb.HistoryDBProvider, error) {
	logger.Debugf("constructing CouchDB historyDB storage provider")
	couchInstance, err := couchdb.CreateCouchInstance(couchDBDef.URL, couchDBDef.Username, couchDBDef.Password,
		couchDBDef.MaxRetries, couchDBDef.MaxRetriesOnStartup, couchDBDef.RequestTimeout)
	if err != nil {
		return nil, errors.WithMessage(err, "obtaining CouchDB HistoryDB provider failed")
	}
	return &historyDBProvider{couchDBInstance: couchInstance}, nil
}

// GetDBHandle gets the handle to a named database
func (provider *historyDBProvider) GetDBHandle(dbName string) (historydb.HistoryDB, error) {
	database, err := couchdb.CreateCouchDatabase(provider.couchDBInstance, couchdb.ConstructBlockchainDBName(dbName, dbNameSuffix))
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

// Close closes the underlying db
func (provider *historyDBProvider) Close() {
	panic("Not implemented")
}

// historyDB implements HistoryDB interface
type historyDB struct {
	// CouchDB client
	couchDB *couchdb.CouchDatabase
	// CouchDB document revision for the savepoint
	savepointRev string
}

// NewHistoryQueryExecutor implements method in HistoryDB interface
func (historyDB *historyDB) NewHistoryQueryExecutor(blockStore blkstorage.BlockStore) (ledger.HistoryQueryExecutor, error) {
	return nil, fmt.Errorf("Not implemented")
}

// Commit implements method in HistoryDB interface
func (historyDB *historyDB) Commit(block *common.Block) error {
	// We're only interested in writes from valid and endorsed transactions
	keys, err := getModifiedKeysFromEndorsedTrxs(historyDB.couchDB.DBName, block)
	if err != nil {
		return err
	}
	docs, err := keysToCouchDocs(keys)
	if err != nil {
		return err
	}
	// Create CouchDB doc savepoint
	heightDoc, err := newSavepointDoc(newHeight(block), historyDB.savepointRev)
	if err != nil {
		return err
	}
	docs = append(docs, heightDoc)
	// Save to CouchDB
	results, err := historyDB.couchDB.BatchUpdateDocuments(docs)
	if err != nil {
		return err
	}
	// Save the savepoint's new revision for the next commit
	for _, result := range results {
		if heightDocIdKey == result.ID {
			historyDB.savepointRev = result.Rev
			break
		}
	}
	logger.Debugf(
		"Channel [%s]: Updates committed to history database for blockNo [%v]",
		historyDB.couchDB.DBName, block.Header.Number,
	)
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

// ShouldRecover implements method in interface kvledger.Recoverer
func (historyDB *historyDB) ShouldRecover(lastAvailableBlock uint64) (bool, uint64, error) {
	return false, 0, fmt.Errorf("Not implemented")
}

// CommitLostBlock implements method in interface kvledger.Recoverer
func (historyDB *historyDB) CommitLostBlock(blockAndPvtdata *ledger.BlockAndPvtData) error {
	return fmt.Errorf("Not implemented")
}

// Converts the given keys into CouchDB docs
func keysToCouchDocs(keys []string) ([]*couchdb.CouchDoc, error) {
	var docs []*couchdb.CouchDoc
	for _, key := range keys {
		doc := make(map[string]interface{})
		doc["_id"] = key
		bytes, err := json.Marshal(doc)
		if err != nil {
			return nil, errors.Wrapf(err, "error marshalling write [%s] to json", key)
		}
		docs = append(docs, &couchdb.CouchDoc{JSONValue: bytes})
	}
	return docs, nil
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

// Returns the set of modified keys from valid and endorsed transactions.
func getModifiedKeysFromEndorsedTrxs(channel string, block *common.Block) ([]string, error) {
	keys := []string{}
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
					keys = append(keys, composeKey(nsRWSet.NameSpace, kvWrite.Key, block.Header.Number, tranNo))
				}
			}

		} else {
			logger.Debugf("Skipping transaction [%d] since it is not an endorsement transaction\n", tranNo)
		}
		tranNo++
	}
	return keys, nil
}

// Builds a composite key of the form "namespace-key-blockNum-trxNum" for use as CouchDB _id.
// This operation and decomposeKey() are symmetrical.
// We choose to include the block number and transaction number in order to make the key unique and
// create new CouchDB documents when saving modifications for the same namespace-key combination.
// This avoids loss of data due to CouchDB compaction.
func composeKey(namespace, key string, blockNum, trxNum uint64) string {
	return fmt.Sprintf("%s-%s-%d-%d", namespace, key, blockNum, trxNum)
}

// Decomposes a key built using composeKey() into its ordered parts.
// This operation and composeKey() are symmetrical.
func decomposeKey(composite string) (namespace, key string, height *version.Height, err error) {
	parts := strings.Split(composite, "-")
	if len(parts) != 4 {
		return "", "", nil, errors.New(fmt.Sprintf("[%s] does not match the expected format for composite keys: namespace-key-blockNum-trxNum", composite))
	}
	blockNum, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return "", "", nil, errors.Wrapf(err, "failed to parse block number from composite key [%s]", composite)
	}
	trxNum, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil {
		return "", "", nil, errors.Wrapf(err, "failed to parse transaction number from composite key [%s]", composite)
	}
	return parts[0], parts[1], version.NewHeight(blockNum, trxNum), nil
}
