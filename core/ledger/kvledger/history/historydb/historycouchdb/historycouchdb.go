/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package historycouchdb

import (
	"encoding/json"
	"fmt"

	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/pkg/errors"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/kvledger/history/historydb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/protos/common"
)

var logger = flogging.MustGetLogger("historycouchdb")

// historyDBProvider implements interface historydb.HistoryDBProvider
type historyDBProvider struct {
	couchDBInstance *couchdb.CouchInstance
}

// NewHistoryDBProvider instantiates historyDBProvider
func NewHistoryDBProvider() (historydb.HistoryDBProvider, error) {
	logger.Debugf("constructing CouchDB historyDB storage provider")
	couchDBDef := couchdb.GetCouchDBDefinition()
	couchInstance, err := couchdb.CreateCouchInstance(couchDBDef.URL, couchDBDef.Username, couchDBDef.Password,
		couchDBDef.MaxRetries, couchDBDef.MaxRetriesOnStartup, couchDBDef.RequestTimeout)
	if err != nil {
		return nil, errors.WithMessage(err, "obtaining CouchDB HistoryDB provider failed")
	}
	return &historyDBProvider{couchDBInstance: couchInstance}, nil
}

// GetDBHandle gets the handle to a named database
func (provider *historyDBProvider) GetDBHandle(dbName string) (historydb.HistoryDB, error) {
	database, err := couchdb.CreateCouchDatabase(provider.couchDBInstance, couchdb.ConstructBlockchainDBName(dbName, "history"))
	if err != nil {
		return nil, errors.WithMessage(err, "obtaining handle on CouchDB HistoryDB failed")
	}
	return &historyDB{couchDB: database}, nil
}

// Close closes the underlying db
func (provider *historyDBProvider) Close() {
	panic("Not implemented")
}

// historyDB implements HistoryDB interface
type historyDB struct {
	couchDB *couchdb.CouchDatabase
}

// NewHistoryQueryExecutor implements method in HistoryDB interface
func (historyDB *historyDB) NewHistoryQueryExecutor(blockStore blkstorage.BlockStore) (ledger.HistoryQueryExecutor, error) {
	return nil, fmt.Errorf("Not implemented")
}

// Commit implements method in HistoryDB interface
func (historyDB *historyDB) Commit(block *common.Block) error {
	// Get the history batch from the block
	batch, height, err := historydb.ConstructHistoryBatch(historyDB.couchDB.DBName, block)
	if err != nil {
		return err
	}

	// Convert batch writes to CouchDB docs
	docs, err := historyBatchToCouchDocs(batch)
	if err != nil {
		return err
	}

	// Create CouchDB doc savepoint
	heightDoc, err := newHeightDoc(height)
	if err != nil {
		return err
	}
	docs = append(docs, heightDoc)

	// Insert history batch. Update save-point doc.
	_, err = historyDB.couchDB.BatchUpdateDocuments(docs)
	if err != nil {
		return err
	}

	logger.Debugf("Channel [%s]: Updates committed to history database for blockNo [%v]", historyDB.couchDB.DBName, block.Header.Number)
	return nil
}

// GetBlockNumFromSavepoint implements method in HistoryDB interface
func (historyDB *historyDB) GetLastSavepoint() (*version.Height, error) {
	return nil, fmt.Errorf("Not implemented")
}

// ShouldRecover implements method in interface kvledger.Recoverer
func (historyDB *historyDB) ShouldRecover(lastAvailableBlock uint64) (bool, uint64, error) {
	return false, 0, fmt.Errorf("Not implemented")
}

// CommitLostBlock implements method in interface kvledger.Recoverer
func (historyDB *historyDB) CommitLostBlock(blockAndPvtdata *ledger.BlockAndPvtData) error {
	return fmt.Errorf("Not implemented")
}

// Converts batch writes to CouchDB docs
func historyBatchToCouchDocs(batch [][]byte) ([]*couchdb.CouchDoc, error) {
	var docs []*couchdb.CouchDoc

	for _, write := range batch {
		doc := make(map[string]interface{})
		doc["_id"] = string(write)

		bytes, err := json.Marshal(doc)
		if err != nil {
			return nil, errors.Wrapf(err, "error marshalling write [%s] to json", string(write))
		}

		docs = append(docs, &couchdb.CouchDoc{JSONValue: bytes})
	}

	return docs, nil
}

// Returns a new CouchDB doc for the height ("save-point")
func newHeightDoc(height *version.Height) (*couchdb.CouchDoc, error) {
	doc := make(map[string]interface{})
	doc["height"] = string(height.ToBytes())
	bytes, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}
	return &couchdb.CouchDoc{JSONValue: bytes}, nil
}
