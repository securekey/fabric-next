/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package historycouchdb

import (
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
func NewHistoryDBProvider() (*historyDBProvider, error) {
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
	database, err := couchdb.CreateCouchDatabase(provider.couchDBInstance, dbName)
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
	return fmt.Errorf("Not implemented")
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
