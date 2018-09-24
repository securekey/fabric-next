/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package historycouchdb

import (
	"fmt"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/kvledger/history/historydb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/protos/common"
)

var logger = flogging.MustGetLogger("historycouchdb")

// HistoryDBProvider implements interface HistoryDBProvider
type HistoryDBProvider struct {
	// TODO
}

// NewHistoryDBProvider instantiates HistoryDBProvider
func NewHistoryDBProvider() *HistoryDBProvider {
	// TODO
	return &HistoryDBProvider{}
}

// GetDBHandle gets the handle to a named database
func (provider *HistoryDBProvider) GetDBHandle(dbName string) (historydb.HistoryDB, error) {
	// TODO
	return newHistoryDB(), nil
}

// Close closes the underlying db
func (provider *HistoryDBProvider) Close() {
	panic("Not implemented")
}

// historyDB implements HistoryDB interface
type historyDB struct {
	dbName string
}

// newHistoryDB constructs an instance of HistoryDB
func newHistoryDB() *historyDB {
	return &historyDB{}
}

// NewHistoryQueryExecutor implements method in HistoryDB interface
func (historyDB *historyDB) NewHistoryQueryExecutor(blockStore blkstorage.BlockStore) (ledger.HistoryQueryExecutor, error) {
	return nil, fmt.Errorf("Not implemented")
}

// Commit implements method in HistoryDB interface
func (historyDB *historyDB) Commit(block *common.Block) error {

	// Get the history batch from the block, including the savepoint
	//historyBatch, err := historydb.ConstructHistoryBatch(historyDB.dbName, block)
	_, err := historydb.ConstructHistoryBatch(historyDB.dbName, block)
	if err != nil {
		return err
	}

	// Move the batch to LevelDB batch
	// TODO

	// write the block's history records and savepoint to LevelDB
	// Setting snyc to true as a precaution, false may be an ok optimization after further testing.
	// TODO

	logger.Debugf("Channel [%s]: Updates committed to history database for blockNo [%v]", historyDB.dbName, block.Header.Number)
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
