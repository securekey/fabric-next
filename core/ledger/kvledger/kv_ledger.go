/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package kvledger

import (
	"fmt"
	"sync"

	"github.com/hyperledger/fabric/common/flogging"
	commonledger "github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/common/metrics"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/cceventmgmt"
	"github.com/hyperledger/fabric/core/ledger/confighistory"
	"github.com/hyperledger/fabric/core/ledger/kvledger/bookkeeping"
	"github.com/hyperledger/fabric/core/ledger/kvledger/history/historydb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/privacyenabledstate"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb/kvcache"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/txmgr"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/txmgr/lockbasedtxmgr"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/core/ledger/ledgerstorage"
	"github.com/hyperledger/fabric/core/ledger/pvtdatapolicy"
	ledgerutil "github.com/hyperledger/fabric/core/ledger/util"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

var logger = flogging.MustGetLogger("kvledger")

// KVLedger provides an implementation of `ledger.PeerLedger`.
// This implementation provides a key-value based data model
type kvLedger struct {
	ledgerID               string
	blockStore             *ledgerstorage.Store
	txtmgmt                txmgr.LockBasedTxMgr
	historyDB              historydb.HistoryDB
	versionedDB            privacyenabledstate.DB
	kvCacheProvider        *kvcache.KVCacheProvider
	configHistoryRetriever ledger.ConfigHistoryRetriever
	blockAPIsRWLock        *sync.RWMutex
	bcInfo                 *common.BlockchainInfo

	commitCh        chan *ledger.BlockAndPvtData
	indexCh         chan *indexUpdate
	stoppedCommitCh chan struct{}
	stoppedIndexCh  chan struct{}
	doneCh          chan struct{}
}

// NewKVLedger constructs new `KVLedger`
func newKVLedger(
	ledgerID string,
	blockStore *ledgerstorage.Store,
	versionedDB privacyenabledstate.DB,
	historyDB historydb.HistoryDB,
	configHistoryMgr confighistory.Mgr,
	stateListeners []ledger.StateListener,
	bookkeeperProvider bookkeeping.Provider) (*kvLedger, error) {

	logger.Debugf("Creating KVLedger ledgerID=%s: ", ledgerID)
	stateListeners = append(stateListeners, configHistoryMgr)
	// Create a kvLedger for this chain/ledger, which encasulates the underlying
	// id store, blockstore, txmgr (state database), history database
	l := &kvLedger{
		ledgerID:        ledgerID,
		blockStore:      blockStore,
		historyDB:       historyDB,
		versionedDB:     versionedDB,
		kvCacheProvider: versionedDB.GetKVCacheProvider(),
		blockAPIsRWLock: &sync.RWMutex{},
		commitCh:        make(chan *ledger.BlockAndPvtData),
		indexCh:         make(chan *indexUpdate),
		stoppedCommitCh: make(chan struct{}),
		stoppedIndexCh:  make(chan struct{}),
		doneCh:          make(chan struct{}),
	}

	// TODO Move the function `GetChaincodeEventListener` to ledger interface and
	// this functionality of regiserting for events to ledgermgmt package so that this
	// is reused across other future ledger implementations
	ccEventListener := versionedDB.GetChaincodeEventListener()
	logger.Debugf("Register state db for chaincode lifecycle events: %t", ccEventListener != nil)
	if ccEventListener != nil {
		cceventmgmt.GetMgr().Register(ledgerID, ccEventListener)
	}
	btlPolicy := pvtdatapolicy.NewBTLPolicy(l)
	if err := l.initTxMgr(versionedDB, stateListeners, btlPolicy, bookkeeperProvider); err != nil {
		return nil, err
	}
	l.initBlockStore(btlPolicy)
	if ledgerconfig.IsCommitter() {
		//Recover both state DB and history DB if they are out of sync with block storage
		if err := l.recoverDBs(); err != nil {
			panic(fmt.Errorf(`Error during state DB recovery:%s`, err))
		}
	}
	l.configHistoryRetriever = configHistoryMgr.GetRetriever(ledgerID, l)

	// pre populate non durable private data cache
	var err error
	l.bcInfo, err = blockStore.GetBlockchainInfo()
	if err != nil {
		logger.Warningf("Skipping pre populate of non-durable private data cache due to failed fetching BlockChainInfo [ledgerID:%s] - error : %s", ledgerID, err)
	} else {
		err = l.populateNonDurablePvtCache(l.bcInfo.Height)
		if err != nil {
			logger.Warningf("Skipping pre populate of non-durable private data cache on new KV ledger because it failed [ledgerID:%s] - error : %s", ledgerID, err)
		}
	}

	go l.commitWatcher(btlPolicy)
	go l.indexWriter()

	return l, nil
}

func (l *kvLedger) initTxMgr(versionedDB privacyenabledstate.DB, stateListeners []ledger.StateListener,
	btlPolicy pvtdatapolicy.BTLPolicy, bookkeeperProvider bookkeeping.Provider) error {
	var err error
	l.txtmgmt, err = lockbasedtxmgr.NewLockBasedTxMgr(l.ledgerID, versionedDB, stateListeners, btlPolicy, bookkeeperProvider)
	return err
}

func (l *kvLedger) initBlockStore(btlPolicy pvtdatapolicy.BTLPolicy) {
	l.blockStore.Init(btlPolicy)
}

//Recover the state database and history database (if exist)
//by recommitting last valid blocks
func (l *kvLedger) recoverDBs() error {
	logger.Debugf("Entering recoverDB()")
	//If there is no block in blockstorage, nothing to recover.
	info, _ := l.blockStore.GetBlockchainInfo()
	if info.Height == 0 {
		logger.Debug("Block storage is empty.")
		return nil
	}
	lastAvailableBlockNum := info.Height - 1
	recoverables := []recoverable{l.txtmgmt, l.historyDB}
	recoverers := []*recoverer{}
	for _, recoverable := range recoverables {
		recoverFlag, firstBlockNum, err := recoverable.ShouldRecover(lastAvailableBlockNum)
		if err != nil {
			return err
		}
		if recoverFlag {
			recoverers = append(recoverers, &recoverer{firstBlockNum, recoverable})
		}
	}
	if len(recoverers) == 0 {
		return nil
	}
	if len(recoverers) == 1 {
		return l.recommitLostBlocks(recoverers[0].firstBlockNum, lastAvailableBlockNum, recoverers[0].recoverable)
	}

	// both dbs need to be recovered
	if recoverers[0].firstBlockNum > recoverers[1].firstBlockNum {
		// swap (put the lagger db at 0 index)
		recoverers[0], recoverers[1] = recoverers[1], recoverers[0]
	}
	if recoverers[0].firstBlockNum != recoverers[1].firstBlockNum {
		// bring the lagger db equal to the other db
		if err := l.recommitLostBlocks(recoverers[0].firstBlockNum, recoverers[1].firstBlockNum-1,
			recoverers[0].recoverable); err != nil {
			return err
		}
	}
	// get both the db upto block storage
	return l.recommitLostBlocks(recoverers[1].firstBlockNum, lastAvailableBlockNum,
		recoverers[0].recoverable, recoverers[1].recoverable)
}

//recommitLostBlocks retrieves blocks in specified range and commit the write set to either
//state DB or history DB or both
func (l *kvLedger) recommitLostBlocks(firstBlockNum uint64, lastBlockNum uint64, recoverables ...recoverable) error {
	var err error
	var blockAndPvtdata *ledger.BlockAndPvtData
	for blockNumber := firstBlockNum; blockNumber <= lastBlockNum; blockNumber++ {
		if blockAndPvtdata, err = l.GetPvtDataAndBlockByNum(blockNumber, nil); err != nil {
			return err
		}
		for _, r := range recoverables {
			if err := r.CommitLostBlock(blockAndPvtdata); err != nil {
				return err
			}
		}
	}
	return nil
}

//populateNonDurablePvtCache will populate non durable private data cache
func (l *kvLedger) populateNonDurablePvtCache(lastBlockNum uint64) error {
	if lastBlockNum <= 0 {
		logger.Debugf("block number is zero, nothing to do for non-durable cache")
		return nil
	}

	var initBlockNum = lastBlockNum - ledgerconfig.GetKVCacheBlocksToLive()
	if initBlockNum < 0 {
		initBlockNum = 0
	}

	var err error
	var blockAndPvtdata *ledger.BlockAndPvtData
	for blockNumber := initBlockNum; blockNumber <= lastBlockNum; blockNumber++ {
		if blockAndPvtdata, err = l.GetPvtDataAndBlockByNum(blockNumber, nil); err != nil {
			return err
		}
		err = l.cacheNonDurableBlock(blockAndPvtdata)
		if err != nil {
			return err
		}
	}
	return nil
}

// GetTransactionByID retrieves a transaction by id
func (l *kvLedger) GetTransactionByID(txID string, hints ...ledger.SearchHint) (*peer.ProcessedTransaction, error) {
	tranEnv, err := l.blockStore.RetrieveTxByID(txID, hints...)
	if err != nil {
		return nil, err
	}
	txVResult, err := l.blockStore.RetrieveTxValidationCodeByTxID(txID)
	if err != nil {
		return nil, err
	}
	processedTran := &peer.ProcessedTransaction{TransactionEnvelope: tranEnv, ValidationCode: int32(txVResult)}
	l.blockAPIsRWLock.RLock()
	l.blockAPIsRWLock.RUnlock()
	return processedTran, nil
}

// GetBlockchainInfo returns basic info about blockchain
func (l *kvLedger) GetBlockchainInfo() (*common.BlockchainInfo, error) {
	l.blockAPIsRWLock.RLock()
	defer l.blockAPIsRWLock.RUnlock()
	return l.bcInfo, nil
}

// GetBlockByNumber returns block at a given height
// blockNumber of  math.MaxUint64 will return last block
func (l *kvLedger) GetBlockByNumber(blockNumber uint64) (*common.Block, error) {
	block, err := l.blockStore.RetrieveBlockByNumber(blockNumber)
	l.blockAPIsRWLock.RLock()
	l.blockAPIsRWLock.RUnlock()
	return block, err
}

func (l *kvLedger) AddBlock(pvtdataAndBlock *ledger.BlockAndPvtData) error {

	logger.Debugf("[%s] Adding block [%d] to storage", l.ledgerID, pvtdataAndBlock.Block.Header.Number)
	err := l.blockStore.AddBlock(pvtdataAndBlock.Block)
	if err != nil {
		return err
	}

	logger.Debugf("[%s] Adding block [%d] transactions to state cache", l.ledgerID, pvtdataAndBlock.Block.Header.Number)

	l.blockAPIsRWLock.Lock()
	indexUpdate, err := l.cacheBlock(pvtdataAndBlock)
	if err != nil {
		l.blockAPIsRWLock.Unlock()
		return err
	}
	//update local block chain info
	err = l.blockStore.CheckpointBlock(pvtdataAndBlock.Block)
	if err != nil {
		l.blockAPIsRWLock.Unlock()
		return err
	}

	l.blockAPIsRWLock.Unlock()

	l.indexCh <- indexUpdate

	return nil
}

// GetBlocksIterator returns an iterator that starts from `startBlockNumber`(inclusive).
// The iterator is a blocking iterator i.e., it blocks till the next block gets available in the ledger
// ResultsIterator contains type BlockHolder
func (l *kvLedger) GetBlocksIterator(startBlockNumber uint64) (commonledger.ResultsIterator, error) {
	blkItr, err := l.blockStore.RetrieveBlocks(startBlockNumber)
	if err != nil {
		return nil, err
	}
	return &blocksItr{l.blockAPIsRWLock, blkItr}, nil
}

// GetBlockByHash returns a block given it's hash
func (l *kvLedger) GetBlockByHash(blockHash []byte) (*common.Block, error) {
	block, err := l.blockStore.RetrieveBlockByHash(blockHash)
	l.blockAPIsRWLock.RLock()
	l.blockAPIsRWLock.RUnlock()
	return block, err
}

// GetBlockByTxID returns a block which contains a transaction
func (l *kvLedger) GetBlockByTxID(txID string) (*common.Block, error) {
	block, err := l.blockStore.RetrieveBlockByTxID(txID)
	l.blockAPIsRWLock.RLock()
	l.blockAPIsRWLock.RUnlock()
	return block, err
}

func (l *kvLedger) GetTxValidationCodeByTxID(txID string) (peer.TxValidationCode, error) {
	txValidationCode, err := l.blockStore.RetrieveTxValidationCodeByTxID(txID)
	l.blockAPIsRWLock.RLock()
	l.blockAPIsRWLock.RUnlock()
	return txValidationCode, err
}

//Prune prunes the blocks/transactions that satisfy the given policy
func (l *kvLedger) Prune(policy commonledger.PrunePolicy) error {
	return errors.New("Not yet implemented")
}

// NewTxSimulator returns new `ledger.TxSimulator`
func (l *kvLedger) NewTxSimulator(txid string) (ledger.TxSimulator, error) {
	return l.txtmgmt.NewTxSimulator(txid)
}

// NewQueryExecutor gives handle to a query executor.
// A client can obtain more than one 'QueryExecutor's for parallel execution.
// Any synchronization should be performed at the implementation level if required
func (l *kvLedger) NewQueryExecutor() (ledger.QueryExecutor, error) {
	return l.txtmgmt.NewQueryExecutor(util.GenerateUUID())
}

// GetState return state from db
func (l *kvLedger) GetState(namespace string, key string) ([]byte, error) {
	vv, err := l.txtmgmt.GetDB().GetState(namespace, key)
	if err != nil {
		return nil, err
	}
	if vv != nil {
		return vv.Value, nil
	}
	return nil, nil
}

// NewHistoryQueryExecutor gives handle to a history query executor.
// A client can obtain more than one 'HistoryQueryExecutor's for parallel execution.
// Any synchronization should be performed at the implementation level if required
// Pass the ledger blockstore so that historical values can be looked up from the chain
func (l *kvLedger) NewHistoryQueryExecutor() (ledger.HistoryQueryExecutor, error) {
	return l.historyDB.NewHistoryQueryExecutor(l.blockStore)
}

// CommitWithPvtData commits the block and the corresponding pvt data in an atomic operation
func (l *kvLedger) CommitWithPvtData(pvtdataAndBlock *ledger.BlockAndPvtData) error {
	stopWatch := metrics.StopWatch("kvledger_CommitWithPvtData_duration")
	defer stopWatch()

	l.blockAPIsRWLock.Lock()
	indexUpdate, err := l.cacheBlock(pvtdataAndBlock)
	if err != nil {
		l.blockAPIsRWLock.Unlock()
		panic(fmt.Errorf("block was not cached [%s]", err))
	}
	l.blockAPIsRWLock.Unlock()

	l.indexCh <- indexUpdate
	l.commitCh <- pvtdataAndBlock

	return nil
}

func (l *kvLedger) commitWithPvtData(pvtdataAndBlock *ledger.BlockAndPvtData) error {
	stopWatch := metrics.StopWatch("kvledger_CommitWithPvtData_worker_duration")
	defer stopWatch()

	block := pvtdataAndBlock.Block
	blockNo := pvtdataAndBlock.Block.Header.Number

	logger.Debugf("[%s] Committing block [%d] to storage", l.ledgerID, blockNo)

	if err := l.blockStore.CommitWithPvtData(pvtdataAndBlock); err != nil {
		return errors.WithMessage(err, `Error during commit to block store`)
	}

	logger.Debugf("[%s] Committing block [%d] transactions to state database", l.ledgerID, blockNo)
	if err := l.txtmgmt.Commit(pvtdataAndBlock.Block.Header.Number); err != nil {
		return errors.WithMessage(err, `Error during commit to txmgr`)
	}

	// History database could be written in parallel with state and/or async as a future optimization,
	// although it has not been a bottleneck...no need to clutter the log with elapsed duration.
	if ledgerconfig.IsHistoryDBEnabled() {
		logger.Debugf("[%s] Committing block [%d] transactions to history database", l.ledgerID, blockNo)
		if err := l.historyDB.Commit(block); err != nil {
			return errors.WithMessage(err, `Error during commit to history db`)
		}
	}

	// Set the checkpoint now that all of the data has been successfully committed
	if err := l.blockStore.CheckpointBlock(block); err != nil {
		return errors.WithMessage(err, `Error during checkpoint`)
	}

	return nil
}

// ValidateMVCC validates block for MVCC conflicts and phantom reads against committed data
func (l *kvLedger) ValidateMVCC(ctx context.Context, block *common.Block, txFlags ledgerutil.TxValidationFlags, filter ledgerutil.TxFilter) error {
	return l.txtmgmt.ValidateMVCC(ctx, block, txFlags, filter)
}

// ValidateBlockWithPvtData validate commit with pvt data
func (l *kvLedger) ValidateBlockWithPvtData(pvtdataAndBlock *ledger.BlockAndPvtData) error {
	return l.txtmgmt.ValidateAndPrepare(pvtdataAndBlock, true)
}

// GetPvtDataAndBlockByNum returns the block and the corresponding pvt data.
// The pvt data is filtered by the list of 'collections' supplied
func (l *kvLedger) GetPvtDataAndBlockByNum(blockNum uint64, filter ledger.PvtNsCollFilter) (*ledger.BlockAndPvtData, error) {
	stopWatch := metrics.StopWatch("kvledger_GetPvtDataAndBlockByNum_duration")
	defer stopWatch()

	blockAndPvtdata, err := l.blockStore.GetPvtDataAndBlockByNum(blockNum, filter)
	l.blockAPIsRWLock.RLock()
	l.blockAPIsRWLock.RUnlock()
	return blockAndPvtdata, err
}

// GetPvtDataByNum returns only the pvt data  corresponding to the given block number
// The pvt data is filtered by the list of 'collections' supplied
func (l *kvLedger) GetPvtDataByNum(blockNum uint64, filter ledger.PvtNsCollFilter) ([]*ledger.TxPvtData, error) {
	stopWatch := metrics.StopWatch("kvledger_GetPvtDataByNum_duration")
	defer stopWatch()

	pvtdata, err := l.blockStore.GetPvtDataByNum(blockNum, filter)
	l.blockAPIsRWLock.RLock()
	l.blockAPIsRWLock.RUnlock()
	return pvtdata, err
}

// Purge removes private read-writes set generated by endorsers at block height lesser than
// a given maxBlockNumToRetain. In other words, Purge only retains private read-write sets
// that were generated at block height of maxBlockNumToRetain or higher.
func (l *kvLedger) PurgePrivateData(maxBlockNumToRetain uint64) error {
	return fmt.Errorf("not yet implemented")
}

// PrivateDataMinBlockNum returns the lowest retained endorsement block height
func (l *kvLedger) PrivateDataMinBlockNum() (uint64, error) {
	return 0, fmt.Errorf("not yet implemented")
}

func (l *kvLedger) GetConfigHistoryRetriever() (ledger.ConfigHistoryRetriever, error) {
	return l.configHistoryRetriever, nil
}

// Close closes `KVLedger`
func (l *kvLedger) Close() {

	close(l.doneCh)
	<-l.stoppedCommitCh
	<-l.stoppedIndexCh

	l.blockStore.Shutdown()
	l.txtmgmt.Shutdown()
}

type blocksItr struct {
	blockAPIsRWLock *sync.RWMutex
	blocksItr       commonledger.ResultsIterator
}

func (itr *blocksItr) Next() (commonledger.QueryResult, error) {
	block, err := itr.blocksItr.Next()
	if err != nil {
		return nil, err
	}
	itr.blockAPIsRWLock.RLock()
	itr.blockAPIsRWLock.RUnlock()
	return block, nil
}

func (itr *blocksItr) Close() {
	itr.blocksItr.Close()
}
