/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package kvledger

import (
	"sync"

	"github.com/hyperledger/fabric/common/flogging"
	commonledger "github.com/hyperledger/fabric/common/ledger"
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
	"fmt"
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
	stats                  *ledgerStats
	bcInfo                 *common.BlockchainInfo
	btlPolicy              pvtdatapolicy.BTLPolicy

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
	bookkeeperProvider bookkeeping.Provider,
	ccInfoProvider ledger.DeployedChaincodeInfoProvider,
	stats *ledgerStats,
) (*kvLedger, error) {
	logger.Debugf("Creating KVLedger ledgerID=%s: ", ledgerID)
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
	btlPolicy := pvtdatapolicy.ConstructBTLPolicy(&collectionInfoRetriever{l, ccInfoProvider})
	if err := l.initTxMgr(versionedDB, stateListeners, btlPolicy, bookkeeperProvider, ccInfoProvider); err != nil {
		return nil, err
	}
	l.initBlockStore(btlPolicy)
	l.btlPolicy = btlPolicy
	if ledgerconfig.IsCommitter() {
		//Recover both state DB and history DB if they are out of sync with block storage
		if err := l.recoverDBs(); err != nil {
			panic(errors.WithMessage(err, "error during state DB recovery"))
		}
	}
	l.configHistoryRetriever = configHistoryMgr.GetRetriever(ledgerID, l)

	// pre populate non durable private data cache
	var err error
	l.bcInfo, err = blockStore.GetBlockchainInfo()
	if err != nil {
		logger.Warningf("Skipping pre populate of non-durable private data cache due to failed fetching BlockChainInfo [ledgerID:%s] - error : %s", ledgerID, err)
		return nil, err
	} else {
		err = l.populateNonDurablePvtCache(l.bcInfo.Height)
		if err != nil {
			logger.Warningf("Skipping pre populate of non-durable private data cache on new KV ledger because it failed [ledgerID:%s] - error : %s", ledgerID, err)
		}
	}

	go l.commitWatcher(btlPolicy)
	go l.indexWriter()

	// initialize stat with the current height
	stats.updateBlockchainHeight(l.bcInfo.Height)
	l.stats = stats
	return l, nil
}

func (l *kvLedger) initTxMgr(versionedDB privacyenabledstate.DB, stateListeners []ledger.StateListener,
	btlPolicy pvtdatapolicy.BTLPolicy, bookkeeperProvider bookkeeping.Provider, ccInfoProvider ledger.DeployedChaincodeInfoProvider) error {
	var err error
	l.txtmgmt, err = lockbasedtxmgr.NewLockBasedTxMgr(l.ledgerID, versionedDB, stateListeners, btlPolicy, bookkeeperProvider, ccInfoProvider, l.stateCommitDoneCh)
	return err
}

func (l *kvLedger) initBlockStore(btlPolicy pvtdatapolicy.BTLPolicy) {
	l.blockStore.Init(btlPolicy)
}

//Recover the state database and history database (if exist)
//by recommitting last valid blocks
func (l *kvLedger) recoverDBs() error {
	logger.Debugf("Entering recoverDB()")
	if err := l.syncStateAndHistoryDBWithBlockstore(); err != nil {
		return err
	}
	if err := l.syncStateDBWithPvtdatastore(); err != nil {
		return err
	}
	return nil
}

func (l *kvLedger) syncStateAndHistoryDBWithBlockstore() error {
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

func (l *kvLedger) syncStateDBWithPvtdatastore() error {
	// TODO: So far, the design philosophy was that the scope of block storage is
	// limited to storing and retrieving blocks data with certain guarantees and statedb is
	// for the state management. The higher layer, 'kvledger', coordinates the acts between
	// the two. However, with maintaining the state of the consumption of blocks (i.e,
	// lastUpdatedOldBlockList for pvtstore reconciliation) within private data block storage
	// breaks that assumption. The knowledge of what blocks have been consumed for the purpose
	// of state update should not lie with the source (i.e., pvtdatastorage). A potential fix
	// is mentioned in FAB-12731
	blocksPvtData, err := l.blockStore.GetLastUpdatedOldBlocksPvtData()
	if err != nil {
		return err
	}
	if err := l.txtmgmt.RemoveStaleAndCommitPvtDataOfOldBlocks(blocksPvtData); err != nil {
		return err
	}
	if err := l.blockStore.ResetLastUpdatedOldBlocksList(); err != nil {
		return err
	}

	return nil
}

//recommitLostBlocks retrieves blocks in specified range and commit the write set to either
//state DB or history DB or both
func (l *kvLedger) recommitLostBlocks(firstBlockNum uint64, lastBlockNum uint64, recoverables ...recoverable) error {
	logger.Infof("Recommitting lost blocks - firstBlockNum=%d, lastBlockNum=%d, recoverables=%#v", firstBlockNum, lastBlockNum, recoverables)
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
	logger.Infof("Recommitted lost blocks - firstBlockNum=%d, lastBlockNum=%d, recoverables=%#v", firstBlockNum, lastBlockNum, recoverables)
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
	return errors.New("not yet implemented")
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
	//stopWatch := metrics.StopWatch("kvledger_CommitWithPvtData_duration")
	//defer stopWatch()

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
	//stopWatch := metrics.StopWatch("kvledger_CommitWithPvtData_worker_duration")
	//defer stopWatch()
	var err error

	block := pvtdataAndBlock.Block
	blockNo := pvtdataAndBlock.Block.Header.Number

	startBlockProcessing := time.Now()
	logger.Debugf("[%s] Validating state for block [%d]", l.ledgerID, blockNo)
	txstatsInfo, err := l.txtmgmt.ValidateAndPrepare(pvtdataAndBlock, true)
	if err != nil {
		return err
	}
	elapsedBlockProcessing := time.Since(startBlockProcessing)

	startCommitBlockStorage := time.Now()
	logger.Debugf("[%s] Committing block [%d] to storage", l.ledgerID, blockNo)
	if err = l.blockStore.CommitWithPvtData(pvtdataAndBlock); err != nil {
		return err
	}
	elapsedCommitBlockStorage := time.Since(startCommitBlockStorage)

	startCommitState := time.Now()
	logger.Debugf("[%s] Committing block [%d] transactions to state database", l.ledgerID, blockNo)
	if err = l.txtmgmt.Commit(); err != nil {
		panic(errors.WithMessage(err, "error during commit to txmgr"))
	}
	elapsedCommitState := time.Since(startCommitState)

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

	elapsedCommitWithPvtData := time.Since(startBlockProcessing)

	logger.Infof("[%s] Committed block [%d] with %d transaction(s) in %dms (state_validation=%dms block_commit=%dms state_commit=%dms)",
		l.ledgerID, block.Header.Number, len(block.Data.Data),
		elapsedCommitWithPvtData/time.Millisecond,
		elapsedBlockProcessing/time.Millisecond,
		elapsedCommitBlockStorage/time.Millisecond,
		elapsedCommitState/time.Millisecond,
	)
	l.updateBlockStats(blockNo,
		elapsedBlockProcessing,
		elapsedCommitBlockStorage,
		elapsedCommitState,
		txstatsInfo,
	)

	return nil
}

func (l *kvLedger) updateBlockStats(
	blockNum uint64,
	blockProcessingTime time.Duration,
	blockstorageCommitTime time.Duration,
	statedbCommitTime time.Duration,
	txstatsInfo []*txmgr.TxStatInfo,
) {
	l.stats.updateBlockchainHeight(blockNum + 1)
	l.stats.updateBlockProcessingTime(blockProcessingTime)
	l.stats.updateBlockstorageCommitTime(blockstorageCommitTime)
	l.stats.updateStatedbCommitTime(statedbCommitTime)
	l.stats.updateTransactionsStats(txstatsInfo)
}

// GetMissingPvtDataInfoForMostRecentBlocks returns the missing private data information for the
// most recent `maxBlock` blocks which miss at least a private data of a eligible collection.
func (l *kvLedger) GetMissingPvtDataInfoForMostRecentBlocks(maxBlock int) (ledger.MissingPvtDataInfo, error) {
	return l.blockStore.GetMissingPvtDataInfoForMostRecentBlocks(maxBlock)
}

// ValidateMVCC validates block for MVCC conflicts and phantom reads against committed data
func (l *kvLedger) ValidateMVCC(ctx context.Context, block *common.Block, txFlags ledgerutil.TxValidationFlags, filter ledgerutil.TxFilter) error {
	return l.txtmgmt.ValidateMVCC(ctx, block, txFlags, filter)
}

// ValidateBlockWithPvtData validate commit with pvt data
func (l *kvLedger) ValidateBlockWithPvtData(pvtdataAndBlock *ledger.BlockAndPvtData) (error) {
	_, err := l.txtmgmt.ValidateAndPrepare(pvtdataAndBlock, true)
	return err
}

// GetPvtDataAndBlockByNum returns the block and the corresponding pvt data.
// The pvt data is filtered by the list of 'collections' supplied
func (l *kvLedger) GetPvtDataAndBlockByNum(blockNum uint64, filter ledger.PvtNsCollFilter) (*ledger.BlockAndPvtData, error) {
	//stopWatch := metrics.StopWatch("kvledger_GetPvtDataAndBlockByNum_duration")
	//defer stopWatch()

	blockAndPvtdata, err := l.blockStore.GetPvtDataAndBlockByNum(blockNum, filter)
	l.blockAPIsRWLock.RLock()
	l.blockAPIsRWLock.RUnlock()
	return blockAndPvtdata, err
}

// GetPvtDataByNum returns only the pvt data  corresponding to the given block number
// The pvt data is filtered by the list of 'collections' supplied
func (l *kvLedger) GetPvtDataByNum(blockNum uint64, filter ledger.PvtNsCollFilter) ([]*ledger.TxPvtData, error) {
	//stopWatch := metrics.StopWatch("kvledger_GetPvtDataByNum_duration")
	//defer stopWatch()

	pvtdata, err := l.blockStore.GetPvtDataByNum(blockNum, filter)
	l.blockAPIsRWLock.RLock()
	l.blockAPIsRWLock.RUnlock()
	return pvtdata, err
}

// Purge removes private read-writes set generated by endorsers at block height lesser than
// a given maxBlockNumToRetain. In other words, Purge only retains private read-write sets
// that were generated at block height of maxBlockNumToRetain or higher.
func (l *kvLedger) PurgePrivateData(maxBlockNumToRetain uint64) error {
	return errors.New("not yet implemented")
}

// PrivateDataMinBlockNum returns the lowest retained endorsement block height
func (l *kvLedger) PrivateDataMinBlockNum() (uint64, error) {
	return 0, errors.New("not yet implemented")
}

func (l *kvLedger) GetConfigHistoryRetriever() (ledger.ConfigHistoryRetriever, error) {
	return l.configHistoryRetriever, nil
}

func (l *kvLedger) CommitPvtDataOfOldBlocks(pvtData []*ledger.BlockPvtData) ([]*ledger.PvtdataHashMismatch, error) {
	logger.Debugf("[%s:] Comparing pvtData of [%d] old blocks against the hashes in transaction's rwset to find valid and invalid data",
		l.ledgerID, len(pvtData))
	validPvtData, hashMismatches, err := ConstructValidAndInvalidPvtData(pvtData, l.blockStore)
	if err != nil {
		return nil, err
	}

	logger.Debugf("[%s:] Committing pvtData of [%d] old blocks to the pvtdatastore", l.ledgerID, len(pvtData))
	err = l.blockStore.CommitPvtDataOfOldBlocks(validPvtData)
	if err != nil {
		return nil, err
	}

	logger.Debugf("[%s:] Committing pvtData of [%d] old blocks to the stateDB", l.ledgerID, len(pvtData))
	err = l.txtmgmt.RemoveStaleAndCommitPvtDataOfOldBlocks(validPvtData)
	if err != nil {
		return nil, err
	}

	logger.Debugf("[%s:] Clearing the bookkeeping information from pvtdatastore", l.ledgerID)
	if err := l.blockStore.ResetLastUpdatedOldBlocksList(); err != nil {
		return nil, err
	}

	return hashMismatches, nil
}

func (l *kvLedger) GetMissingPvtDataTracker() (ledger.MissingPvtDataTracker, error) {
	return l, nil
}

// Close closes `KVLedger`
func (l *kvLedger) Close() {

	close(l.doneCh)
	<-l.stoppedCommitCh
	<-l.stateCommitDoneCh
	<-l.stoppedIndexCh

	l.blockStore.Shutdown()
	l.txtmgmt.Shutdown()
}

// TODO: merge into BlockStore interface
type blockCommitNotifier interface {
	BlockCommitted() (uint64, chan struct{})
}

// commitWatcher gets notified when each commit is done and it performs required cache cleanup
func (l *kvLedger) commitWatcher(btlPolicy pvtdatapolicy.BTLPolicy) {
	// TODO: merge interfaces
	store, ok := l.blockStore.BlockStore.(blockCommitNotifier)
	if !ok {
		panic("commitWatcher using an incompatible blockStore")
	}

	blockNo, nextBlockCh := store.BlockCommitted()

	type blockCommitProgress struct {
		nextBlock                                   *common.Block
		commitStartTime                             time.Time
		stateCommittedDuration, elapsedBlockStorage time.Duration
		blockCommitted, stateCommitted              bool
	}
	commitProgress := make(map[uint64]*blockCommitProgress)

	checkForDone := func(cp *blockCommitProgress) {
		if cp.blockCommitted && cp.stateCommitted {
			elapsedCommitWithPvtData := time.Since(cp.commitStartTime)

			/*metrics.RootScope.Gauge(fmt.Sprintf("kvledger_%s_committed_block_number", metrics.FilterMetricName(l.ledgerID))).Update(float64(blockNo))
			metrics.RootScope.Timer(fmt.Sprintf("kvledger_%s_committed_duration", metrics.FilterMetricName(l.ledgerID))).Record(elapsedCommitWithPvtData)
			if metrics.IsDebug() {
				metrics.RootScope.Timer(fmt.Sprintf("kvledger_%s_committed_state_duration", metrics.FilterMetricName(l.ledgerID))).Record(cp.stateCommittedDuration)
				metrics.RootScope.Timer(fmt.Sprintf("kvledger_%s_committed_block_duration", metrics.FilterMetricName(l.ledgerID))).Record(cp.elapsedBlockStorage)
			}*/

			// TODO: more precise start times for elapsedBlockStorage and stateCommittedDuration
			logger.Infof("[%s] Committed block [%d] with %d transaction(s) in %dms (block_commit=%dms state_commit=%dms)",
				l.ledgerID, blockNo, len(cp.nextBlock.Data.Data), elapsedCommitWithPvtData/time.Millisecond, cp.elapsedBlockStorage/time.Millisecond, cp.stateCommittedDuration/time.Millisecond)

			delete(commitProgress, cp.nextBlock.GetHeader().GetNumber())
		}
	}

	for {
		select {
		case <-l.doneCh: // kvledger is shutting down.
			close(l.stoppedCommitCh)
			return
		case pvtdataAndBlock := <-l.stateCommitDoneCh: // State has been committed - unpin keys from cache
			block := pvtdataAndBlock.Block
			pvtData := pvtdataAndBlock.PvtData

			cp, ok := commitProgress[block.Header.Number]
			if !ok {
				panic(fmt.Sprintf("unexpected block committed [%d]", block.Header.Number))
			}

			logger.Debugf("*** cleaning up pinned tx in cache for cacheBlock %d channelID %s\n", block.Header.Number, l.ledgerID)
			validatedTxOps, pvtDataHashedKeys, txValidationFlags, err := l.getKVFromBlock(block, btlPolicy)
			if err != nil {
				logger.Errorf(" failed to clear pinned tx for committed block %d : %s", pvtdataAndBlock.Block.Header.GetNumber(), err)
			}
			pvtDataKeys, _, err := getPrivateDataKV(block.Header.Number, l.ledgerID, pvtData, txValidationFlags, btlPolicy)
			if err != nil {
				logger.Errorf(" failed to clear pinned tx for committed block %d : %s", pvtdataAndBlock.Block.Header.GetNumber(), err)
			}

			l.kvCacheProvider.OnTxCommit(validatedTxOps, pvtDataKeys, pvtDataHashedKeys)

			cp.stateCommittedDuration = time.Since(cp.commitStartTime)
			cp.stateCommitted = true
			checkForDone(cp)
		case <-nextBlockCh: // A block has been committed to storage.
			blockNo, nextBlockCh = store.BlockCommitted()

			if !ledgerconfig.IsCommitter() { // TODO: refactor AddBlock to do similar
				continue
			}

			cp, ok := commitProgress[blockNo]
			if !ok {
				panic(fmt.Sprintf("unexpected block committed [%d, %d]", blockNo, cp.nextBlock.Header.Number))
			}

			cp.elapsedBlockStorage = time.Since(cp.commitStartTime)
			cp.blockCommitted = true
			checkForDone(cp)
		case pvtdataAndBlock := <-l.commitCh: // Process next block through commit workflow (should be last case statement).
			cp := blockCommitProgress{
				nextBlock:       pvtdataAndBlock.Block,
				commitStartTime: time.Now(),
			}
			commitProgress[pvtdataAndBlock.Block.GetHeader().GetNumber()] = &cp

			err := l.commitWithPvtData(pvtdataAndBlock)
			if err != nil {
				panic(err)
			}
		}
	}
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

type collectionInfoRetriever struct {
	ledger       ledger.PeerLedger
	infoProvider ledger.DeployedChaincodeInfoProvider
}

func (r *collectionInfoRetriever) CollectionInfo(chaincodeName, collectionName string) (*common.StaticCollectionConfig, error) {
	qe, err := r.ledger.NewQueryExecutor()
	if err != nil {
		return nil, err
	}
	defer qe.Done()
	return r.infoProvider.CollectionInfo(chaincodeName, collectionName, qe)
}
