/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package lockbasedtxmgr

import (
	"sync"

	"fmt"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/metrics"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/kvledger/bookkeeping"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/privacyenabledstate"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/pvtstatepurgemgmt"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/validator"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/validator/valimpl"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/core/ledger/pvtdatapolicy"
	"github.com/hyperledger/fabric/core/ledger/util"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/ledger/rwset/kvrwset"
	"github.com/pkg/errors"
	"github.com/uber-go/tally"
	"golang.org/x/net/context"
)

var logger = flogging.MustGetLogger("lockbasedtxmgr")

// LockBasedTxMgr a simple implementation of interface `txmgmt.TxMgr`.
// This implementation uses a read-write lock to prevent conflicts between transaction simulation and committing
type LockBasedTxMgr struct {
	ledgerid         string
	db               privacyenabledstate.DB
	pvtdataPurgeMgr  *pvtdataPurgeMgr
	validator        validator.Validator
	stateListeners   []ledger.StateListener
	commitRWLock     sync.RWMutex
	pendingUpdate    map[uint64]*update
	pendingUpdateMtx sync.RWMutex
	StopWatch        tally.Stopwatch
	StopWatchAccess  string
	StopWatch1       tally.Stopwatch
	StopWatch1Access string
	btlPolicy        pvtdatapolicy.BTLPolicy
	committedBlock   *ledger.BlockAndPvtData

	commitCh   chan *update
	commitDone chan struct{}
	shutdownCh chan struct{}
	doneCh     chan struct{}
}

type update struct {
	blockAndPvtData *ledger.BlockAndPvtData
	batch           *privacyenabledstate.UpdateBatch
	listeners       []ledger.StateListener
}

func (c *update) blockNum() uint64 {
	return c.blockAndPvtData.Block.Header.Number
}

func (c *update) maxTxNumber() uint64 {
	return uint64(len(c.blockAndPvtData.Block.Data.Data)) - 1
}

// NewLockBasedTxMgr constructs a new instance of NewLockBasedTxMgr
func NewLockBasedTxMgr(ledgerid string, db privacyenabledstate.DB, stateListeners []ledger.StateListener,
	btlPolicy pvtdatapolicy.BTLPolicy, bookkeepingProvider bookkeeping.Provider) (*LockBasedTxMgr, error) {
	db.Open()
	txmgr := &LockBasedTxMgr{
		ledgerid:       ledgerid,
		db:             db,
		stateListeners: stateListeners,
		commitCh:       make(chan *update),
		commitDone:     make(chan struct{}),
		shutdownCh:     make(chan struct{}),
		doneCh:         make(chan struct{}),
		btlPolicy:      btlPolicy,
		pendingUpdate:  make(map[uint64]*update),
	}

	pvtstatePurgeMgr, err := pvtstatepurgemgmt.InstantiatePurgeMgr(ledgerid, db, btlPolicy, bookkeepingProvider)
	if err != nil {
		return nil, err
	}
	txmgr.pvtdataPurgeMgr = &pvtdataPurgeMgr{pvtstatePurgeMgr, false}
	txmgr.validator = valimpl.NewStatebasedValidator(ledgerid, txmgr, db)

	concurrentBlockWrites := ledgerconfig.GetConcurrentBlockWrites()
	for x := 0; x < concurrentBlockWrites; x++ {
		go txmgr.committer()
	}

	return txmgr, nil
}

// GetLastSavepoint returns the block num recorded in savepoint,
// returns 0 if NO savepoint is found
func (txmgr *LockBasedTxMgr) GetLastSavepoint() (*version.Height, error) {
	return txmgr.db.GetLatestSavePoint()
}

// GetDB returns the db instance
func (txmgr *LockBasedTxMgr) GetDB() privacyenabledstate.DB {
	return txmgr.db
}

// NewQueryExecutor implements method in interface `txmgmt.TxMgr`
func (txmgr *LockBasedTxMgr) NewQueryExecutor(txid string) (ledger.QueryExecutor, error) {
	qe := newQueryExecutor(txmgr, txid, txmgr.btlPolicy)
	stopWatch := metrics.RootScope.Timer("lockbasedtxmgr_NewQueryExecutor_commitRWLock_RLock_wait_duration").Start()
	txmgr.commitRWLock.RLock()
	stopWatch.Stop()
	txmgr.StopWatch = metrics.RootScope.Timer("lockbasedtxmgr_NewQueryExecutor_commitRWLock_RLock_duration").Start()
	txmgr.StopWatchAccess = "1"
	return qe, nil
}

// NewTxSimulator implements method in interface `txmgmt.TxMgr`
func (txmgr *LockBasedTxMgr) NewTxSimulator(txid string) (ledger.TxSimulator, error) {
	logger.Debugf("constructing new tx simulator")
	s, err := newLockBasedTxSimulator(txmgr, txid)
	if err != nil {
		return nil, err
	}
	stopWatch := metrics.RootScope.Timer("lockbasedtxmgr_NewTxSimulator_commitRWLock_RLock_wait_duration").Start()
	txmgr.commitRWLock.RLock()
	stopWatch.Stop()
	txmgr.StopWatch1 = metrics.RootScope.Timer("lockbasedtxmgr_NewTxSimulator_commitRWLock_RLock_duration").Start()
	txmgr.StopWatch1Access = "1"
	return s, nil
}

// ValidateMVCC validates block for MVCC conflicts and phantom reads against committed data
func (txmgr *LockBasedTxMgr) ValidateMVCC(ctx context.Context, block *common.Block, txFlags util.TxValidationFlags, filter util.TxFilter) error {
	err := txmgr.validator.ValidateMVCC(ctx, block, txFlags, filter)
	if err != nil {
		return err
	}
	return nil
}

// ValidateAndPrepare implements method in interface `txmgmt.TxMgr`
func (txmgr *LockBasedTxMgr) ValidateAndPrepare(blockAndPvtdata *ledger.BlockAndPvtData, doMVCCValidation bool) error {
	//TODO
	//if blockAndPvtdata.Block.Header.Number > uint64(ledgerconfig.GetConcurrentBlockWrites()) {
	//	waitForPvt := blockAndPvtdata.Block.Header.Number - uint64(ledgerconfig.GetConcurrentBlockWrites())
	//	// Wait for underlying storage to complete commit on previous block.
	//	logger.Debugf("waiting for previous block to checkpoint [%d]", waitForPvt)
	//	stopWatchWaitBlock := metrics.StopWatch("cached_pvt_store_prepare_wait_block_duration")
	//	c.waitForPvt(context.Background(), waitForPvt)
	//	stopWatchWaitBlock()
	//	logger.Debugf("ready to store incoming block [%d]", blockAndPvtdata.Block.Header.Number)
	//}

	logger.Debugf("Waiting for purge mgr to finish the background job of computing expirying keys for the block")
	txmgr.pvtdataPurgeMgr.WaitForPrepareToFinish()

	logger.Debugf("Validating new block %d with num trans = [%d]", blockAndPvtdata.Block.Header.Number, len(blockAndPvtdata.Block.Data.Data))
	batch, err := txmgr.validator.ValidateAndPrepareBatch(blockAndPvtdata, doMVCCValidation)
	if err != nil {
		return err
	}
	current := update{blockAndPvtData: blockAndPvtdata, batch: batch}
	if err := txmgr.invokeNamespaceListeners(&current); err != nil {
		return err
	}

	txmgr.pushPendingUpdate(blockAndPvtdata.Block.Header.Number, &current)

	return nil
}

func (txmgr *LockBasedTxMgr) invokeNamespaceListeners(c *update) error {
	for _, listener := range txmgr.stateListeners {
		stateUpdatesForListener := extractStateUpdates(c.batch, listener.InterestedInNamespaces())
		if len(stateUpdatesForListener) == 0 {
			continue
		}
		c.listeners = append(c.listeners, listener)
		if err := listener.HandleStateUpdates(txmgr.ledgerid, stateUpdatesForListener, c.blockNum()); err != nil {
			return err
		}
		logger.Debugf("Invoking listener for state changes:%s", listener)
	}
	return nil
}

// Shutdown implements method in interface `txmgmt.TxMgr`
func (txmgr *LockBasedTxMgr) Shutdown() {

	close(txmgr.doneCh)
	<-txmgr.shutdownCh

	txmgr.db.Close()
}

// Commit implements method in interface `txmgmt.TxMgr`
func (txmgr *LockBasedTxMgr) Commit(blockNum uint64) error {
	if !txmgr.checkPendingUpdate(blockNum) {
		panic(fmt.Sprintf("validateAndPrepare() for block %d method should have been called before calling commit()", blockNum))
	}

	pendingUpdate, err := txmgr.popPendingUpdate(blockNum)
	if err != nil {
		panic(err.Error())
	}

	txmgr.commitCh <- pendingUpdate
	return nil
}

// Rollback implements method in interface `txmgmt.TxMgr`
func (txmgr *LockBasedTxMgr) Rollback(blockNum uint64) {
	if !txmgr.checkPendingUpdate(blockNum) {
		panic(fmt.Sprintf("validateAndPrepare() for block %d method should have been called before calling rollback()", blockNum))
	}

	_, err := txmgr.popPendingUpdate(blockNum)
	if err != nil {
		panic(err.Error())
	}
}

// clearCache empty the cache maintained by the statedb implementation
func (txmgr *LockBasedTxMgr) clearCache() {
	if txmgr.db.IsBulkOptimizable() {
		txmgr.db.ClearCachedVersions()
	}
}

// ShouldRecover implements method in interface kvledger.Recoverer
func (txmgr *LockBasedTxMgr) ShouldRecover(lastAvailableBlock uint64) (bool, uint64, error) {
	savepoint, err := txmgr.GetLastSavepoint()
	if err != nil {
		return false, 0, err
	}
	if savepoint == nil {
		return true, 0, nil
	}
	return savepoint.BlockNum != lastAvailableBlock, savepoint.BlockNum + 1, nil
}

// CommitLostBlock implements method in interface kvledger.Recoverer
func (txmgr *LockBasedTxMgr) CommitLostBlock(blockAndPvtdata *ledger.BlockAndPvtData) error {
	block := blockAndPvtdata.Block
	logger.Debugf("Constructing updateSet for the block %d", block.Header.Number)
	if err := txmgr.ValidateAndPrepare(blockAndPvtdata, false); err != nil {
		return err
	}
	logger.Debugf("Committing block %d to state database", block.Header.Number)
	return txmgr.Commit(blockAndPvtdata.Block.Header.Number)
}

//committer commits update batch from incoming commitCh items
//TODO panic may not be required for some errors
func (txmgr *LockBasedTxMgr) committer() {

	const panicMsg = "commit failure"

	for {
		select {
		case <-txmgr.doneCh:
			close(txmgr.shutdownCh)
			return
		case current := <-txmgr.commitCh:
			var commitWatch tally.Stopwatch
			if metrics.IsDebug() {
				// Measure the whole
				commitWatch = metrics.RootScope.Timer("lockbasedtxmgr_Commit_duration").Start()
			}

			// When using the purge manager for the first block commit after peer start, the asynchronous function
			// 'PrepareForExpiringKeys' is invoked in-line. However, for the subsequent blocks commits, this function is invoked
			// in advance for the next block
			if !txmgr.pvtdataPurgeMgr.usedOnce {
				stopWatch := metrics.RootScope.Timer("lockbasedtxmgr_Commit_PrepareForExpiringKeys_duration").Start()
				txmgr.pvtdataPurgeMgr.PrepareForExpiringKeys(current.blockNum())
				txmgr.pvtdataPurgeMgr.usedOnce = true
				stopWatch.Stop()
			}

			forExpiry := current.blockNum() + 1

			if err := txmgr.pvtdataPurgeMgr.RemoveNonDurable(
				current.batch.PvtUpdates, current.batch.HashUpdates); err != nil {
				logger.Errorf("failed to remove non durable : %s", err)
				panic(panicMsg)
			}

			purgeWatch := metrics.RootScope.Timer("lockbasedtxmgr_Commit_DeleteExpiredAndUpdateBookkeeping_duration").Start()
			if err := txmgr.pvtdataPurgeMgr.DeleteExpiredAndUpdateBookkeeping(
				current.batch.PvtUpdates, current.batch.HashUpdates); err != nil {
				logger.Errorf("failed to delete expired and update booking : %s", err)
				panic(panicMsg)
				purgeWatch.Stop()
			}
			purgeWatch.Stop()

			lockWatch := metrics.RootScope.Timer("lockbasedtxmgr_Commit_commitRWLock_duration").Start()
			txmgr.commitRWLock.Lock()
			lockWatch.Stop()
			logger.Debugf("Write lock acquired for committing updates to state database")

			commitHeight := version.NewHeight(current.blockNum(), current.maxTxNumber())
			applyUpdateWatch := metrics.RootScope.Timer("lockbasedtxmgr_Commit_ApplyPrivacyAwareUpdates_duration").Start()
			if err := txmgr.db.ApplyPrivacyAwareUpdates(current.batch, commitHeight); err != nil {
				logger.Errorf("failed to apply updates : %s", err)
				txmgr.commitRWLock.Unlock()
				applyUpdateWatch.Stop()
				panic(panicMsg)
			}
			applyUpdateWatch.Stop()
			logger.Debugf("Updates committed to state database")

			// purge manager should be called (in this call the purge mgr removes the expiry entries from schedules) after committing to statedb
			blkCommitWatch := metrics.RootScope.Timer("lockbasedtxmgr_Commit_BlockCommitDone_duration").Start()
			if err := txmgr.pvtdataPurgeMgr.BlockCommitDone(); err != nil {
				logger.Errorf("failed to purge expiry entries from schedules : %s", err)
				txmgr.commitRWLock.Unlock()
				blkCommitWatch.Stop()
				panic(panicMsg)
			}
			blkCommitWatch.Stop()

			// In the case of error state listeners will not recieve this call - instead a peer panic is caused by the ledger upon receiveing
			// an error from this function
			updateListnWatch := metrics.RootScope.Timer("lockbasedtxmgr_Commit_updateStateListeners_duration").Start()
			txmgr.updateStateListeners(current)
			updateListnWatch.Stop()

			//clean up and prepare for expiring keys
			clearWatch := metrics.RootScope.Timer("lockbasedtxmgr_Commit_defer_duration").Start()
			txmgr.clearCache()
			txmgr.pvtdataPurgeMgr.PrepareForExpiringKeys(forExpiry)
			logger.Debugf("Cleared version cache and launched the background routine for preparing keys to purge with the next block")
			clearWatch.Stop()

			close(txmgr.commitDone)
			txmgr.commitDone = make(chan struct{})
			txmgr.committedBlock = current.blockAndPvtData

			txmgr.commitRWLock.Unlock()

			//notify kv ledger that commit is done for given block and private data

			if metrics.IsDebug() {
				commitWatch.Stop()
			}
		}
	}
}

//BlockCommitted returns recent block committed and chan to notify next block commit
func (txmgr *LockBasedTxMgr) BlockCommitted() (*ledger.BlockAndPvtData, chan struct{}) {

	txmgr.commitRWLock.RLock()
	blockCommitted := txmgr.committedBlock
	commitDone := txmgr.commitDone
	txmgr.commitRWLock.RUnlock()

	return blockCommitted, commitDone
}

func extractStateUpdates(batch *privacyenabledstate.UpdateBatch, namespaces []string) ledger.StateUpdates {
	stateupdates := make(ledger.StateUpdates)
	for _, namespace := range namespaces {
		updatesMap := batch.PubUpdates.GetUpdates(namespace)
		var kvwrites []*kvrwset.KVWrite
		for key, versionedValue := range updatesMap {
			kvwrites = append(kvwrites, &kvrwset.KVWrite{Key: key, IsDelete: versionedValue.Value == nil, Value: versionedValue.Value})
			if len(kvwrites) > 0 {
				stateupdates[namespace] = kvwrites
			}
		}
	}
	return stateupdates
}

func (txmgr *LockBasedTxMgr) updateStateListeners(tx *update) {
	stopWatch := metrics.StopWatch("lockbasedtxmgr_updateStateListenersTimer_duration")
	defer stopWatch()

	for _, l := range tx.listeners {
		l.StateCommitDone(txmgr.ledgerid)
	}
}

func (txmgr *LockBasedTxMgr) RLock() {
	txmgr.commitRWLock.RLock()
}

func (txmgr *LockBasedTxMgr) RUnlock() {
	txmgr.commitRWLock.RUnlock()
}

func (txmgr *LockBasedTxMgr) Lock() {
	txmgr.commitRWLock.Lock()
}

func (txmgr *LockBasedTxMgr) Unlock() {
	txmgr.commitRWLock.Unlock()
}

func (txmgr *LockBasedTxMgr) pushPendingUpdate(blockNumber uint64, update *update) {
	txmgr.pendingUpdateMtx.Lock()
	defer txmgr.pendingUpdateMtx.Unlock()

	txmgr.pendingUpdate[blockNumber] = update
}

func (txmgr *LockBasedTxMgr) checkPendingUpdate(blockNumber uint64) bool {
	txmgr.pendingUpdateMtx.RLock()
	defer txmgr.pendingUpdateMtx.RUnlock()

	_, ok := txmgr.pendingUpdate[blockNumber]
	return ok
}

func (txmgr *LockBasedTxMgr) popPendingUpdate(blockNumber uint64) (*update, error) {
	txmgr.pendingUpdateMtx.Lock()
	defer txmgr.pendingUpdateMtx.Unlock()

	update, ok := txmgr.pendingUpdate[blockNumber]
	if !ok {
		return nil, errors.Errorf("pvt was not prepared [%d]", blockNumber)
	}
	delete(txmgr.pendingUpdate, blockNumber)
	return update, nil
}

// pvtdataPurgeMgr wraps the actual purge manager and an additional flag 'usedOnce'
// for usage of this additional flag, see the relevant comments in the txmgr.Commit() function above
type pvtdataPurgeMgr struct {
	pvtstatepurgemgmt.PurgeMgr
	usedOnce bool
}
