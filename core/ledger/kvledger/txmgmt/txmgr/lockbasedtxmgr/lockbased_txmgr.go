/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package lockbasedtxmgr

import (
	"sync"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/metrics"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/kvledger/bookkeeping"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/privacyenabledstate"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/pvtstatepurgemgmt"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/validator"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/validator/valimpl"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/core/ledger/pvtdatapolicy"
	"github.com/hyperledger/fabric/core/ledger/util"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/ledger/rwset/kvrwset"
	"github.com/uber-go/tally"
)

var logger = flogging.MustGetLogger("lockbasedtxmgr")

const (
	checkpointCommitInterval = 1 // number of commits between checkpoints.
	commitQueueLen           = checkpointCommitInterval
)

// LockBasedTxMgr a simple implementation of interface `txmgmt.TxMgr`.
// This implementation uses a read-write lock to prevent conflicts between transaction simulation and committing
type LockBasedTxMgr struct {
	ledgerid         string
	db               privacyenabledstate.DB
	pvtdataPurgeMgr  *pvtdataPurgeMgr
	validator        validator.Validator
	stateListeners   []ledger.StateListener
	commitRWLock     sync.RWMutex
	current          *current
	StopWatch        tally.Stopwatch
	StopWatchAccess  string
	StopWatch1       tally.Stopwatch
	StopWatch1Access string

	commitCh   chan *current
	commitDone chan *ledger.BlockAndPvtData
	shutdownCh chan struct{}
	doneCh     chan struct{}
}

type current struct {
	blockAndPvtData *ledger.BlockAndPvtData
	batch           *privacyenabledstate.UpdateBatch
	listeners       []ledger.StateListener
}

func (c *current) blockNum() uint64 {
	return c.blockAndPvtData.Block.Header.Number
}

func (c *current) maxTxNumber() uint64 {
	return uint64(len(c.blockAndPvtData.Block.Data.Data)) - 1
}

// NewLockBasedTxMgr constructs a new instance of NewLockBasedTxMgr
func NewLockBasedTxMgr(ledgerid string, db privacyenabledstate.DB, stateListeners []ledger.StateListener,
	btlPolicy pvtdatapolicy.BTLPolicy, bookkeepingProvider bookkeeping.Provider, commitDone chan *ledger.BlockAndPvtData) (*LockBasedTxMgr, error) {
	db.Open()
	txmgr := &LockBasedTxMgr{
		ledgerid:       ledgerid,
		db:             db,
		stateListeners: stateListeners,
		commitCh:       make(chan *current, commitQueueLen),
		commitDone:     commitDone,
		shutdownCh:     make(chan struct{}),
		doneCh:         make(chan struct{}),
	}
	pvtstatePurgeMgr, err := pvtstatepurgemgmt.InstantiatePurgeMgr(ledgerid, db, btlPolicy, bookkeepingProvider)
	if err != nil {
		return nil, err
	}
	txmgr.pvtdataPurgeMgr = &pvtdataPurgeMgr{pvtstatePurgeMgr, false}
	txmgr.validator = valimpl.NewStatebasedValidator(ledgerid, txmgr, db)

	go txmgr.committer()
	return txmgr, nil
}

// GetLastSavepoint returns the block num recorded in savepoint,
// returns 0 if NO savepoint is found
func (txmgr *LockBasedTxMgr) GetLastSavepoint() (*version.Height, error) {
	return txmgr.db.GetLatestSavePoint()
}

// NewQueryExecutor implements method in interface `txmgmt.TxMgr`
func (txmgr *LockBasedTxMgr) NewQueryExecutor(txid string) (ledger.QueryExecutor, error) {
	qe := newQueryExecutor(txmgr, txid)
	stopWatch := metrics.RootScope.Timer("lockbasedtxmgr_NewQueryExecutor_commitRWLock_RLock_wait_time").Start()
	txmgr.commitRWLock.RLock()
	stopWatch.Stop()
	txmgr.StopWatch = metrics.RootScope.Timer("lockbasedtxmgr_NewQueryExecutor_commitRWLock_RLock_time").Start()
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
	stopWatch := metrics.RootScope.Timer("lockbasedtxmgr_NewTxSimulator_commitRWLock_RLock_wait_time").Start()
	txmgr.commitRWLock.RLock()
	stopWatch.Stop()
	txmgr.StopWatch1 = metrics.RootScope.Timer("lockbasedtxmgr_NewTxSimulator_commitRWLock_RLock_time").Start()
	txmgr.StopWatch1Access = "1"
	return s, nil
}

// ValidateMVCC validates block for MVCC conflicts and phantom reads against committed data
func (txmgr *LockBasedTxMgr) ValidateMVCC(block *common.Block, txFlags util.TxValidationFlags, doMVCCValidation bool, filter util.TxFilter) error {
	err := txmgr.validator.ValidateMVCC(block, txFlags, doMVCCValidation, filter)
	if err != nil {
		txmgr.reset()
		return err
	}
	return nil
}

// ValidateAndPrepare implements method in interface `txmgmt.TxMgr`
func (txmgr *LockBasedTxMgr) ValidateAndPrepare(blockAndPvtdata *ledger.BlockAndPvtData, doMVCCValidation bool) error {
	logger.Debugf("Waiting for purge mgr to finish the background job of computing expirying keys for the block")
	txmgr.pvtdataPurgeMgr.WaitForPrepareToFinish()

	logger.Debugf("Validating new block %d with num trans = [%d]", blockAndPvtdata.Block.Header.Number, len(blockAndPvtdata.Block.Data.Data))
	batch, err := txmgr.validator.ValidateAndPrepareBatch(blockAndPvtdata, doMVCCValidation)
	if err != nil {
		txmgr.reset()
		return err
	}
	txmgr.current = &current{blockAndPvtData: blockAndPvtdata, batch: batch}
	if err := txmgr.invokeNamespaceListeners(); err != nil {
		txmgr.reset()
		return err
	}
	return nil
}

func (txmgr *LockBasedTxMgr) invokeNamespaceListeners() error {
	for _, listener := range txmgr.stateListeners {
		stateUpdatesForListener := extractStateUpdates(txmgr.current.batch, listener.InterestedInNamespaces())
		if len(stateUpdatesForListener) == 0 {
			continue
		}
		txmgr.current.listeners = append(txmgr.current.listeners, listener)
		if err := listener.HandleStateUpdates(txmgr.ledgerid, stateUpdatesForListener, txmgr.current.blockNum()); err != nil {
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
func (txmgr *LockBasedTxMgr) Commit() error {

	if txmgr.current == nil {
		panic("validateAndPrepare() method should have been called before calling commit()")
	}

	txmgr.commitCh <- txmgr.current
	return nil
}

// Rollback implements method in interface `txmgmt.TxMgr`
func (txmgr *LockBasedTxMgr) Rollback() {
	txmgr.reset()
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
	// FIXME: Will this work with distributed validation???
	if err := txmgr.ValidateAndPrepare(blockAndPvtdata, false); err != nil {
		return err
	}
	logger.Debugf("Committing block %d to state database", block.Header.Number)
	return txmgr.Commit()
}

//committer commits current batch from incoming commitCh items
//TODO panic may not be required for some errors
func (txmgr *LockBasedTxMgr) committer() {

	const panicMsg = "commit failure"

	for {
		select {
		case <-txmgr.doneCh:
			close(txmgr.shutdownCh)
			return
		case tx := <-txmgr.commitCh:

			var commitWatch tally.Stopwatch
			if metrics.IsDebug() {
				// Measure the whole
				commitWatch = metrics.RootScope.Timer("lockbasedtxmgr_Commit_time").Start()
			}

			// When using the purge manager for the first block commit after peer start, the asynchronous function
			// 'PrepareForExpiringKeys' is invoked in-line. However, for the subsequent blocks commits, this function is invoked
			// in advance for the next block
			if !txmgr.pvtdataPurgeMgr.usedOnce {
				stopWatch := metrics.RootScope.Timer("lockbasedtxmgr_Commit_PrepareForExpiringKeys_time").Start()
				txmgr.pvtdataPurgeMgr.PrepareForExpiringKeys(tx.blockNum())
				txmgr.pvtdataPurgeMgr.usedOnce = true
				stopWatch.Stop()
			}

			forExpiry := tx.blockNum() + 1

			if err := txmgr.pvtdataPurgeMgr.RemoveNonDurable(
				txmgr.current.batch.PvtUpdates, txmgr.current.batch.HashUpdates); err != nil {
				logger.Errorf("failed to remove non durable : %s", err)
				panic(panicMsg)
			}

			purgeWatch := metrics.RootScope.Timer("lockbasedtxmgr_Commit_DeleteExpiredAndUpdateBookkeeping_time").Start()
			if err := txmgr.pvtdataPurgeMgr.DeleteExpiredAndUpdateBookkeeping(
				tx.batch.PvtUpdates, tx.batch.HashUpdates); err != nil {
				logger.Errorf("failed to delete expired and update booking : %s", err)
				panic(panicMsg)
				purgeWatch.Stop()
			}
			purgeWatch.Stop()

			lockWatch := metrics.RootScope.Timer("lockbasedtxmgr_Commit_commitRWLock_time").Start()
			txmgr.commitRWLock.Lock()
			lockWatch.Stop()
			logger.Debugf("Write lock acquired for committing updates to state database")

			commitHeight := version.NewHeight(tx.blockNum(), tx.maxTxNumber())
			applyUpdateWatch := metrics.RootScope.Timer("lockbasedtxmgr_Commit_ApplyPrivacyAwareUpdates_time").Start()
			if err := txmgr.db.ApplyPrivacyAwareUpdates(tx.batch, commitHeight); err != nil {
				logger.Errorf("failed to apply updates : %s", err)
				txmgr.commitRWLock.Unlock()
				applyUpdateWatch.Stop()
				panic(panicMsg)
			}
			applyUpdateWatch.Stop()
			logger.Debugf("Updates committed to state database")

			// purge manager should be called (in this call the purge mgr removes the expiry entries from schedules) after committing to statedb
			blkCommitWatch := metrics.RootScope.Timer("lockbasedtxmgr_Commit_BlockCommitDone_time").Start()
			if err := txmgr.pvtdataPurgeMgr.BlockCommitDone(); err != nil {
				logger.Errorf("failed to purge expiry entries from schedules : %s", err)
				txmgr.commitRWLock.Unlock()
				blkCommitWatch.Stop()
				panic(panicMsg)
			}
			blkCommitWatch.Stop()

			// In the case of error state listeners will not recieve this call - instead a peer panic is caused by the ledger upon receiveing
			// an error from this function
			updateListnWatch := metrics.RootScope.Timer("lockbasedtxmgr_Commit_updateStateListeners_time").Start()
			txmgr.updateStateListeners(tx)
			updateListnWatch.Stop()

			//clean up and prepare for expiring keys
			clearWatch := metrics.RootScope.Timer("lockbasedtxmgr_Commit_defer_time").Start()
			txmgr.clearCache()
			txmgr.pvtdataPurgeMgr.PrepareForExpiringKeys(forExpiry)
			logger.Debugf("Cleared version cache and launched the background routine for preparing keys to purge with the next block")
			clearWatch.Stop()

			txmgr.commitRWLock.Unlock()

			//notify kv ledger that commit is done for given block and private data
			txmgr.commitDone <- tx.blockAndPvtData

			if metrics.IsDebug() {
				commitWatch.Stop()
			}
		}
	}
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

func (txmgr *LockBasedTxMgr) updateStateListeners(tx *current) {

	if metrics.IsDebug() {
		stopWatch := metrics.RootScope.Timer("lockbasedtxmgr_updateStateListenersTimer_time").Start()
		defer stopWatch.Stop()
	}

	for _, l := range tx.listeners {
		l.StateCommitDone(txmgr.ledgerid)
	}
}

func (txmgr *LockBasedTxMgr) reset() {
	txmgr.current = nil
}

// pvtdataPurgeMgr wraps the actual purge manager and an additional flag 'usedOnce'
// for usage of this additional flag, see the relevant comments in the txmgr.Commit() function above
type pvtdataPurgeMgr struct {
	pvtstatepurgemgmt.PurgeMgr
	usedOnce bool
}
