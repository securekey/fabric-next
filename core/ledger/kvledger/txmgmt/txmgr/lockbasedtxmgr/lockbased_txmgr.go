/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package lockbasedtxmgr

import (
	"bytes"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/kvledger/bookkeeping"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/privacyenabledstate"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/pvtstatepurgemgmt"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/queryutil"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/validator"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/validator/valimpl"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/core/ledger/pvtdatapolicy"
	"github.com/hyperledger/fabric/core/ledger/util"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/ledger/rwset"
	"github.com/hyperledger/fabric/protos/ledger/rwset/kvrwset"
	"github.com/pkg/errors"
	//"github.com/uber-go/tally"
)

var logger = flogging.MustGetLogger("lockbasedtxmgr")

// LockBasedTxMgr a simple implementation of interface `txmgmt.TxMgr`.
// This implementation uses a read-write lock to prevent conflicts between transaction simulation and committing
type LockBasedTxMgr struct {
	ledgerid              string
	db                    privacyenabledstate.DB
	pvtdataPurgeMgr       *pvtdataPurgeMgr
	validator             validator.Validator
	stateListeners        []ledger.StateListener
	ccInfoProvider        ledger.DeployedChaincodeInfoProvider
	commitRWLock          sync.RWMutex
	oldBlockCommit        sync.Mutex
	current               *update
	lastCommittedBlockNum uint64

	//StopWatch        tally.Stopwatch
	StopWatchAccess  string
	//StopWatch1       tally.Stopwatch
	StopWatch1Access string
	btlPolicy        pvtdatapolicy.BTLPolicy

	commitCh   chan *update
	commitDone chan *ledger.BlockAndPvtData
	shutdownCh chan struct{}
	doneCh     chan struct{}
}

type update struct {
	blockAndPvtData *ledger.BlockAndPvtData
	batch           *privacyenabledstate.UpdateBatch
	listeners       []ledger.StateListener
	commitDoneCh    chan struct{}
}

func (c *update) blockNum() uint64 {
	return c.blockAndPvtData.Block.Header.Number
}

func (c *update) maxTxNumber() uint64 {
	return uint64(len(c.blockAndPvtData.Block.Data.Data)) - 1
}

// NewLockBasedTxMgr constructs a new instance of NewLockBasedTxMgr
func NewLockBasedTxMgr(ledgerid string, db privacyenabledstate.DB, stateListeners []ledger.StateListener,
	btlPolicy pvtdatapolicy.BTLPolicy, bookkeepingProvider bookkeeping.Provider, ccInfoProvider ledger.DeployedChaincodeInfoProvider,
	commitDone chan *ledger.BlockAndPvtData) (*LockBasedTxMgr, error) {
	db.Open()
	txmgr := &LockBasedTxMgr{
		ledgerid:       ledgerid,
		db:             db,
		stateListeners: stateListeners,
		ccInfoProvider: ccInfoProvider,
		commitCh:       make(chan *update),
		commitDone:     commitDone,
		shutdownCh:     make(chan struct{}),
		doneCh:         make(chan struct{}),
		btlPolicy:      btlPolicy,
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
	qe := newQueryExecutor(txmgr, txid, txmgr.btlPolicy)
	//stopWatch := metrics.RootScope.Timer("lockbasedtxmgr_NewQueryExecutor_commitRWLock_RLock_wait_duration").Start()
	txmgr.commitRWLock.RLock()
	//stopWatch.Stop()
	//txmgr.StopWatch = metrics.RootScope.Timer("lockbasedtxmgr_NewQueryExecutor_commitRWLock_RLock_duration").Start()
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
	//stopWatch := metrics.RootScope.Timer("lockbasedtxmgr_NewTxSimulator_commitRWLock_RLock_wait_duration").Start()
	txmgr.commitRWLock.RLock()
	//stopWatch.Stop()
	//txmgr.StopWatch1 = metrics.RootScope.Timer("lockbasedtxmgr_NewTxSimulator_commitRWLock_RLock_duration").Start()
	txmgr.StopWatch1Access = "1"
	return s, nil
}

// ValidateMVCC validates block for MVCC conflicts and phantom reads against committed data
func (txmgr *LockBasedTxMgr) ValidateMVCC( block *common.Block, txFlags util.TxValidationFlags, filter util.TxFilter) error {
	err := txmgr.validator.ValidateMVCC(block, txFlags, filter)
	if err != nil {
		return err
	}
	return nil
}

// ValidateAndPrepare implements method in interface `txmgmt.TxMgr`
func (txmgr *LockBasedTxMgr) ValidateAndPrepare(blockAndPvtdata *ledger.BlockAndPvtData, doMVCCValidation bool) error {
	if !txmgr.waitForPreviousToFinish() {
		return  errors.New("shutdown has been requested")
	}

	logger.Debugf("Waiting for purge mgr to finish the background job of computing expirying keys for the block")
	txmgr.pvtdataPurgeMgr.WaitForPrepareToFinish()

	logger.Debugf("Validating new block %d with num trans = [%d]", blockAndPvtdata.Block.Header.Number, len(blockAndPvtdata.Block.Data.Data))
	batch, err := txmgr.validator.ValidateAndPrepareBatch(blockAndPvtdata, doMVCCValidation)
	if err != nil {
		txmgr.reset()
		return err
	}
	current := update{blockAndPvtData: blockAndPvtdata, batch: batch, commitDoneCh: make(chan struct{})}
	if err := txmgr.invokeNamespaceListeners(); err != nil {
		return err
	}
	txmgr.current = &current

	return  nil
}

func (txmgr *LockBasedTxMgr) waitForPreviousToFinish() bool {
	if txmgr.current == nil {
		return true
	}

	select {
	case <-txmgr.current.commitDoneCh:
	case <-txmgr.doneCh:
		return false // the committer goroutine is shutting down - no new commits should be done.
	}

	return true
}

// RemoveStaleAndCommitPvtDataOfOldBlocks implements method in interface `txmgmt.TxMgr`
// The following six operations are performed:
// (1) contructs the unique pvt data from the passed blocksPvtData
// (2) acquire a lock on oldBlockCommit
// (3) checks for stale pvtData by comparing [version, valueHash] and removes stale data
// (4) creates update batch from the the non-stale pvtData
// (5) update the BTL bookkeeping managed by the purge manager and prepare expiring keys.
// (6) commit the non-stale pvt data to the stateDB
// This function assumes that the passed input contains only transactions that had been
// marked "Valid". In the current design, this assumption holds true as we store
// missing data info about only valid transactions. Further, gossip supplies only the
// missing pvtData of valid transactions. If these two assumptions are broken due to some bug,
// we are still safe from data consistency point of view as we match the version and the
// value hashes stored in the stateDB before committing the value. However, if pvtData of
// a tuple <ns, Coll, key> is passed for two (or more) transactions with one as valid and
// another as invalid transaction, we might miss to store a missing data forever if the
// version# of invalid tx is greater than the valid tx (as per our logic employed in
// constructUniquePvtData(). Other than a bug, there is another scenario in which this
// function might receive pvtData of both valid and invalid tx. Such a scenario is explained
// in FAB-12924 and is related to state fork and rebuilding ledger state.
func (txmgr *LockBasedTxMgr) RemoveStaleAndCommitPvtDataOfOldBlocks(blocksPvtData map[uint64][]*ledger.TxPvtData) error {
	// (1) as the blocksPvtData can contain multiple versions of pvtData for
	// a given <ns, coll, key>, we need to find duplicate tuples with different
	// versions and use the one with the higher version
	uniquePvtData, err := constructUniquePvtData(blocksPvtData)
	if len(uniquePvtData) == 0 || err != nil {
		return err
	}

	// (2) acquire a lock on oldBlockCommit. If the regular block commit has already
	// acquired this lock, commit of old blocks' pvtData cannot proceed until the lock
	// is released. This is required as the PrepareForExpiringKeys() used in step (5)
	// of this function might affect the result of DeleteExpiredAndUpdateBookkeeping()
	// in Commit()
	txmgr.oldBlockCommit.Lock()
	defer txmgr.oldBlockCommit.Unlock()

	// (3) remove the pvt data which does not matches the hashed
	// value stored in the public state
	if err := uniquePvtData.findAndRemoveStalePvtData(txmgr.db); err != nil {
		return err
	}

	// (4) create the update batch from the uniquePvtData
	batch := uniquePvtData.transformToUpdateBatch()

	// (5) update booking in the purge manager and prepare expiring keys.
	// Though the expiring keys would have been loaded in memory during last
	// PrepareExpiringKeys from Commit but we rerun this here because,
	// RemoveStaleAndCommitPvtDataOfOldBlocks may have added new data which might be
	// eligible for expiry during the next regular block commit.
	if err := txmgr.pvtdataPurgeMgr.UpdateBookkeepingForPvtDataOfOldBlocks(batch.PvtUpdates); err != nil {
		return err
	}

	if txmgr.lastCommittedBlockNum > 0 {
		txmgr.pvtdataPurgeMgr.PrepareForExpiringKeys(txmgr.lastCommittedBlockNum + 1)
	}

	// (6) commit the pvt data to the stateDB
	if err := txmgr.db.ApplyPrivacyAwareUpdates(batch, nil); err != nil {
		return err
	}
	return nil
}

type uniquePvtDataMap map[privacyenabledstate.HashedCompositeKey]*privacyenabledstate.PvtKVWrite

func constructUniquePvtData(blocksPvtData map[uint64][]*ledger.TxPvtData) (uniquePvtDataMap, error) {
	uniquePvtData := make(uniquePvtDataMap)
	// go over the blocksPvtData to find duplicate <ns, coll, key>
	// in the pvtWrites and use the one with the higher version number
	for blkNum, blockPvtData := range blocksPvtData {
		if err := uniquePvtData.updateUsingBlockPvtData(blockPvtData, blkNum); err != nil {
			return nil, err
		}
	} // for each block
	return uniquePvtData, nil
}

func (uniquePvtData uniquePvtDataMap) updateUsingBlockPvtData(blockPvtData []*ledger.TxPvtData, blkNum uint64) error {
	for _, txPvtData := range blockPvtData {
		ver := version.NewHeight(blkNum, txPvtData.SeqInBlock)
		if err := uniquePvtData.updateUsingTxPvtData(txPvtData, ver); err != nil {
			return err
		}
	} // for each tx
	return nil
}
func (uniquePvtData uniquePvtDataMap) updateUsingTxPvtData(txPvtData *ledger.TxPvtData, ver *version.Height) error {
	for _, nsPvtData := range txPvtData.WriteSet.NsPvtRwset {
		if err := uniquePvtData.updateUsingNsPvtData(nsPvtData, ver); err != nil {
			return err
		}
	} // for each ns
	return nil
}
func (uniquePvtData uniquePvtDataMap) updateUsingNsPvtData(nsPvtData *rwset.NsPvtReadWriteSet, ver *version.Height) error {
	for _, collPvtData := range nsPvtData.CollectionPvtRwset {
		if err := uniquePvtData.updateUsingCollPvtData(collPvtData, nsPvtData.Namespace, ver); err != nil {
			return err
		}
	} // for each coll
	return nil
}

func (uniquePvtData uniquePvtDataMap) updateUsingCollPvtData(collPvtData *rwset.CollectionPvtReadWriteSet,
	ns string, ver *version.Height) error {

	kvRWSet := &kvrwset.KVRWSet{}
	if err := proto.Unmarshal(collPvtData.Rwset, kvRWSet); err != nil {
		return err
	}

	hashedCompositeKey := privacyenabledstate.HashedCompositeKey{
		Namespace:      ns,
		CollectionName: collPvtData.CollectionName,
	}

	for _, kvWrite := range kvRWSet.Writes { // for each kv pair
		hashedCompositeKey.KeyHash = string(util.ComputeStringHash(kvWrite.Key))
		uniquePvtData.updateUsingPvtWrite(kvWrite, hashedCompositeKey, ver)
	} // for each kv pair

	return nil
}

func (uniquePvtData uniquePvtDataMap) updateUsingPvtWrite(pvtWrite *kvrwset.KVWrite,
	hashedCompositeKey privacyenabledstate.HashedCompositeKey, ver *version.Height) {

	pvtData, ok := uniquePvtData[hashedCompositeKey]
	if !ok || pvtData.Version.Compare(ver) < 0 {
		uniquePvtData[hashedCompositeKey] =
			&privacyenabledstate.PvtKVWrite{
				Key:      pvtWrite.Key,
				IsDelete: pvtWrite.IsDelete,
				Value:    pvtWrite.Value,
				Version:  ver,
			}
	}
}

func (uniquePvtData uniquePvtDataMap) findAndRemoveStalePvtData(db privacyenabledstate.DB) error {
	// (1) load all committed versions
	if err := uniquePvtData.loadCommittedVersionIntoCache(db); err != nil {
		return err
	}

	// (2) find and remove the stale data
	for hashedCompositeKey, pvtWrite := range uniquePvtData {
		isStale, err := checkIfPvtWriteIsStale(&hashedCompositeKey, pvtWrite, db)
		if err != nil {
			return err
		}
		if isStale {
			delete(uniquePvtData, hashedCompositeKey)
		}
	}
	return nil
}

func (uniquePvtData uniquePvtDataMap) loadCommittedVersionIntoCache(db privacyenabledstate.DB) error {
	// the regular block validation might have populate the cache already. In that scenario,
	// this call would be adding more entries to the existing cache. However, it does not affect
	// the correctness of regular block validation. If an entry already exist for a given
	// hashedCompositeKey, cache would not be updated.
	// Note that ClearCachedVersions would not be called till we validate and commit these
	// pvt data of old blocks. This is because only during the exclusive lock duration, we
	// clear the cache and we have already acquired one before reaching here.
	var hashedCompositeKeys []*privacyenabledstate.HashedCompositeKey
	for hashedCompositeKey := range uniquePvtData {
		hashedCompositeKeys = append(hashedCompositeKeys, &hashedCompositeKey)
	}

	err := db.LoadCommittedVersionsOfPubAndHashedKeys(nil, hashedCompositeKeys)
	if err != nil {
		return err
	}
	return nil
}

func checkIfPvtWriteIsStale(hashedKey *privacyenabledstate.HashedCompositeKey,
	kvWrite *privacyenabledstate.PvtKVWrite, db privacyenabledstate.DB) (bool, error) {

	ns := hashedKey.Namespace
	coll := hashedKey.CollectionName
	keyHashBytes := []byte(hashedKey.KeyHash)
	committedVersion, err := db.GetKeyHashVersion(ns, coll, keyHashBytes)
	if err != nil {
		return true, err
	}

	// for a deleted hashedKey, we would get a nil committed version. Note that
	// the hashedKey was deleted because either it got expired or was deleted by
	// the chaincode itself.
	if committedVersion == nil && kvWrite.IsDelete {
		return false, nil
	}

	/*
		TODO: FAB-12922
		In the first round, we need to the check version of passed pvtData
		against the version of pvtdata stored in the stateDB. In second round,
		for the remaining pvtData, we need to check for stalenss using hashed
		version. In the third round, for the still remaining pvtdata, we need
		to check against hashed values. In each phase we would require to
		perform bulkload of relevant data from the stateDB.
		committedPvtData, err := db.GetPrivateData(ns, coll, kvWrite.Key)
		if err != nil {
			return false, err
		}
		if committedPvtData.Version.Compare(kvWrite.Version) > 0 {
			return false, nil
		}
	*/
	if version.AreSame(committedVersion, kvWrite.Version) {
		return false, nil
	}

	// due to metadata updates, we could get a version
	// mismatch between pvt kv write and the committed
	// hashedKey. In this case, we must compare the hash
	// of the value. If the hash matches, we should update
	// the version number in the pvt kv write and return
	// true as the validation result
	vv, err := db.GetValueHash(ns, coll, keyHashBytes)
	if err != nil {
		return true, err
	}
	if bytes.Equal(vv.Value, util.ComputeHash(kvWrite.Value)) {
		// if hash of value matches, update version
		// and return true
		kvWrite.Version = vv.Version // side effect
		// (checkIfPvtWriteIsStale should not be updating the state)
		return false, nil
	}
	return true, nil
}

func (uniquePvtData uniquePvtDataMap) transformToUpdateBatch() *privacyenabledstate.UpdateBatch {
	batch := privacyenabledstate.NewUpdateBatch()
	for hashedCompositeKey, pvtWrite := range uniquePvtData {
		ns := hashedCompositeKey.Namespace
		coll := hashedCompositeKey.CollectionName
		if pvtWrite.IsDelete {
			batch.PvtUpdates.Delete(ns, coll, pvtWrite.Key, pvtWrite.Version)
		} else {
			batch.PvtUpdates.Put(ns, coll, pvtWrite.Key, pvtWrite.Value, pvtWrite.Version)
		}
	}
	return batch
}

func (txmgr *LockBasedTxMgr) invokeNamespaceListeners() error {
	for _, listener := range txmgr.stateListeners {
		stateUpdatesForListener := extractStateUpdates(txmgr.current.batch, listener.InterestedInNamespaces())
		if len(stateUpdatesForListener) == 0 {
			continue
		}
		txmgr.current.listeners = append(txmgr.current.listeners, listener)

		committedStateQueryExecuter := &queryutil.QECombiner{
			QueryExecuters: []queryutil.QueryExecuter{txmgr.db}}

		postCommitQueryExecuter := &queryutil.QECombiner{
			QueryExecuters: []queryutil.QueryExecuter{
				&queryutil.UpdateBatchBackedQueryExecuter{UpdateBatch: txmgr.current.batch.PubUpdates.UpdateBatch},
				txmgr.db,
			},
		}

		trigger := &ledger.StateUpdateTrigger{
			LedgerID:                    txmgr.ledgerid,
			StateUpdates:                stateUpdatesForListener,
			CommittingBlockNum:          txmgr.current.blockNum(),
			CommittedStateQueryExecutor: committedStateQueryExecuter,
			PostCommitQueryExecutor:     postCommitQueryExecuter,
		}
		if err := listener.HandleStateUpdates(trigger); err != nil {
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

	// wait for background go routine to finish else the timing issue causes a nil pointer inside goleveldb code
	// see FAB-11974
	txmgr.pvtdataPurgeMgr.WaitForPrepareToFinish()
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
	if txmgr.current == nil {
		panic("validateAndPrepare() method should have been called before calling rollback()")
	}
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
	if  err := txmgr.ValidateAndPrepare(blockAndPvtdata, false); err != nil {
		return err
	}

	// log every 1000th block at Info level so that statedb rebuild progress can be tracked in production envs.
	if block.Header.Number%1000 == 0 {
		logger.Infof("Recommitting block [%d] to state database", block.Header.Number)
	} else {
		logger.Debugf("Recommitting block [%d] to state database", block.Header.Number)
	}

	return txmgr.Commit()
}

//committer commits update batch from incoming commitCh items
//TODO panic may not be required for some errors
//TODO metrics
func (txmgr *LockBasedTxMgr) committer() {

	const panicMsg = "commit failure"

	for {
		select {
		case <-txmgr.doneCh:
			close(txmgr.shutdownCh)
			return
		case current := <-txmgr.commitCh:
			/*var commitWatch tally.Stopwatch
			if metrics.IsDebug() {
				// Measure the whole
				commitWatch = metrics.RootScope.Timer("lockbasedtxmgr_Commit_duration").Start()
			}*/

			// we need to acquire a lock on oldBlockCommit. This is required because
			// the DeleteExpiredAndUpdateBookkeeping() would perform incorrect operation if
			// PrepareForExpiringKeys() in RemoveStaleAndCommitPvtDataOfOldBlocks() is allowed to
			// execute parallely. RemoveStaleAndCommitPvtDataOfOldBlocks computes the update
			// batch based on the current state and if we allow regular block commits at the
			// same time, the former may overwrite the newer versions of the data and we may
			// end up with an incorrect update batch.
			txmgr.oldBlockCommit.Lock()

			// When using the purge manager for the first block commit after peer start, the asynchronous function
			// 'PrepareForExpiringKeys' is invoked in-line. However, for the subsequent blocks commits, this function is invoked
			// in advance for the next block
			if !txmgr.pvtdataPurgeMgr.usedOnce {
				//stopWatch := metrics.RootScope.Timer("lockbasedtxmgr_Commit_PrepareForExpiringKeys_duration").Start()
				txmgr.pvtdataPurgeMgr.PrepareForExpiringKeys(current.blockNum())
				txmgr.pvtdataPurgeMgr.usedOnce = true
				//stopWatch.Stop()
			}

			forExpiry := current.blockNum() + 1

			if err := txmgr.pvtdataPurgeMgr.RemoveNonDurable(
				current.batch.PvtUpdates, current.batch.HashUpdates); err != nil {
				logger.Errorf("failed to remove non durable : %s", err)
				panic(panicMsg)
			}

			//purgeWatch := metrics.RootScope.Timer("lockbasedtxmgr_Commit_DeleteExpiredAndUpdateBookkeeping_duration").Start()
			if err := txmgr.pvtdataPurgeMgr.DeleteExpiredAndUpdateBookkeeping(
				current.batch.PvtUpdates, current.batch.HashUpdates); err != nil {
				logger.Errorf("failed to delete expired and update booking : %s", err)
				panic(panicMsg)
				//purgeWatch.Stop()
			}
			//purgeWatch.Stop()

			//lockWatch := metrics.RootScope.Timer("lockbasedtxmgr_Commit_commitRWLock_duration").Start()
			txmgr.commitRWLock.Lock()
			//lockWatch.Stop()
			logger.Debugf("Write lock acquired for committing updates to state database")

			commitHeight := version.NewHeight(current.blockNum(), current.maxTxNumber())
			//applyUpdateWatch := metrics.RootScope.Timer("lockbasedtxmgr_Commit_ApplyPrivacyAwareUpdates_duration").Start()
			if err := txmgr.db.ApplyPrivacyAwareUpdates(current.batch, commitHeight); err != nil {
				logger.Errorf("failed to apply updates : %s", err)
				txmgr.commitRWLock.Unlock()
				//applyUpdateWatch.Stop()
				panic(panicMsg)
			}
			//applyUpdateWatch.Stop()
			logger.Debugf("Updates committed to state database")

			// purge manager should be called (in this call the purge mgr removes the expiry entries from schedules) after committing to statedb
			//blkCommitWatch := metrics.RootScope.Timer("lockbasedtxmgr_Commit_BlockCommitDone_duration").Start()
			if err := txmgr.pvtdataPurgeMgr.BlockCommitDone(); err != nil {
				logger.Errorf("failed to purge expiry entries from schedules : %s", err)
				txmgr.commitRWLock.Unlock()
				//blkCommitWatch.Stop()
				panic(panicMsg)
			}
			//blkCommitWatch.Stop()

			// In the case of error state listeners will not recieve this call - instead a peer panic is caused by the ledger upon receiveing
			// an error from this function
			//updateListnWatch := metrics.RootScope.Timer("lockbasedtxmgr_Commit_updateStateListeners_duration").Start()
			txmgr.updateStateListeners(current)
			//updateListnWatch.Stop()

			//clean up and prepare for expiring keys
			//clearWatch := metrics.RootScope.Timer("lockbasedtxmgr_Commit_defer_duration").Start()
			txmgr.clearCache()
			txmgr.pvtdataPurgeMgr.PrepareForExpiringKeys(forExpiry)
			logger.Debugf("Cleared version cache and launched the background routine for preparing keys to purge with the next block")
			//clearWatch.Stop()

			txmgr.commitRWLock.Unlock()
			close(current.commitDoneCh)

			txmgr.lastCommittedBlockNum = txmgr.current.blockNum()
			txmgr.oldBlockCommit.Unlock()

			//notify kv ledger that commit is done for given block and private data
			txmgr.commitDone <- current.blockAndPvtData

			/*if metrics.IsDebug() {
				commitWatch.Stop()
			}*/
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

func (txmgr *LockBasedTxMgr) updateStateListeners(tx *update) {
	//stopWatch := metrics.StopWatch("lockbasedtxmgr_updateStateListenersTimer_duration")
	//defer stopWatch()

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

func (txmgr *LockBasedTxMgr) reset() {
	txmgr.current = nil
}

// pvtdataPurgeMgr wraps the actual purge manager and an additional flag 'usedOnce'
// for usage of this additional flag, see the relevant comments in the txmgr.Commit() function above
type pvtdataPurgeMgr struct {
	pvtstatepurgemgmt.PurgeMgr
	usedOnce bool
}
