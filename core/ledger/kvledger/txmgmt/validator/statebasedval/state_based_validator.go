/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package statebasedval

import (
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/privacyenabledstate"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/validator/internal"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/core/ledger/util"
	"github.com/hyperledger/fabric/protos/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric/protos/peer"
	"github.com/spf13/viper"
	"golang.org/x/sync/semaphore"
	"golang.org/x/net/context"
	"runtime"
)

var logger = flogging.MustGetLogger("statebasedval")
const (
	preLoadCommittedVersionOfWSetQueueLen = 1
)
// Validator validates a tx against the latest committed state
// and preceding valid transactions with in the same block
type Validator struct {
	db                              privacyenabledstate.DB
	preLoadCommittedVersionOfWSetCh chan *blockAndPvtData
	semaphore                       *semaphore.Weighted
}

//blockAndPvtData contain current block and pvt data
type blockAndPvtData struct {
	block   *internal.Block
	pvtdata map[uint64]*ledger.TxPvtData
	txFilter util.TxFilter

}

// NewValidator constructs StateValidator
func NewValidator(channelID string, db privacyenabledstate.DB) *Validator {
	nWorkers := viper.GetInt("peer.validatorPoolSize")
	if nWorkers <= 0 {
		nWorkers = runtime.NumCPU()
	}

	v := &Validator{
		db: db,
		preLoadCommittedVersionOfWSetCh: make(chan *blockAndPvtData, preLoadCommittedVersionOfWSetQueueLen),
		semaphore:                       semaphore.NewWeighted(int64(nWorkers)),
	}
	go v.preLoadCommittedVersionOfWSet()
	return v
}


// preLoadCommittedVersionOfRSet loads committed version of all keys in each
// transaction's read set into a cache.
func (v *Validator) preLoadCommittedVersionOfRSet(block *internal.Block, shouldLoad util.TxFilter) error {

	// Collect both public and hashed keys in read sets of all transactions in a given block
	var pubKeys []*statedb.CompositeKey
	var hashedKeys []*privacyenabledstate.HashedCompositeKey

	// pubKeysMap and hashedKeysMap are used to avoid duplicate entries in the
	// pubKeys and hashedKeys. Though map alone can be used to collect keys in
	// read sets and pass as an argument in LoadCommittedVersionOfPubAndHashedKeys(),
	// array is used for better code readability. On the negative side, this approach
	// might use some extra memory.
	pubKeysMap := make(map[statedb.CompositeKey]interface{})
	hashedKeysMap := make(map[privacyenabledstate.HashedCompositeKey]interface{})

	for _, tx := range block.Txs {
		if !shouldLoad(tx.IndexInBlock) {
			continue
		}

		logger.Debugf("Pre-loading committed versions for TxIdx %d in block %d", tx.IndexInBlock, block.Num)

		for _, nsRWSet := range tx.RWSet.NsRwSets {
			for _, kvRead := range nsRWSet.KvRwSet.Reads {
				compositeKey := statedb.CompositeKey{
					Namespace: nsRWSet.NameSpace,
					Key:       kvRead.Key,
				}
				if _, ok := pubKeysMap[compositeKey]; !ok {
					pubKeysMap[compositeKey] = nil
					pubKeys = append(pubKeys, &compositeKey)
				}

			}
			for _, colHashedRwSet := range nsRWSet.CollHashedRwSets {
				for _, kvHashedRead := range colHashedRwSet.HashedRwSet.HashedReads {
					hashedCompositeKey := privacyenabledstate.HashedCompositeKey{
						Namespace:      nsRWSet.NameSpace,
						CollectionName: colHashedRwSet.CollectionName,
						KeyHash:        string(kvHashedRead.KeyHash),
					}
					if _, ok := hashedKeysMap[hashedCompositeKey]; !ok {
						hashedKeysMap[hashedCompositeKey] = nil
						hashedKeys = append(hashedKeys, &hashedCompositeKey)
					}
				}
			}
		}
	}

	// Load committed version of all keys into a cache
	if len(pubKeys) > 0 || len(hashedKeys) > 0 {
		err := v.db.LoadCommittedVersionsOfPubAndHashedKeys(pubKeys, hashedKeys)
		if err != nil {
			return err
		}
	}

	return nil
}

// preLoadCommittedVersionOfWSet loads committed version of all keys in each
// transaction's write set into a cache.
func (v *Validator) preLoadCommittedVersionOfWSet() {
	/*stopWatch := metrics.StopWatch("validator_preload_committed_version_wset_duration")
	defer stopWatch()*/

	for {
		select {
		case data := <-v.preLoadCommittedVersionOfWSetCh:
			v.db.GetWSetCacheLock().Lock()
			// Collect both public and hashed keys in read sets of all transactions in a given block
			var pubKeys []*statedb.CompositeKey
			var hashedKeys []*privacyenabledstate.HashedCompositeKey
			var pvtKeys []*privacyenabledstate.PvtdataCompositeKey

			// pubKeysMap and hashedKeysMap are used to avoid duplicate entries in the
			// pubKeys and hashedKeys. Though map alone can be used to collect keys in
			// read sets and pass as an argument in LoadCommittedVersionOfPubAndHashedKeys(),
			// array is used for better code readability. On the negative side, this approach
			// might use some extra memory.
			pubKeysMap := make(map[statedb.CompositeKey]interface{})
			hashedKeysMap := make(map[privacyenabledstate.HashedCompositeKey]interface{})
			pvtKeysMap := make(map[privacyenabledstate.PvtdataCompositeKey]interface{})

			for i, tx := range data.block.Txs {
				if !data.txFilter(i) {
					continue
				}
				for _, nsRWSet := range tx.RWSet.NsRwSets {
					logger.Debugf("Pre-loading %d write sets for Tx index %d in block %d", len(nsRWSet.KvRwSet.Writes), i, data.block.Num)
					for _, kvWrite := range nsRWSet.KvRwSet.Writes {
						compositeKey := statedb.CompositeKey{
							Namespace: nsRWSet.NameSpace,
							Key:       kvWrite.Key,
						}
						if _, ok := pubKeysMap[compositeKey]; !ok {
							pubKeysMap[compositeKey] = nil
							pubKeys = append(pubKeys, &compositeKey)
						}

					}
					logger.Debugf("Pre-loading %d coll hashed write sets for Tx index %d in block %d", len(nsRWSet.CollHashedRwSets), i, data.block.Num)
					for _, colHashedRwSet := range nsRWSet.CollHashedRwSets {
						for _, kvHashedWrite := range colHashedRwSet.HashedRwSet.HashedWrites {
							hashedCompositeKey := privacyenabledstate.HashedCompositeKey{
								Namespace:      nsRWSet.NameSpace,
								CollectionName: colHashedRwSet.CollectionName,
								KeyHash:        string(kvHashedWrite.KeyHash),
							}
							if _, ok := hashedKeysMap[hashedCompositeKey]; !ok {
								hashedKeysMap[hashedCompositeKey] = nil
								hashedKeys = append(hashedKeys, &hashedCompositeKey)
							}
						}
					}
				}
			}
			for _, txPvtdata := range data.pvtdata {
				pvtRWSet, err := rwsetutil.TxPvtRwSetFromProtoMsg(txPvtdata.WriteSet)
				if err != nil {
					logger.Errorf("TxPvtRwSetFromProtoMsg failed %s", err)
					continue
				}
				for _, nsPvtdata := range pvtRWSet.NsPvtRwSet {
					for _, collPvtRwSets := range nsPvtdata.CollPvtRwSets {
						ns := nsPvtdata.NameSpace
						coll := collPvtRwSets.CollectionName
						if err != nil {
							logger.Errorf("collPvtRwSets.CollectionName failed %s", err)
							continue
						}
						logger.Debugf("Pre-loading %d private data write sets in block %d", len(collPvtRwSets.KvRwSet.Writes), data.block.Num)
						for _, write := range collPvtRwSets.KvRwSet.Writes {
							pvtCompositeKey := privacyenabledstate.PvtdataCompositeKey{
								Namespace:      ns,
								Key:            write.Key,
								CollectionName: coll,
							}
							if _, ok := pvtKeysMap[pvtCompositeKey]; !ok {
								pvtKeysMap[pvtCompositeKey] = nil
								pvtKeys = append(pvtKeys, &pvtCompositeKey)
							}
						}
					}
				}

			}

			// Load committed version of all keys into a cache
			if len(pubKeys) > 0 || len(hashedKeys) > 0 || len(pvtKeys) > 0 {
				err := v.db.LoadWSetCommittedVersionsOfPubAndHashedKeys(pubKeys, hashedKeys, pvtKeys, data.block.Num)
				if err != nil {
					logger.Errorf("LoadCommittedVersionsOfPubAndHashedKeys failed %s", err)
				}
			}
			v.db.GetWSetCacheLock().Unlock()
			stopWatch()

		}
	}

}
type validationResponse struct {
	txIdx int
	err   error
}
// ValidateMVCC validates block for MVCC conflicts and phantom reads against committed data
func (v *Validator) ValidateMVCC(ctx context.Context,block *internal.Block, txsFilter util.TxValidationFlags, acceptTx util.TxFilter) error {
	// Check whether statedb implements BulkOptimizable interface. For now,
	// only CouchDB implements BulkOptimizable to reduce the number of REST
	// API calls from peer to CouchDB instance.
	if v.db.IsBulkOptimizable() {
		err := v.preLoadCommittedVersionOfRSet(block, acceptTx)
		if err != nil {
			return err
		}
		v.preLoadCommittedVersionOfWSetCh <- &blockAndPvtData{block: block, txFilter: acceptTx}
	}

	var txs []*internal.Transaction
	for _, tx := range block.Txs {
		if !acceptTx(tx.IndexInBlock) {
			continue
		}

		txStatus := txsFilter.Flag(tx.IndexInBlock)
		if txStatus != peer.TxValidationCode_NOT_VALIDATED {
			logger.Debugf("Not performing MVCC validation of transaction index [%d] in block [%d] TxId [%s] since it has already been set to %s", tx.IndexInBlock, block.Num, tx.ID, txStatus)
			continue
		}
		txs = append(txs, tx)
	}

	responseChan := make(chan *validationResponse)

	go func() {
		for _, tx := range txs {
			// ensure that we don't have too many concurrent validation workers
			v.semaphore.Acquire(context.Background(), 1)

			go func(tx *internal.Transaction) {
				defer v.semaphore.Release(1)

				validationCode, err := v.validateTxMVCC(tx.RWSet)
				if err != nil {
					logger.Infof("Got error validating block for MVCC conflicts [%d] Transaction index [%d] TxId [%s]", block.Num, tx.IndexInBlock, tx.ID)
					responseChan <- &validationResponse{txIdx: tx.IndexInBlock, err: err}
					return
				}

				if validationCode == peer.TxValidationCode_VALID {
					logger.Debugf("MVCC validation of block [%d] at TxIdx [%d] and TxId [%s] marked as valid by state validator. Reason code [%s]",
						block.Num, tx.IndexInBlock, tx.ID, validationCode.String())
				} else {
					logger.Infof("MVCC validation of block [%d] Transaction index [%d] TxId [%s] marked as invalid by state validator. Reason code [%s]",
						block.Num, tx.IndexInBlock, tx.ID, validationCode.String())
				}
				tx.ValidationCode = validationCode
				responseChan <- &validationResponse{txIdx: tx.IndexInBlock}
			}(tx)
		}
	}()

	// Wait for all responses
	var err error
	for i := 0; i < len(txs); i++ {
		response := <-responseChan
		if response.err != nil {
			logger.Warningf("Error in MVCC validation of block [%d] at TxIdx [%d]: %s", block.Num, response.txIdx, response.err)
			// Just set the error and wait for all responses
			err = response.err
		}
	}

	return err
}

// ValidateAndPrepareBatch implements method in Validator interface
func (v *Validator) ValidateAndPrepareBatch(block *internal.Block, doMVCCValidation bool, pvtdata map[uint64]*ledger.TxPvtData) (*internal.PubAndHashUpdates, error) {
	// Check whether statedb implements BulkOptimizable interface. For now,
	// only CouchDB implements BulkOptimizable to reduce the number of REST
	// API calls from peer to CouchDB instance.
	if v.db.IsBulkOptimizable() {
		// Just preload the private data since the block has already been preloaded during MVCC validation
		v.preLoadCommittedVersionOfWSetCh <- &blockAndPvtData{block: &internal.Block{Num: block.Num}, pvtdata: pvtdata}
	}

	updates := internal.NewPubAndHashUpdates()
	for _, tx := range block.Txs {
		if tx.ValidationCode != peer.TxValidationCode_VALID {
			continue
		}
		var validationCode peer.TxValidationCode
		var err error
		if validationCode, err = v.validateEndorserTX(tx.RWSet, doMVCCValidation, updates); err != nil {
			logger.Infof("Got error validating block [%d] Transaction index [%d] TxId [%s]: %s", block.Num, tx.IndexInBlock, tx.ID, err)
			return nil, err
		}

		tx.ValidationCode = validationCode
		if validationCode == peer.TxValidationCode_VALID {
			logger.Debugf("Validating Block [%d] Transaction index [%d] TxId [%s] marked as valid by state validator", block.Num, tx.IndexInBlock, tx.ID)
			committingTxHeight := version.NewHeight(block.Num, uint64(tx.IndexInBlock))
			updates.ApplyWriteSet(tx.RWSet, committingTxHeight, v.db)
		} else {
			logger.Warningf("Validating Block [%d] Transaction index [%d] TxId [%s] marked as invalid by state validator. Reason code [%s]",
				block.Num, tx.IndexInBlock, tx.ID, validationCode.String())
		}
	}
	return updates, nil
}

// validateEndorserTX validates endorser transaction
func (v *Validator) validateEndorserTX(
	txRWSet *rwsetutil.TxRwSet,
	doMVCCValidation bool,
	updates *internal.PubAndHashUpdates) (peer.TxValidationCode, error) {

	var validationCode = peer.TxValidationCode_VALID
	var err error
	//mvccvalidation, may invalidate transaction
	if doMVCCValidation {
		validationCode, err = v.validateTx(txRWSet, updates)
	}
	return validationCode, err
}
func (v *Validator) validateTxMVCC(txRWSet *rwsetutil.TxRwSet) (peer.TxValidationCode, error) {
	// Uncomment the following only for local debugging. Don't want to print data in the logs in production
	//logger.Debugf("validateTx - validating txRWSet: %s", spew.Sdump(txRWSet))
	for _, nsRWSet := range txRWSet.NsRwSets {
		ns := nsRWSet.NameSpace
		// Validate public reads
		if valid, err := v.validateReadSetMVCC(ns, nsRWSet.KvRwSet.Reads); !valid || err != nil {
			if err != nil {
				return peer.TxValidationCode(-1), err
			}
			return peer.TxValidationCode_MVCC_READ_CONFLICT, nil
		}
		// Validate range queries for phantom items
		if valid, err := v.validateRangeQueriesMVCC(ns, nsRWSet.KvRwSet.RangeQueriesInfo); !valid || err != nil {
			if err != nil {
				return peer.TxValidationCode(-1), err
			}
			return peer.TxValidationCode_PHANTOM_READ_CONFLICT, nil
		}
		// Validate hashes for private reads
		if valid, err := v.validateNsHashedReadSetsMVCC(ns, nsRWSet.CollHashedRwSets); !valid || err != nil {
			if err != nil {
				return peer.TxValidationCode(-1), err
			}
			return peer.TxValidationCode_MVCC_READ_CONFLICT, nil
		}
	}
	return peer.TxValidationCode_VALID, nil
}
func (v *Validator) validateTx(txRWSet *rwsetutil.TxRwSet, updates *internal.PubAndHashUpdates) (peer.TxValidationCode, error) {
	// Uncomment the following only for local debugging. Don't want to print data in the logs in production
	//logger.Debugf("validateTx - validating txRWSet: %s", spew.Sdump(txRWSet))
	for _, nsRWSet := range txRWSet.NsRwSets {
		ns := nsRWSet.NameSpace
		// Validate public reads
		if valid, err := v.validateReadSet(ns, nsRWSet.KvRwSet.Reads, updates.PubUpdates); !valid || err != nil {
			if err != nil {
				return peer.TxValidationCode(-1), err
			}
			return peer.TxValidationCode_MVCC_READ_CONFLICT, nil
		}
		// Validate range queries for phantom items
		if valid, err := v.validateRangeQueries(ns, nsRWSet.KvRwSet.RangeQueriesInfo, updates.PubUpdates); !valid || err != nil {
			if err != nil {
				return peer.TxValidationCode(-1), err
			}
			return peer.TxValidationCode_PHANTOM_READ_CONFLICT, nil
		}
		// Validate hashes for private reads
		if valid, err := v.validateNsHashedReadSets(ns, nsRWSet.CollHashedRwSets, updates.HashUpdates); !valid || err != nil {
			if err != nil {
				return peer.TxValidationCode(-1), err
			}
			return peer.TxValidationCode_MVCC_READ_CONFLICT, nil
		}
	}
	return peer.TxValidationCode_VALID, nil
}

////////////////////////////////////////////////////////////////////////////////
/////                 Validation of public read-set
////////////////////////////////////////////////////////////////////////////////
func (v *Validator) validateReadSet(ns string, kvReads []*kvrwset.KVRead, updates *privacyenabledstate.PubUpdateBatch) (bool, error) {
	for _, kvRead := range kvReads {
		if valid, err := v.validateKVRead(ns, kvRead, updates); !valid || err != nil {
			return valid, err
		}
	}
	return true, nil
}

func (v *Validator) validateReadSetMVCC(ns string, kvReads []*kvrwset.KVRead) (bool, error) {
	for _, kvRead := range kvReads {
		if valid, err := v.validateKVReadMVCC(ns, kvRead); !valid || err != nil {
			return valid, err
		}
	}
	return true, nil
}

func (v *Validator) validateKVReadMVCC(ns string, kvRead *kvrwset.KVRead) (bool, error) {
	committedVersion, err := v.db.GetVersion(ns, kvRead.Key)
	if err != nil {
		return false, err
	}

	logger.Debugf("Comparing versions for key [%s]: committed version=%#v and read version=%#v",
		kvRead.Key, committedVersion, rwsetutil.NewVersion(kvRead.Version))
	if !version.AreSame(committedVersion, rwsetutil.NewVersion(kvRead.Version)) {
		logger.Debugf("Version mismatch for key [%s:%s]. Committed version = [%#v], Version in readSet [%#v]",
			ns, kvRead.Key, committedVersion, kvRead.Version)
		return false, nil
	}
	return true, nil
}

// validateKVRead performs mvcc check for a key read during transaction simulation.
// i.e., it checks whether a key/version combination is already updated in the statedb (by an already committed block)
// or in the updates (by a preceding valid transaction in the current block)
func (v *Validator) validateKVRead(ns string, kvRead *kvrwset.KVRead, updates *privacyenabledstate.PubUpdateBatch) (bool, error) {
	if updates.Exists(ns, kvRead.Key) {
		logger.Debugf("Returning invalid since there were updates to [%s]", kvRead.Key)
		return false, nil
	}
	return true, nil
}

////////////////////////////////////////////////////////////////////////////////
/////                 Validation of range queries
////////////////////////////////////////////////////////////////////////////////
func (v *Validator) validateRangeQueriesMVCC(ns string, rangeQueriesInfo []*kvrwset.RangeQueryInfo) (bool, error) {
	for _, rqi := range rangeQueriesInfo {
		if valid, err := v.validateRangeQueryMVCC(ns, rqi); !valid || err != nil {
			return valid, err
		}
	}
	return true, nil
}

func (v *Validator) validateRangeQueries(ns string, rangeQueriesInfo []*kvrwset.RangeQueryInfo, updates *privacyenabledstate.PubUpdateBatch) (bool, error) {
	for _, rqi := range rangeQueriesInfo {
		if valid, err := v.validateRangeQuery(ns, rqi, updates); !valid || err != nil {
			return valid, err
		}
	}
	return true, nil
}

// validateRangeQueryMVCC performs a phantom read check i.e., it
// checks whether the results of the range query are still the same when executed on the
// statedb (latest state as of last committed block)
func (v *Validator) validateRangeQueryMVCC(ns string, rangeQueryInfo *kvrwset.RangeQueryInfo) (bool, error) {
	logger.Debugf("validateRangeQueryMVCC: ns=%s, rangeQueryInfo=%s", ns, rangeQueryInfo)

	var validator rangeQueryValidator
	if rangeQueryInfo.GetReadsMerkleHashes() != nil {
		validator = &rangeQueryHashValidator{}
	} else {
		validator = &rangeQueryResultsValidator{}
	}

	itr, err := v.db.GetStateRangeScanIterator(ns, rangeQueryInfo.StartKey, rangeQueryInfo.EndKey)
	if err != nil {
		return false, err
	}
	defer itr.Close()

	validator.init(rangeQueryInfo, itr)
	return validator.validate()
}

// validateRangeQuery performs a phantom read check i.e., it
// checks whether the results of the range query exist in the updates (prepared by the writes of preceding valid transactions
// in the current block and yet to be committed as part of group commit at the end of the validation of the block)
func (v *Validator) validateRangeQuery(ns string, rangeQueryInfo *kvrwset.RangeQueryInfo, updates *privacyenabledstate.PubUpdateBatch) (bool, error) {
	logger.Debugf("validateRangeQuery: ns=%s, rangeQueryInfo=%s", ns, rangeQueryInfo)

	// If during simulation, the caller had not exhausted the iterator so
	// rangeQueryInfo.EndKey is not actual endKey given by the caller in the range query
	// but rather it is the last key seen by the caller and hence the combinedItr should include the endKey in the results.
	var itr statedb.ResultsIterator
	if rangeQueryInfo.ItrExhausted {
		itr = updates.GetRangeScanIterator(ns, rangeQueryInfo.StartKey, rangeQueryInfo.EndKey)
	} else {
		itr = updates.GetRangeScanIteratorIncludingEndKey(ns, rangeQueryInfo.StartKey, rangeQueryInfo.EndKey)
	}
	defer itr.Close()
	return newRangeQueryUpdatesValidator(rangeQueryInfo, itr).validate()
}

////////////////////////////////////////////////////////////////////////////////
/////                 Validation of hashed read-set
////////////////////////////////////////////////////////////////////////////////
func (v *Validator) validateNsHashedReadSetsMVCC(ns string, collHashedRWSets []*rwsetutil.CollHashedRwSet) (bool, error) {
	for _, collHashedRWSet := range collHashedRWSets {
		if valid, err := v.validateCollHashedReadSetMVCC(ns, collHashedRWSet.CollectionName, collHashedRWSet.HashedRwSet.HashedReads); !valid || err != nil {
			return valid, err
		}
	}
	return true, nil
}

func (v *Validator) validateNsHashedReadSets(ns string, collHashedRWSets []*rwsetutil.CollHashedRwSet,
	updates *privacyenabledstate.HashedUpdateBatch) (bool, error) {
	for _, collHashedRWSet := range collHashedRWSets {
		if valid, err := v.validateCollHashedReadSet(ns, collHashedRWSet.CollectionName, collHashedRWSet.HashedRwSet.HashedReads, updates); !valid || err != nil {
			return valid, err
		}
	}
	return true, nil
}

func (v *Validator) validateCollHashedReadSetMVCC(ns, coll string, kvReadHashes []*kvrwset.KVReadHash) (bool, error) {
	for _, kvReadHash := range kvReadHashes {
		if valid, err := v.validateKVReadHashMVCC(ns, coll, kvReadHash); !valid || err != nil {
			return valid, err
		}
	}
	return true, nil
}

func (v *Validator) validateCollHashedReadSet(ns, coll string, kvReadHashes []*kvrwset.KVReadHash,
	updates *privacyenabledstate.HashedUpdateBatch) (bool, error) {
	for _, kvReadHash := range kvReadHashes {
		if valid, err := v.validateKVReadHash(ns, coll, kvReadHash, updates); !valid || err != nil {
			return valid, err
		}
	}
	return true, nil
}

// validateKVReadHashMVCC performs mvcc check for a hash of a key that is present in the private data space
// i.e., it checks whether a key/version combination is already updated in the statedb (by an already committed block)
func (v *Validator) validateKVReadHashMVCC(ns, coll string, kvReadHash *kvrwset.KVReadHash) (bool, error) {
	committedVersion, err := v.db.GetKeyHashVersion(ns, coll, kvReadHash.KeyHash)
	if err != nil {
		return false, err
	}

	if !version.AreSame(committedVersion, rwsetutil.NewVersion(kvReadHash.Version)) {
		logger.Debugf("Version mismatch for key hash [%s:%s:%#v]. Committed version = [%s], Version in hashedReadSet [%s]",
			ns, coll, kvReadHash.KeyHash, committedVersion, kvReadHash.Version)
		return false, nil
	}
	return true, nil
}

// validateKVReadHash performs mvcc check for a hash of a key that is present in the private data space
// i.e., it checks whether a key/version combination has already been updated by a preceding valid transaction in the current block
func (v *Validator) validateKVReadHash(ns, coll string, kvReadHash *kvrwset.KVReadHash, updates *privacyenabledstate.HashedUpdateBatch) (bool, error) {
	if updates.Contains(ns, coll, kvReadHash.KeyHash) {
		return false, nil
	}
	return true, nil
}
