/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbpvtdata

import (
	"fmt"
	"sort"
	"sync"

	"encoding/hex"

	"sync/atomic"

	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/core/ledger/pvtdatapolicy"
	"github.com/hyperledger/fabric/core/ledger/pvtdatastorage"
	"github.com/hyperledger/fabric/protos/ledger/rwset"
	"github.com/pkg/errors"
	"github.com/willf/bitset"
)

// TODO: This file contains code copied from the base private data store. Both of these packages should be refactored.

type commonStore struct {
	ledgerid           string
	btlPolicy          pvtdatapolicy.BTLPolicy
	isEmpty            bool
	lastCommittedBlock uint64
	purgerLock         sync.Mutex
	// After committing the pvtdata of old blocks,
	// the `isLastUpdatedOldBlocksSet` is set to true.
	// Once the stateDB is updated with these pvtdata,
	// the `isLastUpdatedOldBlocksSet` is set to false.
	// isLastUpdatedOldBlocksSet is mainly used during the
	// recovery process. During the peer startup, if the
	// isLastUpdatedOldBlocksSet is set to true, the pvtdata
	// in the stateDB needs to be updated before finishing the
	// recovery operation.
	isLastUpdatedOldBlocksSet bool
}

// lastUpdatedOldBlocksList keeps the list of last updated blocks
// and is stored as the value of lastUpdatedOldBlocksKey (defined in kv_encoding.go)
type lastUpdatedOldBlocksList []uint64

type entriesForPvtDataOfOldBlocks struct {
	// for each <ns, coll, blkNum, txNum>, store the dataEntry, i.e., pvtData
	dataEntries map[dataKey]*rwset.CollectionPvtReadWriteSet
	// store the retrieved (& updated) expiryData in expiryEntries
	expiryEntries map[expiryKey]*ExpiryData
	// for each <ns, coll, blkNum>, store the retrieved (& updated) bitmap in the missingDataEntries
	missingDataEntries map[nsCollBlk]*bitset.BitSet
}

type blkTranNumKey []byte

type dataEntry struct {
	key   *dataKey
	value *rwset.CollectionPvtReadWriteSet
}

type expiryEntry struct {
	key   *expiryKey
	value *ExpiryData
}

type expiryKey struct {
	expiringBlk   uint64
	committingBlk uint64
}

type nsCollBlk struct {
	ns, coll string
	blkNum   uint64
}

type dataKey struct {
	nsCollBlk
	txNum uint64
	//TODO set purge value
	purge bool
}

type missingDataKey struct {
	nsCollBlk
	isEligible bool
	//TODO set purge value
	purge bool
}

type storeEntries struct {
	dataEntries        []*dataEntry
	expiryEntries      []*expiryEntry
	missingDataEntries map[missingDataKey]*bitset.BitSet
}

func (s *store) Init(btlPolicy pvtdatapolicy.BTLPolicy) {
	s.btlPolicy = btlPolicy
}

func (s *store) Prepare(blockNum uint64, pvtData []*ledger.TxPvtData, missingPvtData ledger.TxMissingPvtDataMap) error {
	//TODO:TxMissingPvtDataMap to handle
	if !ledgerconfig.IsCommitter() {
		panic("calling Prepare on a peer that is not a committer")
	}

	/*	stopWatch := metrics.StopWatch("pvtdatastorage_couchdb_prepare_duration")
		defer stopWatch()*/

	if s.checkPendingPvt(blockNum) {
		return pvtdatastorage.NewErrIllegalCall(`A pending batch exists as as result of last invoke to "Prepare" call. Invoke "Commit" or "Rollback" on the pending batch before invoking "Prepare" function`)
	}

	if s.lastCommittedBlock > blockNum {
		return pvtdatastorage.NewErrIllegalArgs(fmt.Sprintf("Last committed block number in pvt store=%d is greater than recived block number=%d. Cannot prepare an old block # for commit.", s.lastCommittedBlock, blockNum))
	}

	err := s.prepareDB(blockNum, pvtData, missingPvtData)
	if err != nil {
		return err
	}

	logger.Debugf("Saved %d private data write sets for block [%d]", len(pvtData), blockNum)

	return nil
}

func (s *store) Commit(blockNum uint64) error {
	if !ledgerconfig.IsCommitter() {
		panic("calling Commit on a peer that is not a committer")
	}

	/*stopWatch := metrics.StopWatch("pvtdatastorage_couchdb_commit_duration")
	defer stopWatch()*/

	if !s.checkPendingPvt(blockNum) {
		logger.Debugf("There are no committed private data for block [%d] - setting lastCommittedBlock, isEmpty=false and calling performPurgeIfScheduled", blockNum)
		s.lastCommittedBlock = blockNum
		s.isEmpty = false
		s.performPurgeIfScheduled(blockNum)
		return nil
	}

	err := s.commitDB(blockNum)
	if err != nil {
		return err
	}

	s.isEmpty = false
	s.lastCommittedBlock = blockNum
	logger.Debugf("Committed private data for block [%d]", blockNum)
	s.performPurgeIfScheduled(blockNum)
	return nil
}

func (s *store) InitLastCommittedBlock(blockNum uint64) error {
	/*stopWatch := metrics.StopWatch("pvtdatastorage_couchdb_initLastCommittedBlock_duration")
	defer stopWatch()*/
	if !s.isEmpty || len(s.pendingPvtDocs) != 0 {
		return pvtdatastorage.NewErrIllegalCall("The private data store is not empty. InitLastCommittedBlock() function call is not allowed")
	}
	s.isEmpty = false
	s.lastCommittedBlock = blockNum
	logger.Debugf("InitLastCommittedBlock set to block [%d]", blockNum)
	return nil
}

func (s *store) GetPvtDataByBlockNum(blockNum uint64, filter ledger.PvtNsCollFilter) ([]*ledger.TxPvtData, error) {
	/*	stopWatch := metrics.StopWatch("pvtdatastorage_couchdb_getPvtDataByBlockNum_duration")
		defer stopWatch()*/

	logger.Debugf("Get private data for block [%d] from DB [%s], filter=%#v", blockNum, s.db.DBName, filter)
	if s.isEmpty {
		return nil, pvtdatastorage.NewErrOutOfRange("The store is empty")
	}
	lastCommittedBlock, err := s.getLastCommittedBlock()
	if err != nil {
		logger.Debugf("Error getting last committed block from DB [%s]: %s", s.db.DBName, err)
		return nil, errors.Wrap(err, "unable to get last committed block")
	}
	if blockNum > lastCommittedBlock {
		logger.Debugf("Block %d is greater than last committed block %d in DB [%s]", blockNum, lastCommittedBlock, s.db.DBName)
		return nil, pvtdatastorage.NewErrOutOfRange(fmt.Sprintf("Last committed block=%d, block requested=%d", lastCommittedBlock, blockNum))
	}
	logger.Debugf("Querying private data storage for write sets using blockNum=%d in DB [%s]", blockNum, s.db.DBName)

	results, err := s.getPvtDataByBlockNumDB(blockNum)
	if err != nil {
		if _, ok := err.(*NotFoundInIndexErr); ok {
			logger.Debugf("No private data for block %d in DB [%s]: %s", blockNum, s.db.DBName)
			return nil, nil
		}
		logger.Debugf("Error getting private data for block %d in DB [%s]: %s", blockNum, s.db.DBName, err)
		return nil, err
	}

	logger.Debugf("Got private data results for block %d in DB [%s]: %#v", blockNum, s.db.DBName, results)

	var blockPvtdata []*ledger.TxPvtData
	var currentTxNum uint64
	var currentTxWsetAssember *txPvtdataAssembler
	firstItr := true

	var sortedKeys []string
	for key := range results {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Strings(sortedKeys)

	for _, key := range sortedKeys {
		dataKeyBytes, err := hex.DecodeString(key)
		if err != nil {
			return nil, err
		}
		dataValueBytes := results[key]

		if v11Format(dataKeyBytes) {
			return v11RetrievePvtdata(results, filter)
		}
		dataKey := decodeDatakey(dataKeyBytes)
		expired, err := isExpired(dataKey.nsCollBlk, s.btlPolicy, lastCommittedBlock)
		if err != nil {
			return nil, err
		}
		if expired || !passesFilter(dataKey, filter) {
			continue
		}
		dataValue, err := decodeDataValue(dataValueBytes)
		if err != nil {
			return nil, err
		}

		if firstItr {
			currentTxNum = dataKey.txNum
			currentTxWsetAssember = newTxPvtdataAssembler(blockNum, currentTxNum)
			firstItr = false
		}

		if dataKey.txNum != currentTxNum {
			blockPvtdata = append(blockPvtdata, currentTxWsetAssember.getTxPvtdata())
			currentTxNum = dataKey.txNum
			currentTxWsetAssember = newTxPvtdataAssembler(blockNum, currentTxNum)
		}
		currentTxWsetAssember.add(dataKey.ns, dataValue)
	}
	if currentTxWsetAssember != nil {
		blockPvtdata = append(blockPvtdata, currentTxWsetAssember.getTxPvtdata())
	}

	logger.Debugf("Successfully retrieved private data for block %d in DB [%s]: %#v", blockNum, s.db.DBName, blockPvtdata)
	return blockPvtdata, nil

}

func (s *store) HasPendingBatch() (bool, error) {
	return len(s.pendingPvtDocs) != 0, nil
}

// Warning
// LastCommittedBlockHeight return non sequenced block height
// if concurrentBlockWrites bigger than 1
func (s *store) LastCommittedBlockHeight() (uint64, error) {
	if s.isEmpty {
		return 0, nil
	}
	return s.lastCommittedBlock + 1, nil
}

func (s *store) IsEmpty() (bool, error) {
	return s.isEmpty, nil
}

// Rollback implements the function in the interface `Store`
func (s *store) Rollback(blockNum uint64) error {
	if !s.checkPendingPvt(blockNum) {
		return pvtdatastorage.NewErrIllegalCall("No pending batch to rollback")
	}
	s.popPendingPvt(blockNum)
	return nil
}

func (s *store) performPurgeIfScheduled(latestCommittedBlk uint64) {
	if latestCommittedBlk%ledgerconfig.GetPvtdataStorePurgeInterval() != 0 {
		return
	}
	go func() {
		s.purgerLock.Lock()
		logger.Debugf("Purger started: Purging expired private data till block number [%d]", latestCommittedBlk)
		defer s.purgerLock.Unlock()
		err := s.purgeExpiredData(latestCommittedBlk)
		if err != nil {
			logger.Warningf("Could not purge data from pvtdata store:%s", err)
		}
		logger.Debug("Purger finished")
	}()
}

func (s *store) purgeExpiredData(maxBlkNum uint64) error {
	results, err := s.getExpiryEntriesDB(maxBlkNum)
	if _, ok := err.(*NotFoundInIndexErr); ok {
		logger.Debugf("no private data to purge [%d]", maxBlkNum)
		return nil
	}
	if err != nil {
		return err
	}

	var expiredEntries []*expiryEntry
	for k, value := range results {
		kBytes, err := hex.DecodeString(k)
		if err != nil {
			return err
		}

		expiryKey := decodeExpiryKey(kBytes)
		if err != nil {
			return err
		}
		expiryValue, err := decodeExpiryValue(value)
		if err != nil {
			return err
		}

		if expiryKey.expiringBlk <= maxBlkNum {
			expiredEntries = append(expiredEntries, &expiryEntry{key: expiryKey, value: expiryValue})
		}
	}

	err = s.purgeExpiredDataDB(maxBlkNum, expiredEntries)
	if err != nil {
		return err
	}

	logger.Infof("[%s] - [%d] Entries purged from private data storage till block number [%d]", s.ledgerid, len(results), maxBlkNum)
	return nil
}

func (s *store) Shutdown() {
	// do nothing
}

func (s *store) getLastCommittedBlock() (uint64, error) {
	return s.lastCommittedBlock, nil
}

// GetMissingPvtDataInfoForMostRecentBlocks returns the missing private data information for the
// most recent `maxBlock` blocks which miss at least a private data of a eligible collection.
func (s *store) GetMissingPvtDataInfoForMostRecentBlocks(maxBlock int) (ledger.MissingPvtDataInfo, error) {
	// we assume that this function would be called by the gossip only after processing the
	// last retrieved missing pvtdata info and committing the same.
	if maxBlock < 1 {
		return nil, nil
	}

	missingPvtDataInfo := make(ledger.MissingPvtDataInfo)
	numberOfBlockProcessed := 0
	lastProcessedBlock := uint64(0)
	isMaxBlockLimitReached := false
	// as we are not acquiring a read lock, new blocks can get committed while we
	// construct the MissingPvtDataInfo. As a result, lastCommittedBlock can get
	// changed. To ensure consistency, we atomically load the lastCommittedBlock value
	lastCommittedBlock := atomic.LoadUint64(&s.lastCommittedBlock)

	results, err := s.getRangeMissingPvtDataByMaxBlockNumDB(0, lastCommittedBlock)
	if err != nil {
		if _, ok := err.(*NotFoundInIndexErr); ok {
			logger.Debugf("No missing private data for max block %d in DB [%s]: %s", lastCommittedBlock, s.db.DBName)
			return nil, nil
		}
		logger.Debugf("Error getting missing private data for max block %d in DB [%s]: %s", lastCommittedBlock, s.db.DBName, err)
		return nil, err
	}

	for key, value := range results {
		missingDataKeyBytes, err := hex.DecodeString(key)
		if err != nil {
			return nil, err
		}
		missingDataKey := decodeMissingDataKey(missingDataKeyBytes)

		if isMaxBlockLimitReached && (missingDataKey.blkNum != lastProcessedBlock) {
			// esnures that exactly maxBlock number
			// of blocks' entries are processed
			break
		}

		// check whether the entry is expired. If so, move to the next item.
		// As we may use the old lastCommittedBlock value, there is a possibility that
		// this missing data is actually expired but we may get the stale information.
		// Though it may leads to extra work of pulling the expired data, it will not
		// affect the correctness. Further, as we try to fetch the most recent missing
		// data (less possibility of expiring now), such scenario would be rare. In the
		// best case, we can load the latest lastCommittedBlock value here atomically to
		// make this scenario very rare.
		lastCommittedBlock = atomic.LoadUint64(&s.lastCommittedBlock)
		expired, err := isExpired(missingDataKey.nsCollBlk, s.btlPolicy, lastCommittedBlock)
		if err != nil {
			return nil, err
		}
		if expired {
			continue
		}

		// check for an existing entry for the blkNum in the MissingPvtDataInfo.
		// If no such entry exists, create one. Also, keep track of the number of
		// processed block due to maxBlock limit.
		if _, ok := missingPvtDataInfo[missingDataKey.blkNum]; !ok {
			numberOfBlockProcessed++
			if numberOfBlockProcessed == maxBlock {
				isMaxBlockLimitReached = true
				// as there can be more than one entry for this block,
				// we cannot `break` here
				lastProcessedBlock = missingDataKey.blkNum
			}
		}

		valueBytes := value
		bitmap, err := decodeMissingDataValue(valueBytes)
		if err != nil {
			return nil, err
		}

		// for each transaction which misses private data, make an entry in missingBlockPvtDataInfo
		for index, isSet := bitmap.NextSet(0); isSet; index, isSet = bitmap.NextSet(index + 1) {
			txNum := uint64(index)
			missingPvtDataInfo.Add(missingDataKey.blkNum, txNum, missingDataKey.ns, missingDataKey.coll)
		}
	}

	return missingPvtDataInfo, nil
}

func (s *store) CommitPvtDataOfOldBlocks(blocksPvtData map[uint64][]*ledger.TxPvtData) error {
	if s.isLastUpdatedOldBlocksSet {
		return pvtdatastorage.NewErrIllegalCall(`The lastUpdatedOldBlocksList is set. It means that the
		stateDB may not be in sync with the pvtStore`)
	}

	err := s.commitPvtDataOfOldBlocksDB(blocksPvtData)
	if err != nil {
		return err
	}

	s.isLastUpdatedOldBlocksSet = true

	return nil
}

func (s *store) GetLastUpdatedOldBlocksPvtData() (map[uint64][]*ledger.TxPvtData, error) {
	if !s.isLastUpdatedOldBlocksSet {
		return nil, nil
	}

	updatedBlksList, err := s.getLastUpdatedOldBlocksList()
	if err != nil {
		return nil, err
	}

	blksPvtData := make(map[uint64][]*ledger.TxPvtData)
	for _, blkNum := range updatedBlksList {
		if blksPvtData[blkNum], err = s.GetPvtDataByBlockNum(blkNum, nil); err != nil {
			return nil, err
		}
	}
	return blksPvtData, nil
}

func (s *store) ResetLastUpdatedOldBlocksList() error {
	if err := s.deleteLastUpdatedOldBlocksList(); err != nil {
		return err
	}
	s.isLastUpdatedOldBlocksSet = false
	return nil
}

func (s *store) ProcessCollsEligibilityEnabled(committingBlk uint64, nsCollMap map[string][]string) error {
	//TODO
	return nil
}

func (s *store) constructUpdateEntriesFromDataEntries(dataEntries []*dataEntry) (*entriesForPvtDataOfOldBlocks, error) {
	updateEntries := &entriesForPvtDataOfOldBlocks{
		dataEntries:        make(map[dataKey]*rwset.CollectionPvtReadWriteSet),
		expiryEntries:      make(map[expiryKey]*ExpiryData),
		missingDataEntries: make(map[nsCollBlk]*bitset.BitSet)}

	// for each data entry, first, get the expiryData and missingData from the pvtStore.
	// Second, update the expiryData and missingData as per the data entry. Finally, add
	// the data entry along with the updated expiryData and missingData to the update entries
	for _, dataEntry := range dataEntries {
		// get the expiryBlk number to construct the expiryKey
		expiryKey, err := s.constructExpiryKeyFromDataEntry(dataEntry)
		if err != nil {
			return nil, err
		}

		// get the existing expiryData ntry
		var expiryData *ExpiryData
		if !neverExpires(expiryKey.expiringBlk) {
			if expiryData, err = s.getExpiryDataFromUpdateEntriesOrStore(updateEntries, expiryKey); err != nil {
				return nil, err
			}
			if expiryData == nil {
				// data entry is already expired
				// and purged (a rare scenario)
				continue
			}
		}

		// get the existing missingData entry
		var missingData *bitset.BitSet
		nsCollBlk := dataEntry.key.nsCollBlk
		if missingData, err = s.getMissingDataFromUpdateEntriesOrStore(updateEntries, nsCollBlk); err != nil {
			return nil, err
		}
		if missingData == nil {
			// data entry is already expired
			// and purged (a rare scenario)
			continue
		}

		updateEntries.addDataEntry(dataEntry)
		if expiryData != nil { // would be nill for the never expiring entry
			expiryEntry := &expiryEntry{&expiryKey, expiryData}
			updateEntries.updateAndAddExpiryEntry(expiryEntry, dataEntry.key)
		}
		updateEntries.updateAndAddMissingDataEntry(missingData, dataEntry.key)
	}
	return updateEntries, nil
}

func (s *store) constructExpiryKeyFromDataEntry(dataEntry *dataEntry) (expiryKey, error) {
	// get the expiryBlk number to construct the expiryKey
	nsCollBlk := dataEntry.key.nsCollBlk
	expiringBlk, err := s.btlPolicy.GetExpiringBlock(nsCollBlk.ns, nsCollBlk.coll, nsCollBlk.blkNum)
	if err != nil {
		return expiryKey{}, err
	}
	return expiryKey{expiringBlk, nsCollBlk.blkNum}, nil
}

func (s *store) getExpiryDataFromUpdateEntriesOrStore(updateEntries *entriesForPvtDataOfOldBlocks, expiryKey expiryKey) (*ExpiryData, error) {
	expiryData, ok := updateEntries.expiryEntries[expiryKey]
	if !ok {
		var err error
		expiryData, err = s.getExpiryDataOfExpiryKey(&expiryKey)
		if err != nil {
			return nil, err
		}
	}
	return expiryData, nil
}

func (s *store) getExpiryDataOfExpiryKey(expiryKey *expiryKey) (*ExpiryData, error) {
	var expiryEntriesMap map[string][]byte
	var err error
	if expiryEntriesMap, err = s.getExpiryEntriesDB(expiryKey.committingBlk); err != nil {
		return nil, err
	}
	v := expiryEntriesMap[hex.EncodeToString(encodeExpiryKey(expiryKey))]
	if v == nil {
		return nil, nil
	}
	return decodeExpiryValue(v)
}

func (s *store) getMissingDataFromUpdateEntriesOrStore(updateEntries *entriesForPvtDataOfOldBlocks, nsCollBlk nsCollBlk) (*bitset.BitSet, error) {
	missingData, ok := updateEntries.missingDataEntries[nsCollBlk]
	if !ok {
		var err error
		missingDataKey := &missingDataKey{nsCollBlk: nsCollBlk, isEligible: true}
		missingData, err = s.getBitmapOfMissingDataKey(missingDataKey)
		if err != nil {
			return nil, err
		}
	}
	return missingData, nil
}

func (s *store) getBitmapOfMissingDataKey(missingDataKey *missingDataKey) (*bitset.BitSet, error) {
	var missingEntriesMap map[string][]byte
	var err error
	if missingEntriesMap, err = s.getRangeMissingPvtDataByMaxBlockNumDB(missingDataKey.blkNum, missingDataKey.blkNum); err != nil {
		return nil, err
	}
	v := missingEntriesMap[hex.EncodeToString(encodeMissingDataKey(missingDataKey))]
	if v == nil {
		return nil, nil
	}
	return decodeMissingDataValue(v)
}

func (updateEntries *entriesForPvtDataOfOldBlocks) addDataEntry(dataEntry *dataEntry) {
	dataKey := dataKey{nsCollBlk: dataEntry.key.nsCollBlk, txNum: dataEntry.key.txNum}
	updateEntries.dataEntries[dataKey] = dataEntry.value
}

func (updateEntries *entriesForPvtDataOfOldBlocks) updateAndAddExpiryEntry(expiryEntry *expiryEntry, dataKey *dataKey) {
	txNum := dataKey.txNum
	nsCollBlk := dataKey.nsCollBlk
	// update
	expiryEntry.value.addPresentData(nsCollBlk.ns, nsCollBlk.coll, txNum)
	// we cannot delete entries from MissingDataMap as
	// we keep only one entry per missing <ns-col>
	// irrespective of the number of txNum.

	// add
	expiryKey := expiryKey{expiryEntry.key.expiringBlk, expiryEntry.key.committingBlk}
	updateEntries.expiryEntries[expiryKey] = expiryEntry.value
}

func (updateEntries *entriesForPvtDataOfOldBlocks) updateAndAddMissingDataEntry(missingData *bitset.BitSet, dataKey *dataKey) {

	txNum := dataKey.txNum
	nsCollBlk := dataKey.nsCollBlk
	// update
	missingData.Clear(uint(txNum))
	// add
	updateEntries.missingDataEntries[nsCollBlk] = missingData
}
