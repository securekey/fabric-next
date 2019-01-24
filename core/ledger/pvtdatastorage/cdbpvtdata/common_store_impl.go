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

	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/core/ledger/pvtdatapolicy"
	"github.com/hyperledger/fabric/core/ledger/pvtdatastorage"
	"github.com/hyperledger/fabric/core/ledger/pvtdatastorage/pvtmetadata"
	"github.com/hyperledger/fabric/protos/ledger/rwset"
	"github.com/pkg/errors"
)

// TODO: This file contains code copied from the base private data store. Both of these packages should be refactored.

type commonStore struct {
	ledgerid           string
	btlPolicy          pvtdatapolicy.BTLPolicy
	isEmpty            bool
	lastCommittedBlock uint64
	purgerLock         sync.Mutex
}

type blkTranNumKey []byte

type dataEntry struct {
	key   *dataKey
	value *rwset.CollectionPvtReadWriteSet
}

type expiryEntry struct {
	key   *expiryKey
	value *pvtmetadata.ExpiryData
}

type expiryKey struct {
	expiringBlk   uint64
	committingBlk uint64
}

type dataKey struct {
	blkNum   uint64
	txNum    uint64
	ns, coll string
	purge    bool
}

func (s *store) Init(btlPolicy pvtdatapolicy.BTLPolicy) {
	s.btlPolicy = btlPolicy
}

func (s *store) Prepare(blockNum uint64, pvtData []*ledger.TxPvtData, data ledger.TxMissingPvtDataMap) error {
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

	err := s.prepareDB(blockNum, pvtData)
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

/*	stopWatch := metrics.StopWatch("pvtdatastorage_couchdb_commit_duration")
	defer stopWatch()*/

	if !s.batchPending {
		return pvtdatastorage.NewErrIllegalCall("No pending batch to commit")
	}
	committingBlockNum := s.nextBlockNum()
	logger.Debugf("Committing private data for block [%d]", committingBlockNum)

	err := s.commitDB(committingBlockNum)
	if err != nil {
		return err
	}

	s.batchPending = false
	s.isEmpty = false
	s.lastCommittedBlock = committingBlockNum
	logger.Debugf("Committed private data for block [%d]", committingBlockNum)
	s.performPurgeIfScheduled(committingBlockNum)
	return nil
}

func (s *store) InitLastCommittedBlock(blockNum uint64) error {
/*	stopWatch := metrics.StopWatch("pvtdatastorage_couchdb_initLastCommittedBlock_duration")
	defer stopWatch()*/
	if !(s.isEmpty && !s.batchPending) {
		return pvtdatastorage.NewErrIllegalCall("The private data store is not empty. InitLastCommittedBlock() function call is not allowed")
	}

	s.isEmpty = false
	s.lastCommittedBlock = blockNum

	pvtstoreLastCommittedBlock, notEmpty, err := lookupLastBlock(s.db)
	if err != nil {
		return err
	}
	//TODO add logic to support non-contiguous pvt blocks removal
	if notEmpty && pvtstoreLastCommittedBlock > blockNum {
		// delete all documents above blockNum
		for i := blockNum + 1; i <= pvtstoreLastCommittedBlock+numMetaDocs+1; i++ {
			doc, rev, e := s.db.ReadDoc(blockNumberToKey(i))
			if e != nil {
				return e
			}
			if doc != nil {
				e = s.db.DeleteDoc(blockNumberToKey(i), rev)
				if e != nil {
					return e
				}
			}
		}
	}

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
	for key, _ := range results {
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
		expired, err := isExpired(dataKey, s.btlPolicy, lastCommittedBlock)
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
	return s.GetMissingPvtDataInfoForMostRecentBlocks(maxBlock)
}

func (s *store) CommitPvtDataOfOldBlocks (blocksPvtData map[uint64][]*ledger.TxPvtData) error {
	return s.CommitPvtDataOfOldBlocks(blocksPvtData)
	}

func (s *store) GetLastUpdatedOldBlocksPvtData() (map[uint64][]*ledger.TxPvtData, error){
	return s.GetLastUpdatedOldBlocksPvtData()
}

func (s *store) ResetLastUpdatedOldBlocksList() error {
	return s.ResetLastUpdatedOldBlocksList()
}

func (s *store)  ProcessCollsEligibilityEnabled(committingBlk uint64, nsCollMap map[string][]string) error {
	return s.ProcessCollsEligibilityEnabled(committingBlk,nsCollMap)
}