/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cachedpvtdatastore

import (
	"context"

	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/core/ledger/pvtdatapolicy"
	"github.com/hyperledger/fabric/core/ledger/pvtdatastorage"
	"github.com/pkg/errors"
)

type cachedPvtDataStore struct {
	pvtDataStore       pvtdatastorage.Store
	pvtDataCache       pvtdatastorage.Store
	pvtDataStoreCh     chan *pvtPrepareData
	pvtDataCommitCh    chan *pvtPrepareData
	pvtDataRollbackCh  chan *pvtPrepareData
	writerClosedCh     chan struct{}
	doneCh             chan struct{}
	pvtReadyCh         chan bool
	sigCh              chan struct{}
	missingDataIndexDB *leveldbhelper.DBHandle
}

type pvtPrepareData struct {
	blockNum       uint64
	pvtData        []*ledger.TxPvtData
	missingPvtData ledger.TxMissingPvtDataMap
}

func newCachedPvtDataStore(pvtDataStore pvtdatastorage.Store, pvtDataCache pvtdatastorage.Store, missingDataIndexDBHandle *leveldbhelper.DBHandle) (*cachedPvtDataStore, error) {
	c := cachedPvtDataStore{
		pvtDataStore:       pvtDataStore,
		pvtDataCache:       pvtDataCache,
		pvtDataStoreCh:     make(chan *pvtPrepareData),
		pvtDataCommitCh:    make(chan *pvtPrepareData),
		pvtDataRollbackCh:  make(chan *pvtPrepareData),
		writerClosedCh:     make(chan struct{}),
		doneCh:             make(chan struct{}),
		pvtReadyCh:         make(chan bool),
		sigCh:              make(chan struct{}),
		missingDataIndexDB: missingDataIndexDBHandle,
	}

	concurrentBlockWrites := ledgerconfig.GetConcurrentBlockWrites()
	for x := 0; x < concurrentBlockWrites; x++ {
		go c.pvtDataWriter()
	}

	return &c, nil
}

func (c *cachedPvtDataStore) Init(btlPolicy pvtdatapolicy.BTLPolicy) {
	c.pvtDataCache.Init(btlPolicy)
	c.pvtDataStore.Init(btlPolicy)
}

// Prepare pvt data in cache and send pvt data to background prepare/commit go routine
func (c *cachedPvtDataStore) Prepare(blockNum uint64, pvtData []*ledger.TxPvtData, pvtMissingDataMap ledger.TxMissingPvtDataMap) error {
	err := c.pvtDataCache.Prepare(blockNum, pvtData, pvtMissingDataMap)
	if err != nil {
		return errors.WithMessage(err, "Prepare pvtdata in cache failed")
	}

	if blockNum > uint64(ledgerconfig.GetConcurrentBlockWrites()) {
		waitForPvt := blockNum - uint64(ledgerconfig.GetConcurrentBlockWrites())
		// Wait for underlying storage to complete commit on previous block.
		logger.Debugf("waiting for previous block to checkpoint [%d]", waitForPvt)
		//stopWatchWaitBlock := metrics.StopWatch("cached_pvt_store_prepare_wait_block_duration")
		c.waitForPvt(context.Background(), waitForPvt)
		//stopWatchWaitBlock()
		logger.Debugf("ready to store incoming block [%d]", blockNum)
	}

	//stopWatchWaitQueue := metrics.StopWatch("cached_pvt_store_prepare_wait_queue_duration")
	c.pvtDataStoreCh <- &pvtPrepareData{blockNum: blockNum, pvtData: pvtData}
	//stopWatchWaitQueue()
	return nil
}

// pvtDataWriter go routine to prepare and commit pvt in db
func (c *cachedPvtDataStore) pvtDataWriter() {
	const panicMsg = "pvt data processing failure"

	for {
		select {
		case <-c.doneCh:
			close(c.writerClosedCh)
			return
		case pvtPrepareData := <-c.pvtDataStoreCh:
			logger.Debugf("prepare pvt data for storage [%d], length of pvtData:%d", pvtPrepareData.blockNum, len(pvtPrepareData.pvtData))
			err := c.pvtDataStore.Prepare(pvtPrepareData.blockNum, pvtPrepareData.pvtData, nil)
			if err != nil {
				logger.Errorf("pvt data was not added [%d, %s]", pvtPrepareData.blockNum, err)
				panic(panicMsg)
			}
			c.pvtReadyCh <- true
		case pvtPrepareData := <-c.pvtDataCommitCh:
			if err := c.pvtDataStore.Commit(pvtPrepareData.blockNum); err != nil {
				logger.Errorf("pvt data was not committed to db [%d, %s]", pvtPrepareData.blockNum, err)
				panic(panicMsg)
			}
			close(c.sigCh)
			c.sigCh = make(chan struct{})
		case pvtPrepareData := <-c.pvtDataRollbackCh:
			if err := c.pvtDataStore.Rollback(pvtPrepareData.blockNum); err != nil {
				logger.Errorf("pvt data rollback in db failed [%d, %s]", pvtPrepareData.blockNum, err)
				panic(panicMsg)
			}
		}
	}
}

// Commit pvt data in cache and call background pvtDataWriter go routine to commit data
func (c *cachedPvtDataStore) Commit(blockNum uint64) error {
	err := c.pvtDataCache.Commit(blockNum)
	if err != nil {
		return errors.WithMessage(err, "Commit pvtdata in cache failed")
	}
	<-c.pvtReadyCh
	c.pvtDataCommitCh <- &pvtPrepareData{blockNum: blockNum}
	return nil
}

func (c *cachedPvtDataStore) InitLastCommittedBlock(blockNum uint64) error {
	logger.Debugf("InitLastCommittedBlock blockNum %d", blockNum)
	isEmpty, err := c.pvtDataCache.IsEmpty()
	if err != nil {
		return err
	}
	if isEmpty {
		logger.Debugf("InitLastCommittedBlock in cache blockNum %d", blockNum)
		err := c.pvtDataCache.InitLastCommittedBlock(blockNum)
		if err != nil {
			return errors.WithMessage(err, "InitLastCommittedBlock pvtdata in cache failed")
		}
	}
	isEmpty, err = c.pvtDataStore.IsEmpty()
	if err != nil {
		return err
	}
	if isEmpty {
		logger.Debugf("InitLastCommittedBlock in pvtDataStore blockNum %d", blockNum)
		return c.pvtDataStore.InitLastCommittedBlock(blockNum)
	}
	return nil
}

func (c *cachedPvtDataStore) GetPvtDataByBlockNum(blockNum uint64, filter ledger.PvtNsCollFilter) ([]*ledger.TxPvtData, error) {
	data, err := c.pvtDataCache.GetPvtDataByBlockNum(blockNum, filter)
	if err != nil {
		logger.Errorf("GetPvtDataByBlockNum in cache failed %s", err.Error())
		return nil, errors.WithMessage(err, "GetPvtDataByBlockNum in cache failed")
	}
	if data != nil {
		//metrics.IncrementCounter("cachepvtdatastore_getpvtdatabyblocknum_request_hit")
		return data, nil
	}
	logger.Warningf("GetPvtDataByBlockNum didn't find pvt data in cache for blockNum %d", blockNum)
	data, err = c.pvtDataStore.GetPvtDataByBlockNum(blockNum, filter)
	if err != nil {
		return nil, err
	}
	if len(data) > 0 {
		//metrics.IncrementCounter("cachepvtdatastore_getpvtdatabyblocknum_request_miss")
	}

	return data, nil
}

func (c *cachedPvtDataStore) HasPendingBatch() (bool, error) {
	return c.pvtDataStore.HasPendingBatch()
}

func (c *cachedPvtDataStore) LastCommittedBlockHeight() (uint64, error) {
	return c.pvtDataCache.LastCommittedBlockHeight()
}

func (c *cachedPvtDataStore) IsEmpty() (bool, error) {
	pvtDataCacheIsEmpty, err := c.pvtDataCache.IsEmpty()
	if err != nil {
		return false, err
	}
	pvtDataStoreIsEmpty, err := c.pvtDataStore.IsEmpty()
	if err != nil {
		return false, err
	}
	return pvtDataCacheIsEmpty || pvtDataStoreIsEmpty, nil
}

// Rollback pvt data in cache and call background pvtDataWriter go routine to rollback data
func (c *cachedPvtDataStore) Rollback(blockNum uint64) error {
	err := c.pvtDataCache.Rollback(blockNum)
	if err != nil {
		return errors.WithMessage(err, "Rollback pvtdata in cache failed")
	}
	<-c.pvtReadyCh
	c.pvtDataRollbackCh <- &pvtPrepareData{blockNum: blockNum}
	return nil
}

func (c *cachedPvtDataStore) Shutdown() {
	close(c.doneCh)
	<-c.writerClosedCh
	c.pvtDataCache.Shutdown()
	c.pvtDataStore.Shutdown()
}

func (c *cachedPvtDataStore) GetMissingPvtDataInfoForMostRecentBlocks(maxBlock int) (ledger.MissingPvtDataInfo, error) {
	return c.pvtDataStore.GetMissingPvtDataInfoForMostRecentBlocks(maxBlock)
}

func (c *cachedPvtDataStore) ProcessCollsEligibilityEnabled(committingBlk uint64, nsCollMap map[string][]string) error {
	c.pvtDataStore.ProcessCollsEligibilityEnabled(committingBlk, nsCollMap)
}

func (c *cachedPvtDataStore) CommitPvtDataOfOldBlocks(blocksPvtData map[uint64][]*ledger.TxPvtData) error {

	err := c.pvtDataCache.CommitPvtDataOfOldBlocks(blocksPvtData)
	if err != nil {
		return errors.WithMessage(err, "CommitPvtDataOfOldBlocks in cache failed")
	}
	err = c.pvtDataStore.CommitPvtDataOfOldBlocks(blocksPvtData)
	if err != nil {
		return errors.WithMessage(err, "CommitPvtDataOfOldBlocks in store failed")
	}
	return nil
}

func (c *cachedPvtDataStore) GetLastUpdatedOldBlocksPvtData() (map[uint64][]*ledger.TxPvtData, error) {
	return c.pvtDataStore.GetLastUpdatedOldBlocksPvtData()
}

func (c *cachedPvtDataStore) ResetLastUpdatedOldBlocksList() error {
	return c.pvtDataStore.ResetLastUpdatedOldBlocksList()
}

func (c *cachedPvtDataStore) waitForPvt(ctx context.Context, blockNum uint64) {
	var lastBlockNumber uint64
PvtLoop:
	for {
		lastBlockNumber, _ = c.pvtDataStore.LastCommittedBlockHeight()
		// LastCommittedBlockHeight return LastCommittedBlockHeight+1
		lastBlockNumber = lastBlockNumber - 1

		if lastBlockNumber >= blockNum {
			break
		}

		logger.Debugf("waiting for newer pvt blocks [%d, %d]", lastBlockNumber, blockNum)
		select {
		case <-ctx.Done():
			break PvtLoop
		case <-c.sigCh:
		}
	}

	logger.Debugf("finished waiting for pvt blocks [%d, %d]", lastBlockNumber, blockNum)
}
