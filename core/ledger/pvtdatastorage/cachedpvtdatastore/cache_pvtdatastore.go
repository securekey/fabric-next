/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cachedpvtdatastore

import (
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/pvtdatapolicy"
	"github.com/hyperledger/fabric/core/ledger/pvtdatastorage"
	"github.com/pkg/errors"
)

const (
	pvtDataStorageQueueLen = 1
)

type cachedPvtDataStore struct {
	pvtDataStore      pvtdatastorage.Store
	pvtDataCache      pvtdatastorage.Store
	pvtDataStoreCh    chan *pvtPrepareData
	writerClosedCh    chan struct{}
	commitReadyCh     chan bool
	prepareReadyCh    chan bool
	doneCh            chan struct{}
	commitImmediately bool
	firstExecuteDone  bool
}

type pvtPrepareData struct {
	blockNum uint64
	pvtData  []*ledger.TxPvtData
	missingPvtData ledger.TxMissingPvtDataMap
}

func newCachedPvtDataStore(pvtDataStore pvtdatastorage.Store, pvtDataCache pvtdatastorage.Store) (*cachedPvtDataStore, error) {
	c := cachedPvtDataStore{
		pvtDataStore:      pvtDataStore,
		pvtDataCache:      pvtDataCache,
		pvtDataStoreCh:    make(chan *pvtPrepareData, pvtDataStorageQueueLen),
		writerClosedCh:    make(chan struct{}),
		commitReadyCh:     make(chan bool),
		prepareReadyCh:    make(chan bool),
		doneCh:            make(chan struct{}),
		commitImmediately: false,
		firstExecuteDone:  false,
	}

	go c.pvtDataWriter()

	return &c, nil
}

func (c *cachedPvtDataStore) Init(btlPolicy pvtdatapolicy.BTLPolicy) {
	c.pvtDataCache.Init(btlPolicy)
	c.pvtDataStore.Init(btlPolicy)
}

// Prepare pvt data in cache and send pvt data to background prepare/commit go routine
func (c *cachedPvtDataStore) Prepare(blockNum uint64, pvtData []*ledger.TxPvtData, pvtMissingDataMap ledger.TxMissingPvtDataMap) error {
	err := c.pvtDataCache.Prepare(blockNum, pvtData, pvtMissingDataMap )
	if err != nil {
		return errors.WithMessage(err, "Prepare pvtdata in cache failed")
	}
	if blockNum == 0 {
		c.commitImmediately = true
		return c.pvtDataStore.Prepare(blockNum, pvtData, pvtMissingDataMap )
	}
	if c.firstExecuteDone {
		<-c.prepareReadyCh
	}
	c.firstExecuteDone = true
	c.pvtDataStoreCh <- &pvtPrepareData{blockNum: blockNum, pvtData: pvtData}
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
			logger.Debugf("prepare pvt data for storage [%d]", pvtPrepareData.blockNum)
			err := c.pvtDataStore.Prepare(pvtPrepareData.blockNum, pvtPrepareData.pvtData, pvtPrepareData.missingPvtData)
			if err != nil {
				logger.Errorf("pvt data was not added [%d, %s]", pvtPrepareData.blockNum, err)
				panic(panicMsg)
			}
			// we will wait until
			commitReady := <-c.commitReadyCh
			if commitReady {
				if err := c.pvtDataStore.Commit(); err != nil {
					logger.Errorf("pvt data was not committed to db [%d, %s]", pvtPrepareData.blockNum, err)
					panic(panicMsg)
				}
			} else {
				if err := c.pvtDataStore.Rollback(); err != nil {
					logger.Errorf("pvt data rollback in db failed [%d, %s]", pvtPrepareData.blockNum, err)
					panic(panicMsg)
				}
			}
			c.prepareReadyCh <- true
		}
	}
}

// Commit pvt data in cache and call background pvtDataWriter go routine to commit data
func (c *cachedPvtDataStore) Commit() error {
	err := c.pvtDataCache.Commit()
	if err != nil {
		return errors.WithMessage(err, "Commit pvtdata in cache failed")
	}
	if c.commitImmediately {
		c.commitImmediately = false
		return c.pvtDataStore.Commit()
	}
	// send signal to pvtDataWriter func to commit the pvt data
	c.commitReadyCh <- true
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
func (c *cachedPvtDataStore) Rollback() error {
	err := c.pvtDataCache.Rollback()
	if err != nil {
		return errors.WithMessage(err, "Rollback pvtdata in cache failed")
	}
	c.commitReadyCh <- false
	return nil
}

func (c *cachedPvtDataStore) Shutdown() {
	close(c.doneCh)
	<-c.writerClosedCh
	c.pvtDataCache.Shutdown()
	c.pvtDataStore.Shutdown()
}
