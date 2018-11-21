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
}

type pvtPrepareData struct {
	blockNum uint64
	pvtData  []*ledger.TxPvtData
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
	}

	go c.pvtDataWriter()

	return &c, nil
}

func (c *cachedPvtDataStore) Init(btlPolicy pvtdatapolicy.BTLPolicy) {
	c.pvtDataCache.Init(btlPolicy)
	c.pvtDataStore.Init(btlPolicy)
}

// Prepare pvt data in cache and send pvt data to background prepare/commit go routine
func (c *cachedPvtDataStore) Prepare(blockNum uint64, pvtData []*ledger.TxPvtData) error {
	err := c.pvtDataCache.Prepare(blockNum, pvtData)
	if err != nil {
		return errors.WithMessage(err, "Prepare pvtdata in cache failed")
	}
	if blockNum == 0 {
		c.commitImmediately = true
		return c.pvtDataStore.Prepare(blockNum, pvtData)
	}
	if !c.commitImmediately {
		<-c.prepareReadyCh
	}
	c.commitImmediately = false
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
			logger.Infof("prepare pvt data for storage [%d]", pvtPrepareData.blockNum)
			err := c.pvtDataStore.Prepare(pvtPrepareData.blockNum, pvtPrepareData.pvtData)
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
		return c.pvtDataStore.Commit()
	}
	// send signal to pvtDataWriter func to commit the pvt data
	c.commitReadyCh <- true
	return nil
}

func (c *cachedPvtDataStore) InitLastCommittedBlock(blockNum uint64) error {
	err := c.pvtDataCache.InitLastCommittedBlock(blockNum)
	if err != nil {
		return errors.WithMessage(err, "InitLastCommittedBlock pvtdata in cache failed")
	}
	return c.pvtDataStore.InitLastCommittedBlock(blockNum)
}

func (c *cachedPvtDataStore) GetPvtDataByBlockNum(blockNum uint64, filter ledger.PvtNsCollFilter) ([]*ledger.TxPvtData, error) {
	data, err := c.pvtDataCache.GetPvtDataByBlockNum(blockNum, filter)
	if err != nil {
		logger.Errorf("GetPvtDataByBlockNum in cache failed %s", err.Error())
		return nil, errors.WithMessage(err, "GetPvtDataByBlockNum in cache failed")
	}
	if data != nil {
		return data, nil
	}
	logger.Warningf("GetPvtDataByBlockNum didn't find pvt data in cache for blockNum %d", blockNum)
	data, err = c.pvtDataStore.GetPvtDataByBlockNum(blockNum, filter)
	if err != nil {
		return nil, err
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
	return c.pvtDataCache.IsEmpty()
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
