/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cachedpvtdatastoreß

import (
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/pvtdatapolicy"
	"github.com/hyperledger/fabric/core/ledger/pvtdatastorage"
	"github.com/pkg/errors"
)

type cachedPvtDataStore struct {
	pvtDataStore pvtdatastorage.Store
	pvtDataCache pvtdatastorage.Store
}

func newCachedPvtDataStore(pvtDataStore pvtdatastorage.Store, pvtDataCache pvtdatastorage.Store) (*cachedPvtDataStore, error) {
	c := cachedPvtDataStore{
		pvtDataStore: pvtDataStore,
		pvtDataCache: pvtDataCache,
	}

	return &c, nil
}

func (c *cachedPvtDataStore) Init(btlPolicy pvtdatapolicy.BTLPolicy) {
	c.pvtDataCache.Init(btlPolicy)
	c.pvtDataStore.Init(btlPolicy)
}

func (c *cachedPvtDataStore) Prepare(blockNum uint64, pvtData []*ledger.TxPvtData) error {
	err := c.pvtDataCache.Prepare(blockNum, pvtData)
	if err != nil {
		return errors.Wrap(err, "Prepare pvtdata in cache failed")
	}
	return c.pvtDataStore.Prepare(blockNum, pvtData)
}

func (c *cachedPvtDataStore) Commit() error {
	err := c.pvtDataCache.Commit()
	if err != nil {
		return errors.Wrap(err, "Commit pvtdata in cache failed")
	}
	return c.pvtDataStore.Commit()
}

func (c *cachedPvtDataStore) InitLastCommittedBlock(blockNum uint64) error {
	err := c.pvtDataCache.InitLastCommittedBlock(blockNum)
	if err != nil {
		return errors.Wrap(err, "InitLastCommittedBlock pvtdata in cache failed")
	}
	return c.pvtDataStore.InitLastCommittedBlock(blockNum)
}

func (c *cachedPvtDataStore) GetPvtDataByBlockNum(blockNum uint64, filter ledger.PvtNsCollFilter) ([]*ledger.TxPvtData, error) {
	data, err := c.pvtDataCache.GetPvtDataByBlockNum(blockNum, filter)
	if err != nil {
		return nil, errors.Wrap(err, "GetPvtDataByBlockNum in cache failed")
	}
	if data != nil {
		return data, nil
	}
	return c.pvtDataStore.GetPvtDataByBlockNum(blockNum, filter)

}

func (c *cachedPvtDataStore) HasPendingBatch() (bool, error) {
	return c.pvtDataCache.HasPendingBatch()
}

func (c *cachedPvtDataStore) LastCommittedBlockHeight() (uint64, error) {
	return c.pvtDataCache.LastCommittedBlockHeight()
}

func (c *cachedPvtDataStore) IsEmpty() (bool, error) {
	return c.pvtDataCache.IsEmpty()
}

// Rollback implements the function in the interface `Store`
func (c *cachedPvtDataStore) Rollback() error {
	err := c.pvtDataCache.Rollback()
	if err != nil {
		return errors.Wrap(err, "Rollback pvtdata in cache failed")
	}
	return c.pvtDataStore.Rollback()
}

func (c *cachedPvtDataStore) Shutdown() {
	c.pvtDataCache.Shutdown()
	c.pvtDataStore.Shutdown()
}
