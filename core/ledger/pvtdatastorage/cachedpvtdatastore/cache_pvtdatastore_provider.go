/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cachedpvtdatastore

import (
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/core/ledger/pvtdatastorage"
	"github.com/hyperledger/fabric/core/ledger/pvtdatastorage/cdbpvtdata"
)

var logger = flogging.MustGetLogger("pvtdatacache")

// CachedPvtDataProvider encapsulates the storage and cache providers in addition to the missing data index provider
type CachedPvtDataProvider struct {
	storageProvider          pvtdatastorage.Provider
	cacheProvider            pvtdatastorage.Provider
	missingDataIndexProvider *leveldbhelper.Provider
}

// NewProvider creates a new PvtDataStoreProvider that combines a cache provider and a backing storage provider
func NewProvider(storageProvider pvtdatastorage.Provider, cacheProvider pvtdatastorage.Provider) *CachedPvtDataProvider {
	var m *leveldbhelper.Provider
	if cdbs, ok := storageProvider.(*cdbpvtdata.Provider); ok {
		m = cdbs.GetMissingDataIndexProvider()
	}
	p := CachedPvtDataProvider{
		storageProvider:          storageProvider,
		cacheProvider:            cacheProvider,
		missingDataIndexProvider: m,
	}

	return &p
}

// OpenStore creates a pvt data store instance for the given ledger ID
func (c *CachedPvtDataProvider) OpenStore(ledgerID string) (pvtdatastorage.Store, error) {
	pvtDataStore, err := c.storageProvider.OpenStore(ledgerID)
	if err != nil {
		return nil, err
	}
	pvtDataCache, err := c.cacheProvider.OpenStore(ledgerID)
	if err != nil {
		return nil, err
	}

	var dbHandle *leveldbhelper.DBHandle
	if c.missingDataIndexProvider != nil {
		dbHandle = c.missingDataIndexProvider.GetDBHandle(ledgerID)
	}

	s, err := newCachedPvtDataStore(pvtDataStore, pvtDataCache, dbHandle)
	if err != nil {
		return nil, err
	}

	return s.pvtDataStore, nil
}

// Close cleans up the Provider
func (c *CachedPvtDataProvider) Close() {
	c.cacheProvider.Close()
	c.storageProvider.Close()
}
