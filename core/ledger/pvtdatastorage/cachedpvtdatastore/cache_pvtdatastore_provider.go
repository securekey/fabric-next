/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cachedpvtdatastore

import (
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/ledger/pvtdatastorage"
)

var logger = flogging.MustGetLogger("pvtdatacache")

type CachedPvtDataProvider struct {
	storageProvider pvtdatastorage.Provider
	cacheProvider   pvtdatastorage.Provider
}

// NewProvider creates a new PvtDataStoreProvider that combines a cache provider and a backing storage provider
func NewProvider(storageProvider pvtdatastorage.Provider, cacheProvider pvtdatastorage.Provider) *CachedPvtDataProvider {
	p := CachedPvtDataProvider{
		storageProvider: storageProvider,
		cacheProvider:   cacheProvider,
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

	s, err := newCachedPvtDataStore(pvtDataStore, pvtDataCache)
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
