/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mempvtdatacache

import (
	"github.com/hyperledger/fabric/common/flogging"
)

var logger = flogging.MustGetLogger("peer")

// MemPvtDataCacheProvider provides pvt data cache in memory
type MemPvtDataCacheProvider struct {
	cacheLimit int
}

// NewProvider constructs a filesystem based pvt data store provider
func NewProvider(cacheLimit int) *MemPvtDataCacheProvider {
	return &MemPvtDataCacheProvider{cacheLimit}
}

// OpenPvtDataCache opens the pvt data cache for the given ledger ID
/*func (p *MemPvtDataCacheProvider) OpenStore(ledgerID string) (pvtdatastorage.Store, error) {
	s := newPvtDataCache(p.cacheLimit, ledgerID)
	return s, nil
}*/

// Close cleans up the Provider
func (p *MemPvtDataCacheProvider) Close() {
}
