/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package memblkcache

import (
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
)

var logger = flogging.MustGetLogger("memblkcache")

// MemBlockCacheProvider provides block cache in memory
type MemBlockCacheProvider struct {
	cacheLimit int
}

// NewProvider constructs a filesystem based block store provider
func NewProvider(cacheLimit int) *MemBlockCacheProvider {
	return &MemBlockCacheProvider{cacheLimit}
}

// OpenBlockStore opens the block cache for the given ledger ID
func (p *MemBlockCacheProvider) OpenBlockCache(ledgerid string) (blkstorage.BlockCache, error) {
	s := newBlockCache(p.cacheLimit)
	return s, nil
}

// Exists returns whether or not the given ledger ID exists
func (p *MemBlockCacheProvider) Exists(ledgerid string) (bool, error) {
	return false, nil
}

// List returns the available ledger IDs
func (p *MemBlockCacheProvider) List() ([]string, error) {
	return nil, nil
}

// Close cleans up the Provider
func (p *MemBlockCacheProvider) Close() {
}
