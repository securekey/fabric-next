/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cachedblkstore

import (
	"context"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("blkcache")

type CachedBlockstoreProvider struct {
	storageProvider blkstorage.BlockStoreProvider
	indexProvider   blkstorage.BlockIndexProvider
	cacheProvider   blkstorage.BlockCacheProvider
}

// TODO: merge into BlockStore interface
type blockStoreWithCheckpoint interface {
	blkstorage.BlockStore
	WaitForBlock(ctx context.Context, blockNum uint64) uint64
	BlockCommitted() (uint64, chan struct{})
	LastBlockNumber() uint64
}

// NewProvider creates a new BlockStoreProvider that combines a cache (+ index) provider and a backing storage provider
func NewProvider(storageProvider blkstorage.BlockStoreProvider, indexProvider blkstorage.BlockIndexProvider, cacheProvider blkstorage.BlockCacheProvider) *CachedBlockstoreProvider {
	p := CachedBlockstoreProvider{
		storageProvider: storageProvider,
		cacheProvider:   cacheProvider,
		indexProvider:   indexProvider,
	}

	return &p
}

// CreateBlockStore creates a block store instance for the given ledger ID
func (p *CachedBlockstoreProvider) CreateBlockStore(ledgerid string) (blkstorage.BlockStore, error) {
	return p.OpenBlockStore(ledgerid)
}

// CreateBlockStore creates a block store instance for the given ledger ID
func (p *CachedBlockstoreProvider) OpenBlockStore(ledgerid string) (blkstorage.BlockStore, error) {
	blockStore, err := p.storageProvider.OpenBlockStore(ledgerid)
	if err != nil {
		return nil, err
	}

	blockStoreWithCheckpoint, ok := blockStore.(blockStoreWithCheckpoint)
	if !ok {
		return nil, errors.New("invalid block store interface")
	}

	blockIndex, err := p.indexProvider.OpenBlockIndex(ledgerid)
	if err != nil {
		return nil, err
	}

	blockCache, err := p.cacheProvider.OpenBlockCache(ledgerid)
	if err != nil {
		return nil, err
	}

	s, err := newCachedBlockStore(blockStoreWithCheckpoint, blockIndex, blockCache)
	if err != nil {
		return nil, err
	}

	return s, nil
}

// Exists returns whether or not the given ledger ID exists
func (p *CachedBlockstoreProvider) Exists(ledgerid string) (bool, error) {
	// TODO: handle cache recovery
	return p.storageProvider.Exists(ledgerid)
}

// List returns the available ledger IDs
func (p *CachedBlockstoreProvider) List() ([]string, error) {
	// TODO: handle cache recovery
	return p.storageProvider.List()
}

// Close cleans up the Provider
func (p *CachedBlockstoreProvider) Close() {
	p.cacheProvider.Close()
	p.indexProvider.Close()
	p.storageProvider.Close()
}
