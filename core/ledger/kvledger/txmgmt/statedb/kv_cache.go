/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package statedb

import (
	"sync"

	"github.com/golang/groupcache/lru"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
)

type ValidatedTx struct {
	Key          string
	Value        []byte
	BlockNum     uint64
	IndexInBlock int
}

type ValidatedTxOp struct {
	Namespace string
	IsDeleted bool
	ValidatedTx
}

type KVCache struct {
	namespace        string
	capacity         int
	validatedTxCache *lru.Cache
	mutex		     sync.Mutex
	hit              uint64
	miss             uint64
}

var (
	kvCacheMap map[string]*KVCache
	kvCacheMtx sync.Mutex
)

func InitKVCache() {
	kvCacheMap = make(map[string]*KVCache)
}

func GetKVCache(namespace string) (*KVCache, error) {
	kvCacheMtx.Lock()
	defer kvCacheMtx.Unlock()

	kvCache, found := kvCacheMap[namespace]
	if !found {
		kvCache = newKVCache(namespace)
		kvCacheMap[namespace] = kvCache
	}

	return kvCache, nil
}

func newKVCache(
	namespace string) *KVCache {
	cacheSize := ledgerconfig.GetBlockCacheSize()

	validatedTxCache := lru.New(cacheSize)

	cache := KVCache{
		namespace:        namespace,
		capacity:         cacheSize,
		validatedTxCache: validatedTxCache,
	}

	return &cache
}

func (c *KVCache) get(key string) (*ValidatedTx, bool) {
	txn, ok := c.validatedTxCache.Get(key)

	if ok {
		return txn.(*ValidatedTx), ok
	}
	return nil, ok
}

func (c *KVCache) Put(validatedTx *ValidatedTx) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	exitingKeyVal, found := c.get(validatedTx.Key)
	// Add to the cache if the exisiting version is older
	if (found && exitingKeyVal.BlockNum < validatedTx.BlockNum && exitingKeyVal.IndexInBlock < validatedTx.IndexInBlock) || !found {
		c.validatedTxCache.Add(validatedTx.Key, validatedTx)
	}
}

func (c *KVCache) Get(key string) (*ValidatedTx, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	txn, ok := c.get(key)
	if !ok {
		c.miss++
		return nil, false
	}

	c.hit++
	return txn, true
}

func (c *KVCache) Size() int {
	return c.validatedTxCache.Len()
}

func (c *KVCache) Capacity() int {
	return c.capacity
}

func (c *KVCache) MustRemove(key string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.validatedTxCache.Remove(key)
}

// Remove from the cache if the blockNum and indexInBlock are bigger than the corresponding values in the cache
func (c *KVCache) Remove(key string, blockNum uint64, indexInBlock int) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	exitingKeyVal, found := c.get(key)
	// Remove from the cache if the existing version is older
	if found && exitingKeyVal.BlockNum < blockNum && exitingKeyVal.IndexInBlock < indexInBlock {
		c.validatedTxCache.Remove(key)
	}
}

func (c *KVCache) Clear() {
	c.miss = 0
	c.hit = 0
	c.validatedTxCache.Clear()
}

func (c *KVCache) Hit() uint64 {
	return c.hit
}

func (c *KVCache) Miss() uint64 {
	return c.miss
}
