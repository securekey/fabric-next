/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package kvledger

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

func (c *KVCache) Put(validatedTx *ValidatedTx) {
	c.validatedTxCache.Add(validatedTx.Key, validatedTx)
}

func (c *KVCache) Get(key string) (*ValidatedTx, bool) {
	txn, ok := c.validatedTxCache.Get(key)
	if !ok {
		return nil, false
	}

	return txn.(*ValidatedTx), true
}

func (c *KVCache) Size() int {
	return c.validatedTxCache.Len()
}

func (c *KVCache) Capacity() int {
	return c.capacity
}

func (c *KVCache) Remove(key string) {
	c.validatedTxCache.Remove(key)
}

func (c *KVCache) Clear() {
	c.validatedTxCache.Clear()
}
