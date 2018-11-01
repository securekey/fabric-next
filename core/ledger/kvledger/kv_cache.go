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

type KVCache struct {
	ValidatedTxCache *lru.Cache
	mutex            sync.RWMutex
}

func NewKVCache(
	ledgerID string) *KVCache {
	cacheSize := ledgerconfig.GetBlockCacheSize()

	validatedTxCache := lru.New(cacheSize)
	mtx := sync.RWMutex{}

	cache := KVCache{
		ValidatedTxCache: validatedTxCache,
		mutex:            mtx,
	}

	return &cache
}

func (c *KVCache) Put(validatedTx ValidatedTx) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.ValidatedTxCache.Add(validatedTx.Key, validatedTx)
}

func (c *KVCache) Get(key string) (*ValidatedTx, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	txn, ok := c.ValidatedTxCache.Get(key)
	if !ok {
		return nil, false
	}

	return txn.(*ValidatedTx), true
}
