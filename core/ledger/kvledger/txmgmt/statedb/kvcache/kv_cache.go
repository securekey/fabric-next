/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package kvcache

import (
	"container/list"
	"sync"

	"github.com/golang/groupcache/lru"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/core/ledger/util"
)

// VersionedValue encloses value and corresponding version
type VersionedValue struct {
	Value   []byte
	Version *version.Height
}

var logger = flogging.MustGetLogger("statedb")
var defVal struct{}

const (
	nsJoiner          = "$$"
	pvtDataPrefix     = "p"
	pvtHashDataPrefix = "h"
)

type ValidatedTx struct {
	Key          string
	Value        []byte
	BlockNum     uint64
	IndexInBlock int
}

type ValidatedTxOp struct {
	Namespace string
	ChId      string
	IsDeleted bool
	ValidatedTx
}

type ValidatedPvtData struct {
	ValidatedTxOp
	Collection          string
	Level1ExpiringBlock uint64
	Level2ExpiringBlock uint64
	PolicyBTL           uint64
}

type KVCache struct {
	cacheName            string
	capacity             int
	validatedTxCache     *lru.Cache
	nonDurablePvtCache   map[string]*ValidatedPvtData
	expiringPvtKeys      map[uint64]*list.List
	pinnedTx             map[string]*ValidatedTx
	keys                 map[string]struct{}
	nonDurableSortedKeys []string
	mutex                sync.Mutex
	hit                  uint64
	miss                 uint64
}

func DerivePvtHashDataNs(namespace, collection string) string {
	return namespace + nsJoiner + pvtHashDataPrefix + collection
}

func DerivePvtDataNs(namespace, collection string) string {
	return namespace + nsJoiner + pvtDataPrefix + collection
}

func newKVCache(
	cacheName string) *KVCache {
	cacheSize := ledgerconfig.GetKVCacheSize()

	validatedTxCache := lru.New(cacheSize)

	nonDurablePvtCache := make(map[string]*ValidatedPvtData)
	expiringPvtKeys := make(map[uint64]*list.List)
	pinnedTx := make(map[string]*ValidatedTx)
	keys := make(map[string]struct{})
	nonDurableSortedKeys := make([]string, 0)

	cache := KVCache{
		cacheName:            cacheName,
		capacity:             cacheSize,
		validatedTxCache:     validatedTxCache,
		nonDurablePvtCache:   nonDurablePvtCache,
		expiringPvtKeys:      expiringPvtKeys,
		pinnedTx:             pinnedTx,
		keys:                 keys,
		nonDurableSortedKeys: nonDurableSortedKeys,
	}

	cache.validatedTxCache.OnEvicted = cleanUpKeys(&cache)

	return &cache
}

//cleanUpKeys removes entry in cache.keys when corresponding cache entry gets purged
func cleanUpKeys(cache *KVCache) func(key lru.Key, value interface{}) {
	return func(key lru.Key, value interface{}) {
		keyStr, _ := key.(string)
		delete(cache.keys, keyStr)
	}
}

func (c *KVCache) get(key string) (*ValidatedTx, bool) {

	pvt, ok := c.nonDurablePvtCache[key]
	if ok {
		return &pvt.ValidatedTxOp.ValidatedTx, ok
	}

	txn, ok := c.validatedTxCache.Get(key)
	if ok {
		return txn.(*ValidatedTx), ok
	}

	txn, ok = c.pinnedTx[key]
	if ok {
		return txn.(*ValidatedTx), ok
	}

	return nil, false
}

func (c *KVCache) getNonDurable(key string) (*ValidatedPvtData, bool) {

	pvt, ok := c.nonDurablePvtCache[key]
	if ok {
		return pvt, ok
	}

	return nil, false
}

func (c *KVCache) Put(validatedTx *ValidatedTx, pin bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	exitingKeyVal, found := c.get(validatedTx.Key)
	// Add to the cache if the existing version is older
	if (found && exitingKeyVal.BlockNum < validatedTx.BlockNum) ||
		(found && exitingKeyVal.BlockNum == validatedTx.BlockNum && exitingKeyVal.IndexInBlock < validatedTx.IndexInBlock) || !found {
		c.validatedTxCache.Add(validatedTx.Key, validatedTx)
		if pin {
			c.pinnedTx[validatedTx.Key] = validatedTx
		}
		c.keys[validatedTx.Key] = defVal
	}
}

// PutPrivate will add the validateTx to the 'permanent' lru cache (if level2 Block height > level1 Block)
// or to the 'non-durable' cache otherwise
func (c *KVCache) PutPrivate(validatedTx *ValidatedPvtData, pin bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Data goes to 'permanent' lru cache condition
	if validatedTx.Level2ExpiringBlock > validatedTx.Level1ExpiringBlock {
		exitingKeyVal, found := c.get(validatedTx.Key)
		// Add to the cache if the existing version is older
		if (found && exitingKeyVal.BlockNum < validatedTx.BlockNum) ||
			(found && exitingKeyVal.BlockNum == validatedTx.BlockNum && exitingKeyVal.IndexInBlock < validatedTx.IndexInBlock) || !found {
			logger.Debugf("Adding key[%s] to durable private data; level1[%d] level2[%d]", validatedTx.Key, validatedTx.Level1ExpiringBlock, validatedTx.Level2ExpiringBlock)
			newTx := validatedTx.ValidatedTxOp.ValidatedTx
			c.validatedTxCache.Add(validatedTx.Key, &newTx)
			if pin {
				c.pinnedTx[validatedTx.Key] = &validatedTx.ValidatedTx
			}
			c.keys[validatedTx.Key] = defVal
		}
		return
	}

	// Otherwise, data goes to 'non-durable' cache
	c.addNonDurable(validatedTx)
}

// PutPrivateNonDurable will add validatedTx data to the 'non-durable' cache only
func (c *KVCache) PutPrivateNonDurable(validatedTx *ValidatedPvtData) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.addNonDurable(validatedTx)
}

func (c *KVCache) addNonDurable(validatedTx *ValidatedPvtData) {
	if validatedTx.Level2ExpiringBlock <= validatedTx.Level1ExpiringBlock {
		// data goes to 'non-durable' cache
		exitingKeyVal, found := c.getNonDurable(validatedTx.Key)
		// Add to the cache if the existing version is older
		if (found && exitingKeyVal.BlockNum < validatedTx.BlockNum) ||
			(found && exitingKeyVal.BlockNum == validatedTx.BlockNum && exitingKeyVal.IndexInBlock < validatedTx.IndexInBlock) || !found {
			logger.Debugf("Adding key[%s] to expiring private data; level1[%d] level2[%d]", validatedTx.Key, validatedTx.Level1ExpiringBlock, validatedTx.Level2ExpiringBlock)
			c.nonDurablePvtCache[validatedTx.Key] = validatedTx
			c.keys[validatedTx.Key] = defVal
			c.addKeyToExpiryMap(validatedTx.Level1ExpiringBlock, validatedTx.Key)
			if len(c.nonDurablePvtCache) > ledgerconfig.GetKVCacheNonDurableSize() {
				logger.Debugf("Expiring cache size[%d] is over limit[%d] for cache[%s]", len(c.nonDurablePvtCache), ledgerconfig.GetKVCacheNonDurableSize(), c.cacheName)
			}
			return
		}
	}
	logger.Debugf("nothing is added into non-durable private data cache for key[%s] - level1[%d] level2[%d]", validatedTx.Key, validatedTx.Level1ExpiringBlock, validatedTx.Level2ExpiringBlock)
}

func (c *KVCache) purgePrivate(blockNumber uint64) {
	l, ok := c.expiringPvtKeys[blockNumber]
	if !ok {
		// nothing to do
		return
	}

	var deleted int
	e := l.Front()
	for {
		if e == nil {
			break
		}
		key := e.Value.(string)
		pvtData, ok := c.getNonDurable(key)
		if ok && pvtData.Level1ExpiringBlock <= blockNumber {
			delete(c.nonDurablePvtCache, key)
			delete(c.keys, key)
			deleted++
		}
		e = e.Next()
	}

	logger.Infof("Deleted %d keys from level1, processed %d keys for block %d in collection %s", deleted, l.Len(), blockNumber, c.cacheName)

	delete(c.expiringPvtKeys, blockNumber)

}

func (c *KVCache) addKeyToExpiryMap(expiryBlock uint64, key string) {
	l, ok := c.expiringPvtKeys[expiryBlock]
	if !ok {
		l = list.New()
	}

	l.PushFront(key)
	c.expiringPvtKeys[expiryBlock] = l
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
	delete(c.nonDurablePvtCache, key)
	delete(c.pinnedTx, key)
	delete(c.keys, key)
}

// Remove from the cache if the blockNum and indexInBlock are bigger than the corresponding values in the cache
func (c *KVCache) Remove(key string, blockNum uint64, indexInBlock int) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	exitingKeyVal, found := c.get(key)
	// Remove from all the caches if the existing version is older
	if (found && exitingKeyVal.BlockNum < blockNum) ||
		(found && exitingKeyVal.BlockNum == blockNum && exitingKeyVal.IndexInBlock < indexInBlock) {
		c.validatedTxCache.Remove(key)
		delete(c.nonDurablePvtCache, key)
		delete(c.pinnedTx, key)
		delete(c.keys, key)
	}
}

func (c *KVCache) Clear() {
	c.miss = 0
	c.hit = 0
	c.validatedTxCache.Clear()
	c.nonDurablePvtCache = nil
	c.expiringPvtKeys = nil
	c.keys = make(map[string]struct{})
	c.nonDurableSortedKeys = make([]string, 0)
}

func (c *KVCache) sortNonDurableKeys() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.nonDurableSortedKeys = util.GetSortedKeys(c.nonDurablePvtCache)
}

func (c *KVCache) getNonDurableSortedKeys() []string {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.nonDurableSortedKeys
}

func (c *KVCache) Hit() uint64 {
	return c.hit
}

func (c *KVCache) Miss() uint64 {
	return c.miss
}
