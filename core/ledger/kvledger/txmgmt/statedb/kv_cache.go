/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package statedb

import (
	"container/list"
	"sync"

	"github.com/hyperledger/fabric/core/ledger/util"

	"github.com/golang/groupcache/lru"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb/statekeyindex"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
)

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
}

type KVCache struct {
	cacheName          string
	capacity           int
	validatedTxCache   *lru.Cache
	nonDurablePvtCache map[string]*ValidatedPvtData
	expiringPvtKeys    map[uint64]*list.List
	pinnedTx           map[string]*ValidatedTx
	keys               map[string]struct{}
	mutex              sync.Mutex
	hit                uint64
	miss               uint64
}

var (
	kvCacheMap map[string]*KVCache
	kvCacheMtx sync.RWMutex
)

func InitKVCache() {
	kvCacheMap = make(map[string]*KVCache)
}

func getKVCache(chId string, namespace string) (*KVCache, error) {
	cacheName := chId
	if len(namespace) > 0 {
		cacheName = cacheName + "_" + namespace
	}

	kvCache, found := kvCacheMap[cacheName]
	if !found {
		kvCache = newKVCache(cacheName)
		kvCacheMap[cacheName] = kvCache
	}

	return kvCache, nil
}

func GetKVCache(chId string, namespace string) (*KVCache, error) {
	kvCacheMtx.RLock()
	defer kvCacheMtx.RUnlock()

	return getKVCache(chId, namespace)
}

func DerivePvtHashDataNs(namespace, collection string) string {
	return namespace + nsJoiner + pvtHashDataPrefix + collection
}

func DerivePvtDataNs(namespace, collection string) string {
	return namespace + nsJoiner + pvtDataPrefix + collection
}

func PrepareIndexUpdates(validatedTxOps []ValidatedTxOp, validatedPvtData []ValidatedPvtData, validatedPvtHashData []ValidatedPvtData) ([]*statekeyindex.IndexUpdate, []statekeyindex.CompositeKey) {

	var indexUpdates []*statekeyindex.IndexUpdate
	var indexDeletes []statekeyindex.CompositeKey

	for _, v := range validatedTxOps {
		if v.IsDeleted {
			indexDeletes = append(indexDeletes, statekeyindex.CompositeKey{Key: v.Key, Namespace: v.Namespace})
		} else {
			indexUpdate := statekeyindex.IndexUpdate{
				Key:   statekeyindex.CompositeKey{Key: v.Key, Namespace: v.Namespace},
				Value: statekeyindex.Metadata{BlockNumber: v.BlockNum, TxNumber: uint64(v.IndexInBlock)},
			}
			indexUpdates = append(indexUpdates, &indexUpdate)
		}
	}

	for _, v := range validatedPvtData {
		namespace := DerivePvtDataNs(v.Namespace, v.Collection)
		if v.IsDeleted {
			indexDeletes = append(indexDeletes, statekeyindex.CompositeKey{Key: v.Key, Namespace: namespace})
		} else {
			indexUpdate := statekeyindex.IndexUpdate{
				Key:   statekeyindex.CompositeKey{Key: v.Key, Namespace: namespace},
				Value: statekeyindex.Metadata{BlockNumber: v.BlockNum, TxNumber: uint64(v.IndexInBlock)},
			}
			indexUpdates = append(indexUpdates, &indexUpdate)
		}
	}

	for _, v := range validatedPvtHashData {
		namespace := DerivePvtHashDataNs(v.Namespace, v.Collection)
		if v.IsDeleted {
			indexDeletes = append(indexDeletes, statekeyindex.CompositeKey{Key: v.Key, Namespace: namespace})
		} else {
			indexUpdate := statekeyindex.IndexUpdate{
				Key:   statekeyindex.CompositeKey{Key: v.Key, Namespace: namespace},
				Value: statekeyindex.Metadata{BlockNumber: v.BlockNum, TxNumber: uint64(v.IndexInBlock)},
			}
			indexUpdates = append(indexUpdates, &indexUpdate)
		}
	}

	return indexUpdates, indexDeletes
}

func ApplyIndexUpdates(indexUpdates []*statekeyindex.IndexUpdate, indexDeletes []statekeyindex.CompositeKey, ledgerID string) error {

	//Add key index in leveldb
	if len(indexUpdates) > 0 {
		stateKeyIndex, err := statekeyindex.NewProvider().OpenStateKeyIndex(ledgerID)
		if err != nil {
			return err
		}
		err = stateKeyIndex.AddIndex(indexUpdates)
		if err != nil {
			return err
		}
	}

	// Delete key index in leveldb
	if len(indexDeletes) > 0 {
		stateKeyIndex, err := statekeyindex.NewProvider().OpenStateKeyIndex(ledgerID)
		if err != nil {
			return err
		}
		err = stateKeyIndex.DeleteIndex(indexDeletes)
		if err != nil {
			return err
		}
	}

	return nil
}

// UpdateKVCache will purge non durable data from the cache for the given blockNumber and update all caches with the
// provided validatedTxOps, validatedPvtData and validatedPvtHashData and update leveldb indexes for the given ledgerID
func UpdateKVCache(blockNumber uint64, validatedTxOps []ValidatedTxOp, validatedPvtData []ValidatedPvtData, validatedPvtHashData []ValidatedPvtData, pin bool) {
	kvCacheMtx.Lock()
	defer kvCacheMtx.Unlock()

	purgeNonDurable(blockNumber)

	for _, v := range validatedTxOps {
		kvCache, _ := getKVCache(v.ChId, v.Namespace)
		if v.IsDeleted {
			kvCache.Remove(v.Key, v.BlockNum, v.IndexInBlock)
		} else {
			newTx := v.ValidatedTx
			kvCache.Put(&newTx, pin)
		}
	}

	for _, v := range validatedPvtData {
		namespace := DerivePvtDataNs(v.Namespace, v.Collection)
		kvCache, _ := getKVCache(v.ChId, namespace)
		if v.IsDeleted {
			kvCache.Remove(v.Key, v.BlockNum, v.IndexInBlock)
		} else {
			newTx := v
			kvCache.PutPrivate(&newTx, pin)
		}
	}

	for _, v := range validatedPvtHashData {
		namespace := DerivePvtHashDataNs(v.Namespace, v.Collection)
		kvCache, _ := getKVCache(v.ChId, namespace)
		if v.IsDeleted {
			kvCache.Remove(v.Key, v.BlockNum, v.IndexInBlock)
		} else {
			newTx := v
			kvCache.PutPrivate(&newTx, pin)
		}
	}

}

// UpdateNonDurableKVCache will purge non durable data from the cache for the given blockNumber then update it with non durable
// private data only (validatedPvtData and validatedPvtHashData)
func UpdateNonDurableKVCache(blockNumber uint64, validatedPvtData []ValidatedPvtData, validatedPvtHashData []ValidatedPvtData) {
	kvCacheMtx.Lock()
	defer kvCacheMtx.Unlock()

	purgeNonDurable(blockNumber)

	for _, v := range validatedPvtData {
		namespace := DerivePvtDataNs(v.Namespace, v.Collection)
		kvCache, _ := getKVCache(v.ChId, namespace)

		newTx := v
		kvCache.PutPrivateNonDurable(&newTx)

	}

	for _, v := range validatedPvtHashData {
		namespace := DerivePvtHashDataNs(v.Namespace, v.Collection)
		kvCache, _ := getKVCache(v.ChId, namespace)
		newTx := v
		kvCache.PutPrivateNonDurable(&newTx)
	}
}

func GetLeveLDBIterator(namespace, startKey, endKey, ledgerID string) (*leveldbhelper.Iterator, error) {
	kvCacheMtx.RLock()
	defer kvCacheMtx.RUnlock()
	stateKeyIndex, err := statekeyindex.NewProvider().OpenStateKeyIndex(ledgerID)
	if err != nil {
		return nil, err
	}
	return stateKeyIndex.GetIterator(namespace, startKey, endKey), nil

}

func purgeNonDurable(blockNumber uint64) {
	if blockNumber != 0 {
		for _, v := range kvCacheMap {
			v.purgePrivate(blockNumber)
		}
	}
}

func GetFromKVCache(chId string, namespace string, key string) (*VersionedValue, bool) {
	kvCache, _ := GetKVCache(chId, namespace)
	logger.Debugf("Looking for key[%s] in the cache chId[%s], namespace[%s]", key, chId, namespace)
	if validatedTx, ok := kvCache.Get(key); ok {
		versionedValue := &VersionedValue{
			Value: validatedTx.Value,
			Version: &version.Height{
				BlockNum: validatedTx.BlockNum,
				TxNum:    uint64(validatedTx.IndexInBlock),
			},
		}
		return versionedValue, true
	}

	logger.Debugf("Failed to find key[%s] in the cache chId[%s], namespace[%s]", key, chId, namespace)

	return nil, false
}

//GetRangeFromKVCache returns key range from cache and a flag to indicate of all keys are found in cache
func GetRangeFromKVCache(chId, namespace, startKey, endKey string) ([]string, bool) {
	kvCache, _ := GetKVCache(chId, namespace)
	sortedKeys := util.GetSortedKeys(kvCache.keys)
	var keyRange []string
	var foundStartKey, foundEndKey bool

	for _, k := range sortedKeys {
		if k == startKey {
			foundStartKey = true
		}
		if k == endKey {
			foundEndKey = true
			//exclude end key and end the range
			break
		}
		if foundStartKey {
			keyRange = append(keyRange, k)
		}

	}

	return keyRange, foundEndKey
}

//OnTxCommit when called pinned TX for given key gets removed
func OnTxCommit(validatedTxOps []ValidatedTxOp, validatedPvtData []ValidatedPvtData, validatedPvtHashData []ValidatedPvtData) {

	kvCacheMtx.Lock()
	defer kvCacheMtx.Unlock()

	for _, v := range validatedTxOps {
		kvCache, _ := getKVCache(v.ChId, v.Namespace)
		delete(kvCache.pinnedTx, v.Key)
	}

	for _, v := range validatedPvtData {
		namespace := DerivePvtDataNs(v.Namespace, v.Collection)
		kvCache, _ := getKVCache(v.ChId, namespace)
		delete(kvCache.pinnedTx, v.ValidatedTxOp.ValidatedTx.Key)
	}

	for _, v := range validatedPvtHashData {
		namespace := DerivePvtHashDataNs(v.Namespace, v.Collection)
		kvCache, _ := getKVCache(v.ChId, namespace)
		delete(kvCache.pinnedTx, v.ValidatedTxOp.ValidatedTx.Key)

	}
}

func newKVCache(
	cacheName string) *KVCache {
	cacheSize := ledgerconfig.GetKVCacheSize()

	validatedTxCache := lru.New(cacheSize)
	validatedTxCache.OnEvicted = cleanUpKeys(cacheName)
	nonDurablePvtCache := make(map[string]*ValidatedPvtData)
	expiringPvtKeys := make(map[uint64]*list.List)
	pinnedTx := make(map[string]*ValidatedTx)
	keys := make(map[string]struct{})

	cache := KVCache{
		cacheName:          cacheName,
		capacity:           cacheSize,
		validatedTxCache:   validatedTxCache,
		nonDurablePvtCache: nonDurablePvtCache,
		expiringPvtKeys:    expiringPvtKeys,
		pinnedTx:           pinnedTx,
		keys:               keys,
	}

	return &cache
}

//cleanUpKeys removes entry in cache.keys when corresponding cache entry gets purged
func cleanUpKeys(cacheName string) func(key lru.Key, value interface{}) {
	return func(key lru.Key, value interface{}) {
		kvCache, found := kvCacheMap[cacheName]
		if found {
			keyStr, _ := key.(string)
			delete(kvCache.keys, keyStr)
		}
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
			c.pinnedTx[validatedTx.Key] = &validatedTx.ValidatedTx
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
}

func (c *KVCache) Hit() uint64 {
	return c.hit
}

func (c *KVCache) Miss() uint64 {
	return c.miss
}
