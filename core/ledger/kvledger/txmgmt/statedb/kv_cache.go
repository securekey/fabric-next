/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package statedb

import (
	"sync"

	"github.com/golang/groupcache/lru"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb/statekeyindex"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
)

var logger = flogging.MustGetLogger("statedb")

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
	Collection string
}

type KVCache struct {
	cacheName        string
	capacity         int
	validatedTxCache *lru.Cache
	mutex            sync.Mutex
	hit              uint64
	miss             uint64
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

func UpdateKVCache(validatedTxOps []ValidatedTxOp, validatedPvtData []ValidatedPvtData, validatedPvtHashData []ValidatedPvtData, ledgerID string) error {
	kvCacheMtx.Lock()
	defer kvCacheMtx.Unlock()

	indexKeys := make([]statekeyindex.CompositeKey, 0)
	deletedIndexKeys := make([]statekeyindex.CompositeKey, 0)
	
	for _, v := range validatedTxOps {
		kvCache, _ := getKVCache(v.ChId, v.Namespace)
		if v.IsDeleted {
			kvCache.Remove(v.ValidatedTx.Key, v.ValidatedTx.BlockNum, v.ValidatedTx.IndexInBlock)
			deletedIndexKeys = append(deletedIndexKeys, statekeyindex.CompositeKey{Key: v.Key, Namespace: v.Namespace})
		} else {
			newTx := v.ValidatedTx
			kvCache.Put(&newTx)
			indexKeys = append(indexKeys, statekeyindex.CompositeKey{Key: v.Key, Namespace: v.Namespace})
		}
	}

	for _, v := range validatedPvtData {
		namespace := DerivePvtDataNs(v.Namespace, v.Collection)
		kvCache, _ := getKVCache(v.ChId, namespace)
		if v.IsDeleted {
			kvCache.Remove(v.Key, v.BlockNum, v.IndexInBlock)
			deletedIndexKeys = append(deletedIndexKeys, statekeyindex.CompositeKey{Key: v.Key, Namespace: namespace})
		} else {
			newTx := v.ValidatedTxOp.ValidatedTx
			kvCache.Put(&newTx)
			indexKeys = append(indexKeys, statekeyindex.CompositeKey{Key: v.Key, Namespace: namespace})
		}
	}
	
	for _, v := range validatedPvtHashData {
		namespace := DerivePvtHashDataNs(v.Namespace, v.Collection)
		kvCache, _ := getKVCache(v.ChId, namespace)
		if v.IsDeleted {
			kvCache.Remove(v.Key, v.BlockNum, v.IndexInBlock)
			deletedIndexKeys = append(deletedIndexKeys, statekeyindex.CompositeKey{Key: v.Key, Namespace: namespace})
		} else {
			newTx := v.ValidatedTxOp.ValidatedTx
			kvCache.Put(&newTx)
			indexKeys = append(indexKeys, statekeyindex.CompositeKey{Key: v.Key, Namespace: namespace})
		}
	}
	
	//Add key index in leveldb
	if len(indexKeys) > 0 {
		stateKeyIndex, err := statekeyindex.NewProvider().OpenStateKeyIndex(ledgerID)
		if err != nil {
			return err
		}
		err = stateKeyIndex.AddIndex(indexKeys)
		if err != nil {
			return err
		}
	}

	// Delete key index in leveldb
	if len(deletedIndexKeys) > 0 {
		stateKeyIndex, err := statekeyindex.NewProvider().OpenStateKeyIndex(ledgerID)
		if err != nil {
			return err
		}
		err = stateKeyIndex.DeleteIndex(deletedIndexKeys)
		if err != nil {
			return err
		}
	}

	return nil
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

func GetFromKVCache(chId string, namespace string, key string) (*VersionedValue, bool) {
	kvCache, _ := GetKVCache(chId, namespace)
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
	return nil, false
}

func newKVCache(
	cacheName string) *KVCache {
	cacheSize := ledgerconfig.GetKVCacheSize()

	validatedTxCache := lru.New(cacheSize)

	cache := KVCache{
		cacheName:        cacheName,
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
	return nil, false
}

func (c *KVCache) Put(validatedTx *ValidatedTx) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	exitingKeyVal, found := c.get(validatedTx.Key)
	// Add to the cache if the exisiting version is older
	if (found && exitingKeyVal.BlockNum < validatedTx.BlockNum) ||
		(found && exitingKeyVal.BlockNum == validatedTx.BlockNum && exitingKeyVal.IndexInBlock < validatedTx.IndexInBlock) || !found {
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
	if (found && exitingKeyVal.BlockNum < blockNum) ||
		(found && exitingKeyVal.BlockNum == blockNum && exitingKeyVal.IndexInBlock < indexInBlock) {
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
