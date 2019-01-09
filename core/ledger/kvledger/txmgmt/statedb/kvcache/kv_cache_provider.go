/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/
package kvcache

import (
	"sync"

	"strings"

	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb/statekeyindex"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/core/ledger/util"
)

type KVCacheProvider struct {
	kvCacheMap map[string]*KVCache
	kvCacheMtx sync.Mutex
}

func NewKVCacheProvider() *KVCacheProvider {
	return &KVCacheProvider{kvCacheMap: make(map[string]*KVCache), kvCacheMtx: sync.Mutex{}}
}

func (p *KVCacheProvider) getKVCache(chId string, namespace string) (*KVCache, error) {
	cacheName := chId
	if len(namespace) > 0 {
		cacheName = cacheName + "_" + namespace
	}

	kvCache, found := p.kvCacheMap[cacheName]
	if !found {
		kvCache = newKVCache(cacheName)
		p.kvCacheMap[cacheName] = kvCache
	}

	return kvCache, nil
}

func (p *KVCacheProvider) GetKVCache(chId string, namespace string) (*KVCache, error) {
	p.kvCacheMtx.Lock()
	defer p.kvCacheMtx.Unlock()

	return p.getKVCache(chId, namespace)
}

func (p *KVCacheProvider) purgeNonDurable(blockNumber uint64) {
	if blockNumber != 0 {
		for _, v := range p.kvCacheMap {
			v.purgePrivate(blockNumber)
		}
	}
}

// UpdateKVCache will purge non durable data from the cache for the given blockNumber and update all caches with the
// provided validatedTxOps, validatedPvtData and validatedPvtHashData
func (p *KVCacheProvider) UpdateKVCache(blockNumber uint64, validatedTxOps []ValidatedTxOp, validatedPvtData []ValidatedPvtData, validatedPvtHashData []ValidatedPvtData, pin bool) {
	p.kvCacheMtx.Lock()
	defer p.kvCacheMtx.Unlock()

	p.purgeNonDurable(blockNumber)

	for _, v := range validatedTxOps {
		kvCache, _ := p.getKVCache(v.ChId, v.Namespace)
		if v.IsDeleted {
			kvCache.Remove(v.Key, v.BlockNum, v.IndexInBlock)
		} else {
			newTx := v.ValidatedTx
			kvCache.Put(&newTx, pin)
		}
	}
	chIDAndNamespace := make(map[string]struct{})
	for _, v := range validatedPvtData {
		namespace := DerivePvtDataNs(v.Namespace, v.Collection)
		kvCache, _ := p.getKVCache(v.ChId, namespace)
		chIDAndNamespace[v.ChId+"!"+namespace] = defVal
		if v.IsDeleted {
			kvCache.Remove(v.Key, v.BlockNum, v.IndexInBlock)
		} else {
			newTx := v
			kvCache.PutPrivate(&newTx, pin)
		}
	}

	for _, v := range validatedPvtHashData {
		namespace := DerivePvtHashDataNs(v.Namespace, v.Collection)
		kvCache, _ := p.getKVCache(v.ChId, namespace)
		if v.IsDeleted {
			kvCache.Remove(v.Key, v.BlockNum, v.IndexInBlock)
		} else {
			newTx := v
			kvCache.PutPrivate(&newTx, pin)
		}
	}
	//Sort non durable keys in background
	go func() {
		for k := range chIDAndNamespace {
			s := strings.Split(k, "!")
			kvCache, _ := p.getKVCache(s[0], s[1])
			kvCache.sortNonDurableKeys()
		}
	}()

}

// UpdateNonDurableKVCache will purge non durable data from the cache for the given blockNumber then update it with non durable
// private data only (validatedPvtData and validatedPvtHashData)
func (p *KVCacheProvider) UpdateNonDurableKVCache(blockNumber uint64, validatedPvtData []ValidatedPvtData, validatedPvtHashData []ValidatedPvtData) {
	p.kvCacheMtx.Lock()
	defer p.kvCacheMtx.Unlock()

	p.purgeNonDurable(blockNumber)

	for _, v := range validatedPvtData {
		namespace := DerivePvtDataNs(v.Namespace, v.Collection)
		kvCache, _ := p.getKVCache(v.ChId, namespace)

		newTx := v
		kvCache.PutPrivateNonDurable(&newTx)

	}

	for _, v := range validatedPvtHashData {
		namespace := DerivePvtHashDataNs(v.Namespace, v.Collection)
		kvCache, _ := p.getKVCache(v.ChId, namespace)
		newTx := v
		kvCache.PutPrivateNonDurable(&newTx)
	}
}

func (p *KVCacheProvider) GetFromKVCache(chId string, namespace string, key string) (*VersionedValue, bool) {
	kvCache, _ := p.GetKVCache(chId, namespace)
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

//OnTxCommit when called pinned TX for given key gets removed
func (p *KVCacheProvider) OnTxCommit(validatedTxOps []ValidatedTxOp, validatedPvtData []ValidatedPvtData, validatedPvtHashData []ValidatedPvtData) {
	p.kvCacheMtx.Lock()
	defer p.kvCacheMtx.Unlock()

	for _, v := range validatedTxOps {
		kvCache, _ := p.getKVCache(v.ChId, v.Namespace)
		delete(kvCache.pinnedTx, v.Key)
	}

	for _, v := range validatedPvtData {
		namespace := DerivePvtDataNs(v.Namespace, v.Collection)
		kvCache, _ := p.getKVCache(v.ChId, namespace)
		delete(kvCache.pinnedTx, v.ValidatedTxOp.ValidatedTx.Key)
	}

	for _, v := range validatedPvtHashData {
		namespace := DerivePvtHashDataNs(v.Namespace, v.Collection)
		kvCache, _ := p.getKVCache(v.ChId, namespace)
		delete(kvCache.pinnedTx, v.ValidatedTxOp.ValidatedTx.Key)

	}
}

func (p *KVCacheProvider) GetLeveLDBIterator(namespace, startKey, endKey, ledgerID string) (*leveldbhelper.Iterator, error) {
	p.kvCacheMtx.Lock()
	defer p.kvCacheMtx.Unlock()
	stateKeyIndex, err := statekeyindex.NewProvider().OpenStateKeyIndex(ledgerID)
	if err != nil {
		return nil, err
	}
	return stateKeyIndex.GetIterator(namespace, startKey, endKey), nil

}

func (p *KVCacheProvider) PrepareIndexUpdates(validatedTxOps []ValidatedTxOp, validatedPvtData []ValidatedPvtData, validatedPvtHashData []ValidatedPvtData) ([]*statekeyindex.IndexUpdate, []statekeyindex.CompositeKey) {

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

func (p *KVCacheProvider) ApplyIndexUpdates(indexUpdates []*statekeyindex.IndexUpdate, indexDeletes []statekeyindex.CompositeKey, ledgerID string) error {

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

//GetRangeFromKVCache returns key range from cache under given startKey(inclusive) and endKey(exclusive) range
//TODO possible memory issues if empty start/end key used in case of huge cache
func (p *KVCacheProvider) GetRangeFromKVCache(chId, namespace, startKey, endKey string) []string {

	p.kvCacheMtx.Lock()
	defer p.kvCacheMtx.Unlock()

	kvCache, _ := p.getKVCache(chId, namespace)
	sortedKeys := util.GetSortedKeys(kvCache.keys)
	var keyRange []string
	foundStartKey := startKey == ""

	for _, k := range sortedKeys {
		if k == startKey {
			foundStartKey = true
		}
		if k == endKey {
			//exclude end key and end the range
			break
		}
		if foundStartKey {
			keyRange = append(keyRange, k)
		}

	}

	return keyRange
}

//GetNonDurableSortedKeys returns non durable cache sorted keys
func (p *KVCacheProvider) GetNonDurableSortedKeys(chId, namespace string) []string {
	p.kvCacheMtx.Lock()
	defer p.kvCacheMtx.Unlock()

	kvCache, _ := p.getKVCache(chId, namespace)
/*	stopWatch := metrics.StopWatch("getnondurablesortedkeys_duration")
*/
    keys := kvCache.getNonDurableSortedKeys()
	//stopWatch()
	return keys
}
