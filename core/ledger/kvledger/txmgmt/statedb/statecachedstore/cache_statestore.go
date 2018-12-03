/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package statecachedstore

import (
	"github.com/hyperledger/fabric/common/metrics"
	"github.com/hyperledger/fabric/core/common/ccprovider"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb/statekeyindex"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

var defVal struct{}

type cachedStateStore struct {
	stateStore      statedb.VersionedDB
	bulkOptimizable statedb.BulkOptimizable
	indexCapable    statedb.IndexCapable
	ledgerID        string
	stateKeyIndex   statekeyindex.StateKeyIndex
}

func newCachedBlockStore(stateStore statedb.VersionedDB, stateKeyIndex statekeyindex.StateKeyIndex, ledgerID string) *cachedStateStore {
	bulkOptimizable, _ := stateStore.(statedb.BulkOptimizable)
	indexCapable, _ := stateStore.(statedb.IndexCapable)

	s := cachedStateStore{
		stateStore:      stateStore,
		ledgerID:        ledgerID,
		bulkOptimizable: bulkOptimizable,
		indexCapable:    indexCapable,
		stateKeyIndex:   stateKeyIndex,
	}
	return &s
}

// Open implements method in VersionedDB interface
func (c *cachedStateStore) Open() error {
	return c.stateStore.Open()
}

// Close implements method in VersionedDB interface
func (c *cachedStateStore) Close() {
	c.stateStore.Close()
}

// ValidateKeyValue implements method in VersionedDB interface
func (c *cachedStateStore) ValidateKeyValue(key string, value []byte) error {
	return c.stateStore.ValidateKeyValue(key, value)
}

// BytesKeySuppoted implements method in VersionedDB interface
func (c *cachedStateStore) BytesKeySuppoted() bool {
	return c.stateStore.BytesKeySuppoted()
}

// GetState implements method in VersionedDB interface
func (c *cachedStateStore) GetState(namespace string, key string) (*statedb.VersionedValue, error) {

	//TODO Add call to the cache interface first before go to db
	//TODO We will change it when Reza code is ready
	return c.stateStore.GetState(namespace, key)
}

// GetVersion implements method in VersionedDB interface
func (c *cachedStateStore) GetVersion(namespace string, key string) (*version.Height, error) {
	return c.stateStore.GetVersion(namespace, key)
}

// GetStateMultipleKeys implements method in VersionedDB interface
func (c *cachedStateStore) GetStateMultipleKeys(namespace string, keys []string) ([]*statedb.VersionedValue, error) {
	return c.stateStore.GetStateMultipleKeys(namespace, keys)
}

// GetStateRangeScanIterator implements method in VersionedDB interface
// startKey is inclusive
// endKey is exclusive
func (c *cachedStateStore) GetStateRangeScanIterator(namespace string, startKey string, endKey string) (statedb.ResultsIterator, error) {

	//get key range from cache
	keyRange, foundAllKeys := statedb.GetRangeFromKVCache(c.ledgerID, namespace, startKey, endKey)
	if foundAllKeys {
		//if found all keys inside range in cache, no need to go to db
		return newKVScanner(namespace, keyRange, nil, c), nil
	}

	//some keys are missing from cache, so need db iterator too to find tail of range
	dbItr, err := statedb.GetLeveLDBIterator(namespace, startKey, endKey, c.ledgerID)
	if err != nil {
		return nil, err
	}

	if !dbItr.Next() {
		logger.Warningf("*** GetStateRangeScanIterator namespace %s startKey %s endKey %s not found going to db", namespace, startKey, endKey)
		return c.stateStore.GetStateRangeScanIterator(namespace, startKey, endKey)
	}
	dbItr.Prev()
	if metrics.IsDebug() {
		metrics.RootScope.Counter("cachestatestore_getstaterangescaniterator_cache_request_hit").Inc(1)
	}

	return newKVScanner(namespace, keyRange, dbItr, c), nil
}

// ExecuteQuery implements method in VersionedDB interface
func (c *cachedStateStore) ExecuteQuery(namespace, query string) (statedb.ResultsIterator, error) {
	return c.stateStore.ExecuteQuery(namespace, query)
}

// ApplyUpdates implements method in VersionedDB interface
func (c *cachedStateStore) ApplyUpdates(batch *statedb.UpdateBatch, height *version.Height) error {
	return c.stateStore.ApplyUpdates(batch, height)
}

// GetLatestSavePoint implements method in VersionedDB interface
func (c *cachedStateStore) GetLatestSavePoint() (*version.Height, error) {
	return c.stateStore.GetLatestSavePoint()
}

func (c *cachedStateStore) LoadCommittedVersions(keys []*statedb.CompositeKey, preLoaded map[*statedb.CompositeKey]*version.Height) error {
	preloaded := make(map[*statedb.CompositeKey]*version.Height)
	notPreloaded := make([]*statedb.CompositeKey, 0)
	for _, key := range keys {
		metadata, found, err := c.stateKeyIndex.GetMetadata(&statekeyindex.CompositeKey{Key: key.Key, Namespace: key.Namespace})
		if err != nil {
			return errors.Wrapf(err, "failed to retrieve metadata from the stateindex for key: %v", key)
		}
		if found {
			preloaded[key] = version.NewHeight(metadata.BlockNumber, metadata.TxNumber)
		} else {
			notPreloaded = append(notPreloaded, key)
		}
	}
	err := c.bulkOptimizable.LoadCommittedVersions(notPreloaded, preloaded)
	if err != nil {
		return err
	}
	return nil
}

func (c *cachedStateStore) LoadWSetCommittedVersions(keys []*statedb.CompositeKey, keysExist []*statedb.CompositeKey) error {
	keysExist = make([]*statedb.CompositeKey, 0)
	keysNotExist := make([]*statedb.CompositeKey, 0)
	for _, key := range keys {
		_, found, err := c.stateKeyIndex.GetMetadata(&statekeyindex.CompositeKey{Key: key.Key, Namespace: key.Namespace})
		if err != nil {
			return errors.Wrapf(err, "failed to retrieve metadata from the stateindex for key: %v", key)
		}
		if found {
			keysExist = append(keysExist, key)
		} else {
			keysNotExist = append(keysNotExist, key)

		}
	}
	err := c.bulkOptimizable.LoadWSetCommittedVersions(keysNotExist, keysExist)
	if err != nil {
		return err
	}
	return nil
}

func (c *cachedStateStore) GetCachedVersion(namespace, key string) (*version.Height, bool) {
	return c.bulkOptimizable.GetCachedVersion(namespace, key)
}
func (c *cachedStateStore) ClearCachedVersions() {
	c.bulkOptimizable.ClearCachedVersions()
}

func (c *cachedStateStore) GetDBType() string {
	return c.indexCapable.GetDBType()
}
func (c *cachedStateStore) ProcessIndexesForChaincodeDeploy(namespace string, fileEntries []*ccprovider.TarFileEntry) error {
	return c.indexCapable.ProcessIndexesForChaincodeDeploy(namespace, fileEntries)
}

type kvScanner struct {
	namespace        string
	keyRange         []string
	dbItr            iterator.Iterator
	cachedStateStore *cachedStateStore
	index            int
	searchedKeys     map[string]struct{}
}

func newKVScanner(namespace string, keyRange []string, dbItr iterator.Iterator, cachedStateStore *cachedStateStore) *kvScanner {
	return &kvScanner{namespace, keyRange, dbItr, cachedStateStore, 0, make(map[string]struct{})}
}

func (scanner *kvScanner) Next() (statedb.QueryResult, error) {

	key, found := scanner.key()
	if !found {
		return nil, nil
	}

	versionedValue, err := scanner.cachedStateStore.GetState(scanner.namespace, key)
	if err != nil {
		return nil, errors.Wrapf(err, "KVScanner next get value %s %s failed", scanner.namespace, key)
	}

	return &statedb.VersionedKV{
		CompositeKey:   statedb.CompositeKey{Namespace: scanner.namespace, Key: key},
		VersionedValue: *versionedValue}, nil
}

//key fetches next available key from key range if not found then falls back to db iterator.
//flag will return false if key not found anywhere to indicate end of range
func (scanner *kvScanner) key() (string, bool) {
	if scanner.index < len(scanner.keyRange) {
		key := scanner.keyRange[scanner.index]
		scanner.searchedKeys[key] = defVal
		scanner.index++
		return key, true
	} else if scanner.dbItr != nil && scanner.dbItr.Next() {
		dbKey := scanner.dbItr.Key()
		_, key := statekeyindex.SplitCompositeKey(dbKey)
		//to avoid duplicates
		_, found := scanner.searchedKeys[key]
		if !found {
			return key, true
		} else {
			return scanner.key()
		}
	} else {
		return "", false
	}
}

func (scanner *kvScanner) Close() {
	if scanner.dbItr != nil {
		scanner.dbItr.Release()
	}
	scanner.keyRange = nil
	scanner.searchedKeys = nil
}
