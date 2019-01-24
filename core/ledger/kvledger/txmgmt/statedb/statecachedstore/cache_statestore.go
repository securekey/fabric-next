/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package statecachedstore

import (
	"sync"

	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb/kvcache"

	"sort"

	"github.com/hyperledger/fabric/core/common/ccprovider"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb/statekeyindex"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
)

var defVal struct{}

type cachedStateStore struct {
	vdb             statedb.VersionedDB
	bulkOptimizable statedb.BulkOptimizable
	indexCapable    statedb.IndexCapable
	ledgerID        string
}

func newCachedBlockStore(vdb statedb.VersionedDB, ledgerID string) *cachedStateStore {
	bulkOptimizable, _ := vdb.(statedb.BulkOptimizable)
	indexCapable, _ := vdb.(statedb.IndexCapable)

	s := cachedStateStore{
		vdb:             vdb,
		ledgerID:        ledgerID,
		bulkOptimizable: bulkOptimizable,
		indexCapable:    indexCapable,
	}
	return &s
}

// Open implements method in VersionedDB interface
func (c *cachedStateStore) Open() error {
	return c.vdb.Open()
}

// Close implements method in VersionedDB interface
func (c *cachedStateStore) Close() {
	c.vdb.Close()
}

// ValidateKeyValue implements method in VersionedDB interface
func (c *cachedStateStore) ValidateKeyValue(key string, value []byte) error {
	return c.vdb.ValidateKeyValue(key, value)
}

// BytesKeySuppoted implements method in VersionedDB interface
func (c *cachedStateStore) BytesKeySupported() bool {
	return c.vdb.BytesKeySupported()
}

func (c *cachedStateStore) GetKVCacheProvider() *kvcache.KVCacheProvider {
	return c.vdb.GetKVCacheProvider()
}

// GetState implements method in VersionedDB interface
func (c *cachedStateStore) GetState(namespace string, key string) (*statedb.VersionedValue, error) {
	if versionedValue, ok := c.vdb.GetKVCacheProvider().GetFromKVCache(c.ledgerID, namespace, key); ok {
		logger.Debugf("[%s] state retrieved from cache [ns=%s, key=%s]", c.ledgerID, namespace, key)
		/*metrics.IncrementCounter("cachestatestore_getstate_cache_request_hit")*/
		return &statedb.VersionedValue{versionedValue.Value, nil , versionedValue.Version}, nil
	}
	versionedValue, err := c.vdb.GetState(namespace, key)

	if versionedValue != nil && err == nil {
		validatedTx := kvcache.ValidatedTx{
			Key:          key,
			Value:        versionedValue.Value,
			BlockNum:     versionedValue.Version.BlockNum,
			IndexInBlock: int(versionedValue.Version.TxNum),
		}

		validatedTxOp := []kvcache.ValidatedTxOp{
			{
				Namespace:   namespace,
				ChId:        c.ledgerID,
				IsDeleted:   false,
				ValidatedTx: validatedTx,
			},
		}

		// Put retrieved KV from DB to the cache
		go func() {
			c.vdb.GetKVCacheProvider().UpdateKVCache(0, validatedTxOp, nil, nil, false)
		}()
	}

	return versionedValue, err
}

// GetVersion implements method in VersionedDB interface
func (c *cachedStateStore) GetVersion(namespace string, key string) (*version.Height, error) {
	returnVersion, keyFound := c.GetCachedVersion(namespace, key)
	if !keyFound {
		// nil/nil means notFound to callers
		return nil, nil

	}
	return returnVersion, nil
}

// GetStateMultipleKeys implements method in VersionedDB interface
func (c *cachedStateStore) GetStateMultipleKeys(namespace string, keys []string) ([]*statedb.VersionedValue, error) {
	vals := make([]*statedb.VersionedValue, len(keys))
	for i, key := range keys {
		val, err := c.GetState(namespace, key)
		if err != nil {
			return nil, err
		}
		vals[i] = val
	}
	return vals, nil
}

// GetStateRangeScanIterator implements method in VersionedDB interface
// startKey is inclusive
// endKey is exclusive
func (c *cachedStateStore) GetStateRangeScanIterator(namespace string, startKey string, endKey string) (statedb.ResultsIterator, error) {

	/*stopWatch := metrics.StopWatch("cachedStateStore_getStateRangeScanIterator")
	defer stopWatch()*/

	//get key range from cache
	keyRange := c.vdb.GetKVCacheProvider().GetRangeFromKVCache(c.ledgerID, namespace, startKey, endKey)

	//some keys are missing from cache, so need db iterator too to find tail of range
	dbItr, err := c.vdb.GetKVCacheProvider().GetLeveLDBIterator(namespace, startKey, endKey, c.ledgerID)
	if err != nil {
		return nil, err
	}

	if !dbItr.Next() && len(keyRange) == 0 {
		logger.Warningf("*** GetStateRangeScanIterator namespace %s startKey %s endKey %s not found going to db", namespace, startKey, endKey)
		return c.vdb.GetStateRangeScanIterator(namespace, startKey, endKey)
	}

	dbItr.Prev()
/*	metrics.IncrementCounter("cachestatestore_getstaterangescaniterator_cache_request_hit")
*/
	return newKVScanner(namespace, keyRange, dbItr, c), nil
}

func (c *cachedStateStore) GetNonDurableStateRangeScanIterator(namespace string, startKey string, endKey string) (statedb.ResultsIterator, error) {
	sortedKeys := c.vdb.GetKVCacheProvider().GetNonDurableSortedKeys(c.ledgerID, namespace)
	var nextIndex int
	var lastIndex int
	if startKey == "" {
		nextIndex = 0
	} else {
		nextIndex = sort.SearchStrings(sortedKeys, startKey)
	}
	if endKey == "" {
		lastIndex = len(sortedKeys)
	} else {
		lastIndex = sort.SearchStrings(sortedKeys, endKey)
	}
	return &nonDurableKVScanner{namespace, sortedKeys, nextIndex, lastIndex, c}, nil
}
// ExecuteQuery implements method in VersionedDB interface
func (c *cachedStateStore) ExecuteQuery(namespace, query string) (statedb.ResultsIterator, error) {
	return c.vdb.ExecuteQuery(namespace, query)
}
// ExecuteQuery implements method in VersionedDB interface
func (c *cachedStateStore) ExecuteQueryWithMetadata(namespace, query string,metadata map[string]interface{}) (statedb.QueryResultsIterator, error) {
	return c.vdb.ExecuteQueryWithMetadata(namespace, query, metadata)
}
// ApplyUpdates implements method in VersionedDB interface
func (c *cachedStateStore) ApplyUpdates(batch *statedb.UpdateBatch, height *version.Height) error {
	return c.vdb.ApplyUpdates(batch, height)
}

// GetLatestSavePoint implements method in VersionedDB interface
func (c *cachedStateStore) GetLatestSavePoint() (*version.Height, error) {
	return c.vdb.GetLatestSavePoint()
}
func (c *cachedStateStore) GetWSetCacheLock() *sync.RWMutex {
	return c.bulkOptimizable.GetWSetCacheLock()
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

type nonDurableKVScanner struct {
	namespace        string
	sortedKeys       []string
	nextIndex        int
	lastIndex        int
	cachedStateStore *cachedStateStore
}

// Next gives next key and versioned value. It returns a nil when exhausted
func (s *nonDurableKVScanner) Next() (statedb.QueryResult, error) {
	if s.nextIndex >= s.lastIndex {
		return nil, nil
	}
	key := s.sortedKeys[s.nextIndex]
	s.nextIndex++
	versionedValue, err := s.cachedStateStore.GetState(s.namespace, key)
	if err != nil {
		return nil, errors.Wrapf(err, "KVScanner next get value %s %s failed", s.namespace, key)
	}

	return &statedb.VersionedKV{
		CompositeKey:   statedb.CompositeKey{Namespace: s.namespace, Key: key},
		VersionedValue: *versionedValue}, nil
}

// Close implements the method from QueryResult interface
func (s *nonDurableKVScanner) Close() {
	// do nothing
}
