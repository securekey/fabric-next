/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package statecouchdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb/statekeyindex"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/metrics"
	"github.com/hyperledger/fabric/core/common/ccprovider"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb/kvcache"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
)

var logger = flogging.MustGetLogger("statecouchdb")

// querySkip is implemented for future use by query paging
// currently defaulted to 0 and is not used
const querySkip = 0

// VersionedDBProvider implements interface VersionedDBProvider
type VersionedDBProvider struct {
	couchInstance *couchdb.CouchInstance
	databases     map[string]*VersionedDB
	mux           sync.Mutex
	openCounts    uint64
}

// NewVersionedDBProvider instantiates VersionedDBProvider
func NewVersionedDBProvider() (*VersionedDBProvider, error) {
	logger.Debugf("constructing CouchDB VersionedDBProvider")
	couchDBDef := couchdb.GetCouchDBDefinition()
	couchInstance, err := couchdb.CreateCouchInstance(couchDBDef.URL, couchDBDef.Username, couchDBDef.Password,
		couchDBDef.MaxRetries, couchDBDef.MaxRetriesOnStartup, couchDBDef.RequestTimeout, couchDBDef.CreateGlobalChangesDB)
	if err != nil {
		return nil, err
	}
	return &VersionedDBProvider{couchInstance, make(map[string]*VersionedDB), sync.Mutex{}, 0}, nil
}

// GetDBHandle gets the handle to a named database
func (provider *VersionedDBProvider) GetDBHandle(dbName string) (statedb.VersionedDB, error) {
	provider.mux.Lock()
	defer provider.mux.Unlock()
	vdb := provider.databases[dbName]
	if vdb == nil {
		var err error
		vdb, err = newVersionedDB(provider.couchInstance, dbName)
		if err != nil {
			return nil, err
		}
		provider.databases[dbName] = vdb
	}
	return vdb, nil
}

// Close closes the underlying db instance
func (provider *VersionedDBProvider) Close() {
	// No close needed on Couch
}

// VersionedDB implements VersionedDB interface
type VersionedDB struct {
	kvCacheProvider        *kvcache.KVCacheProvider
	couchInstance          *couchdb.CouchInstance
	couchCheckpointRev     string
	metadataDB             *couchdb.CouchDatabase            // A database per channel to store metadata such as savepoint.
	chainName              string                            // The name of the chain/channel.
	namespaceDBs           map[string]*couchdb.CouchDatabase // One database per deployed chaincode.
	committedDataCache     *versionsCache                    // Used as a local cache during bulk processing of a block.
	verCacheLock           sync.RWMutex
	mux                    sync.RWMutex
	committedWSetDataCache map[uint64]*versionsCache // Used as a local cache during bulk processing of a block.
	verWSetCacheLock       *sync.RWMutex
	stateKeyIndexReadyCh   chan struct{}
}

// newVersionedDB constructs an instance of VersionedDB
func newVersionedDB(couchInstance *couchdb.CouchInstance, dbName string) (*VersionedDB, error) {
	// CreateCouchDatabase creates a CouchDB database object, as well as the underlying database if it does not exist
	chainName := dbName
	dbName = couchdb.ConstructMetadataDBName(dbName)

	metadataDB, err := createCouchDatabase(couchInstance, dbName)
	if err != nil {
		return nil, err
	}

	kvCacheProvider := kvcache.NewKVCacheProvider()
	namespaceDBMap := make(map[string]*couchdb.CouchDatabase)
	return &VersionedDB{
		kvCacheProvider:    kvCacheProvider,
		couchInstance:      couchInstance,
		metadataDB:         metadataDB,
		chainName:          chainName,
		namespaceDBs:       namespaceDBMap,
		committedDataCache: newVersionCache(),
		mux:                sync.RWMutex{},
		committedWSetDataCache: make(map[uint64]*versionsCache),
		verWSetCacheLock:       &sync.RWMutex{},
		stateKeyIndexReadyCh:   make(chan struct{}),
	}, nil
}

func createCouchDatabase(couchInstance *couchdb.CouchInstance, dbName string) (*couchdb.CouchDatabase, error) {
	if ledgerconfig.IsCommitter() {
		return couchdb.CreateCouchDatabase(couchInstance, dbName)
	}

	return createCouchDatabaseEndorser(couchInstance, dbName)
}

type dbNotFoundError struct {
	name string
}

func newDBNotFoundError(name string) dbNotFoundError {
	return dbNotFoundError{name: name}
}

func (e dbNotFoundError) Error() string {
	return fmt.Sprintf("DB not found: [%s]", e.name)
}

func isDBNotFoundForEndorser(err error) bool {
	if ledgerconfig.IsCommitter() {
		return false
	}
	_, ok := err.(dbNotFoundError)
	return ok
}

func createCouchDatabaseEndorser(couchInstance *couchdb.CouchInstance, dbName string) (*couchdb.CouchDatabase, error) {
	db, err := couchdb.NewCouchDatabase(couchInstance, dbName)
	if err != nil {
		return nil, err
	}

	dbExists, err := db.ExistsWithRetry()
	if err != nil {
		return nil, err
	}

	if !dbExists {
		return nil, newDBNotFoundError(db.DBName)
	}

	return db, nil
}

func (vdb *VersionedDB) GetKVCacheProvider() *kvcache.KVCacheProvider {
	return vdb.kvCacheProvider
}

// getNamespaceDBHandle gets the handle to a named chaincode database
func (vdb *VersionedDB) getNamespaceDBHandle(namespace string) (*couchdb.CouchDatabase, error) {
	vdb.mux.RLock()
	db := vdb.namespaceDBs[namespace]
	vdb.mux.RUnlock()
	if db != nil {
		return db, nil
	}
	namespaceDBName := couchdb.ConstructNamespaceDBName(vdb.chainName, namespace)
	vdb.mux.Lock()
	defer vdb.mux.Unlock()
	db = vdb.namespaceDBs[namespace]
	if db == nil {
		var err error
		db, err = createCouchDatabase(vdb.couchInstance, namespaceDBName)
		if err != nil {
			return nil, err
		}
		vdb.namespaceDBs[namespace] = db
	}
	return db, nil
}

// ProcessIndexesForChaincodeDeploy creates indexes for a specified namespace
func (vdb *VersionedDB) ProcessIndexesForChaincodeDeploy(namespace string, fileEntries []*ccprovider.TarFileEntry) error {

	db, err := vdb.getNamespaceDBHandle(namespace)
	if err != nil {
		return err
	}

	for _, fileEntry := range fileEntries {
		indexData := fileEntry.FileContent
		filename := fileEntry.FileHeader.Name
		_, err = db.CreateIndex(string(indexData))
		if err != nil {
			return fmt.Errorf("error during creation of index from file=[%s] for chain=[%s]. Error=%s",
				filename, namespace, err)
		}
	}

	return nil

}

func (vdb *VersionedDB) GetDBType() string {
	return "couchdb"
}

// LoadCommittedVersions populates committedVersions and revisionNumbers into cache.
// A bulk retrieve from couchdb is used to populate the cache.
// committedVersions cache will be used for state validation of readsets
// revisionNumbers cache will be used during commit phase for couchdb bulk updates
func (vdb *VersionedDB) LoadCommittedVersions(notPreloaded []*statedb.CompositeKey, preLoaded map[*statedb.CompositeKey]*statekeyindex.Metadata) error {
	committedDataCache := newVersionCache()
	for _, compositeKey := range notPreloaded {
		ns, key := compositeKey.Namespace, compositeKey.Key
		committedDataCache.setVer(ns, key, nil)
	}
	vdb.verCacheLock.Lock()
	defer vdb.verCacheLock.Unlock()
	vdb.committedDataCache = committedDataCache
	for key, md := range preLoaded {
		vdb.committedDataCache.setVer(key.Namespace, key.Key, version.NewHeight(md.BlockNumber, md.TxNumber))
		vdb.committedDataCache.setRev(key.Namespace, key.Key, md.DBTag)
	}
	return nil
}

func (vdb *VersionedDB) GetWSetCacheLock() *sync.RWMutex {
	return vdb.verWSetCacheLock
}

func (vdb *VersionedDB) LoadWSetCommittedVersions(keys []*statedb.CompositeKey, keysExist map[*statedb.CompositeKey]*statekeyindex.Metadata, blockNum uint64) error {
	nsKeysMap := map[string][]string{}
	committedWSetDataCache := newVersionCache()
	for _, compositeKey := range keys {
		ns, key := compositeKey.Namespace, compositeKey.Key
		// in case we don't find it in CouchDB with the 'retrieveMetadata()' call
		committedWSetDataCache.setRev(ns, key, "")
		logger.Debugf("Load into version cache: %s~%s", ns, key)
		nsKeysMap[compositeKey.Namespace] = append(nsKeysMap[compositeKey.Namespace], compositeKey.Key)
	}

	if len(nsKeysMap) > 0 {
		nsMetadataMap, err := vdb.retrieveMetadata(nsKeysMap, true)
		logger.Debugf("nsKeysMap=%s", nsKeysMap)
		logger.Debugf("nsMetadataMap=%s", nsMetadataMap)
		if err != nil {
			return err
		}
		for ns, nsMetadata := range nsMetadataMap {
			for _, keyMetadata := range nsMetadata {
				logger.Debugf("Load into version cache: %s~%s", ns, keyMetadata.ID)
				committedWSetDataCache.setRev(ns, keyMetadata.ID, keyMetadata.Rev)
			}
		}
	}
	for key, metadata := range keysExist {
		committedWSetDataCache.setVer(key.Namespace, key.Key, version.NewHeight(metadata.BlockNumber, metadata.TxNumber))
		committedWSetDataCache.setRev(key.Namespace, key.Key, metadata.DBTag)
	}
	vdb.verCacheLock.Lock()
	defer vdb.verCacheLock.Unlock()
	vdb.committedWSetDataCache[blockNum] = committedWSetDataCache
	return nil
}

// GetVersion implements method in VersionedDB interface
func (vdb *VersionedDB) GetVersion(namespace string, key string) (*version.Height, error) {
	panic("unreachable (the logic moved to cached state store)")
	returnVersion, keyFound := vdb.GetCachedVersion(namespace, key)
	if !keyFound {
		// This if block get executed only during simulation because during commit
		// we always call `LoadCommittedVersions` before calling `GetVersion`
		vv, err := vdb.GetState(namespace, key)
		if err != nil || vv == nil {
			return nil, err
		}
		returnVersion = vv.Version
	}
	return returnVersion, nil
}

// GetCachedVersion returns version from cache. `LoadCommittedVersions` function populates the cache
func (vdb *VersionedDB) GetCachedVersion(namespace string, key string) (*version.Height, bool) {
	logger.Debugf("Retrieving cached version: %s~%s", key, namespace)
	vdb.verCacheLock.RLock()
	defer vdb.verCacheLock.RUnlock()
	return vdb.committedDataCache.getVersion(namespace, key)
}

// ValidateKeyValue implements method in VersionedDB interface
func (vdb *VersionedDB) ValidateKeyValue(key string, value []byte) error {
	err := validateKey(key)
	if err != nil {
		return err
	}
	return validateValue(value)
}

// BytesKeySuppoted implements method in VersionedDB interface
func (vdb *VersionedDB) BytesKeySuppoted() bool {
	return false
}

// GetState implements method in VersionedDB interface
func (vdb *VersionedDB) GetState(namespace string, key string) (*statedb.VersionedValue, error) {
	logger.Debugf("GetState(). ns=%s, key=%s", namespace, key)
	db, err := vdb.getNamespaceDBHandle(namespace)
	if err != nil {
		if isDBNotFoundForEndorser(err) {
			logger.Debugf("DB [%s] Not Found. Returning nil since I'm an endorser.", namespace)
			return nil, nil
		}
		return nil, err
	}
	couchDoc, _, err := db.ReadDoc(key)
	if err != nil {
		return nil, err
	}
	if couchDoc == nil {
		return nil, nil
	}
	kv, err := couchDocToKeyValue(couchDoc)
	if err != nil {
		return nil, err
	}

	logger.Debugf("state retrieved from DB. ns=%s, chainName=%s, key=%s", namespace, vdb.chainName, key)
	metrics.IncrementCounter("cachestatestore_getstate_cache_request_miss")
	return kv.VersionedValue, nil
}

// GetStateMultipleKeys implements method in VersionedDB interface
func (vdb *VersionedDB) GetStateMultipleKeys(namespace string, keys []string) ([]*statedb.VersionedValue, error) {
	panic("unreachable, (the logic moved to cached state store)")

	vals := make([]*statedb.VersionedValue, len(keys))
	for i, key := range keys {
		val, err := vdb.GetState(namespace, key)
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
func (vdb *VersionedDB) GetStateRangeScanIterator(namespace string, startKey string, endKey string) (statedb.ResultsIterator, error) {
	// Get the querylimit from core.yaml
	queryLimit := ledgerconfig.GetQueryLimit()
	db, err := vdb.getNamespaceDBHandle(namespace)
	if err != nil {
		if isDBNotFoundForEndorser(err) {
			logger.Debugf("DB [%s] Not Found. Returning empty range scanner since I'm an endorser.", namespace)
			return newQueryScanner(namespace, nil), nil
		}
		return nil, err
	}
	queryResult, err := db.ReadDocRange(startKey, endKey, queryLimit, querySkip, false)
	if err != nil {
		logger.Debugf("Error calling ReadDocRange(): %s\n", err.Error())
		return nil, err
	}
	if len(queryResult) != 0 {
		metrics.IncrementCounter("cachestatestore_getstaterangescaniterator_cache_request_miss")
	}

	logger.Debugf("Exiting GetStateRangeScanIterator")
	return newQueryScanner(namespace, queryResult), nil
}

func (vdb *VersionedDB) GetNonDurableStateRangeScanIterator(namespace string, startKey string, endKey string) (statedb.ResultsIterator, error) {
	return vdb.GetStateRangeScanIterator(namespace, startKey, endKey)
}

// ExecuteQuery implements method in VersionedDB interface
func (vdb *VersionedDB) ExecuteQuery(namespace, query string) (statedb.ResultsIterator, error) {
	// Get the querylimit from core.yaml
	queryLimit := ledgerconfig.GetQueryLimit()
	// Explicit paging not yet supported.
	// Use queryLimit from config and 0 skip.
	queryString, err := applyAdditionalQueryOptions(query, queryLimit, 0)
	if err != nil {
		logger.Debugf("Error calling applyAdditionalQueryOptions(): %s\n", err.Error())
		return nil, err
	}
	db, err := vdb.getNamespaceDBHandle(namespace)
	if err != nil {
		if isDBNotFoundForEndorser(err) {
			logger.Debugf("DB [%s] Not Found. Returning empty range scanner since I'm an endorser.", namespace)
			return newQueryScanner(namespace, nil), nil
		}
		return nil, err
	}
	queryResult, err := db.QueryDocuments(queryString)
	if err != nil {
		logger.Debugf("Error calling QueryDocuments(): %s\n", err.Error())
		return nil, err
	}
	logger.Debugf("Exiting ExecuteQuery")
	return newQueryScanner(namespace, queryResult), nil
}

// ApplyUpdates implements method in VersionedDB interface
func (vdb *VersionedDB) ApplyUpdates(updates *statedb.UpdateBatch, height *version.Height) error {
	stopWatch := metrics.StopWatch("statecouchdb_ApplyUpdates_duration")
	defer stopWatch()

	// TODO a note about https://jira.hyperledger.org/browse/FAB-8622
	// the function `Apply update can be split into three functions. Each carrying out one of the following three stages`.
	// The write lock is needed only for the stage 2.

	// stage 1 - PrepareForUpdates - db transforms the given batch in the form of underlying db
	// and keep it in memory
	var updateBatches []batch
	var err error
	if updateBatches, err = vdb.buildCommitters(updates, height.BlockNum); err != nil {
		return err
	}
	// stage 2 - ApplyUpdates push the changes to the DB
	if err = executeBatches(updateBatches); err != nil {
		return err
	}

	// Stgae 3 - PostUpdateProcessing - flush and record savepoint.
	namespaces := updates.GetUpdatedNamespaces()
	// Record a savepoint at a given height
	if err = vdb.ensureFullCommitAndRecordSavepoint(height, namespaces); err != nil {
		logger.Errorf("Error during recordSavepoint: %s\n", err.Error())
		return err
	}
	return nil
}

// ClearCachedVersions clears committedVersions and revisionNumbers
func (vdb *VersionedDB) ClearCachedVersions() {
	logger.Debugf("Clear Cache")
	vdb.verCacheLock.Lock()
	vdb.committedDataCache = newVersionCache()
	vdb.verCacheLock.Unlock()
}

// Open implements method in VersionedDB interface
func (vdb *VersionedDB) Open() error {
	// no need to open db since a shared couch instance is used
	return nil
}

// Close implements method in VersionedDB interface
func (vdb *VersionedDB) Close() {
	// no need to close db since a shared couch instance is used
	close(vdb.stateKeyIndexReadyCh)
}

// Savepoint docid (key) for couchdb
const savepointDocID = "statedb_savepoint"

// ensureFullCommitAndRecordSavepoint flushes all the dbs (corresponding to `namespaces`) to disk
// and Record a savepoint in the metadata db.
// Couch parallelizes writes in cluster or sharded setup and ordering is not guaranteed.
// Hence we need to fence the savepoint with sync. So ensure_full_commit on all updated
// namespace DBs is called before savepoint to ensure all block writes are flushed. Savepoint
// itself is flushed to the metadataDB.
func (vdb *VersionedDB) ensureFullCommitAndRecordSavepoint(height *version.Height, namespaces []string) error {
	// ensure full commit to flush all changes on updated namespaces until now to disk
	// namespace also includes empty namespace which is nothing but metadataDB
	var dbs []*couchdb.CouchDatabase
	for _, ns := range namespaces {
		db, err := vdb.getNamespaceDBHandle(ns)
		if err != nil {
			return err
		}
		dbs = append(dbs, db)
	}

	vdb.warmupAllIndexes(dbs)

	// construct savepoint document and save
	savepointCouchDoc, err := encodeSavepoint(height)
	if err != nil {
		return err
	}
	stopWatch := metrics.StopWatch("statecouchdb_ensurefullcommitandrecordsavepoint_savedoc_duration")
	rev, err := vdb.metadataDB.SaveDoc(savepointDocID, vdb.couchCheckpointRev, savepointCouchDoc)
	if err != nil {
		logger.Errorf("Failed to save the savepoint to DB %s\n", err.Error())
		stopWatch()
		return err
	}
	stopWatch()
	vdb.couchCheckpointRev = rev

	// Note: Ensure full commit on metadataDB after storing the savepoint is not necessary
	// as CouchDB syncs states to disk periodically (every 1 second). If peer fails before
	// syncing the savepoint to disk, ledger recovery process kicks in to ensure consistency
	// between CouchDB and block store on peer restart
	return nil
}

// GetLatestSavePoint implements method in VersionedDB interface
func (vdb *VersionedDB) GetLatestSavePoint() (*version.Height, error) {
	var err error
	couchDoc, _, err := vdb.metadataDB.ReadDoc(savepointDocID)
	if err != nil {
		logger.Errorf("Failed to read savepoint data %s\n", err.Error())
		return nil, err
	}
	// ReadDoc() not found (404) will result in nil response, in these cases return height nil
	if couchDoc == nil || couchDoc.JSONValue == nil {
		return nil, nil
	}
	return decodeSavepoint(couchDoc)
}

func (vdb *VersionedDB) IndexReadyChan() chan struct{} {
	return vdb.stateKeyIndexReadyCh
}

// applyAdditionalQueryOptions will add additional fields to the query required for query processing
func applyAdditionalQueryOptions(queryString string, queryLimit, querySkip int) (string, error) {
	const jsonQueryFields = "fields"
	const jsonQueryLimit = "limit"
	const jsonQuerySkip = "skip"
	//create a generic map for the query json
	jsonQueryMap := make(map[string]interface{})
	//unmarshal the selector json into the generic map
	decoder := json.NewDecoder(bytes.NewBuffer([]byte(queryString)))
	decoder.UseNumber()
	err := decoder.Decode(&jsonQueryMap)
	if err != nil {
		return "", err
	}
	if fieldsJSONArray, ok := jsonQueryMap[jsonQueryFields]; ok {
		switch fieldsJSONArray.(type) {
		case []interface{}:
			//Add the "_id", and "version" fields,  these are needed by default
			jsonQueryMap[jsonQueryFields] = append(fieldsJSONArray.([]interface{}),
				idField, versionField)
		default:
			return "", fmt.Errorf("fields definition must be an array")
		}
	}
	// Add limit
	// This will override any limit passed in the query.
	// Explicit paging not yet supported.
	jsonQueryMap[jsonQueryLimit] = queryLimit
	// Add skip of 0.
	// This will override any skip passed in the query.
	// Explicit paging not yet supported.
	jsonQueryMap[jsonQuerySkip] = querySkip
	//Marshal the updated json query
	editedQuery, err := json.Marshal(jsonQueryMap)
	if err != nil {
		return "", err
	}
	logger.Debugf("Rewritten query: %s", editedQuery)
	return string(editedQuery), nil
}

type queryScanner struct {
	cursor    int
	namespace string
	results   []*couchdb.QueryResult
}

func newQueryScanner(namespace string, queryResults []*couchdb.QueryResult) *queryScanner {
	return &queryScanner{-1, namespace, queryResults}
}

func (scanner *queryScanner) Next() (statedb.QueryResult, error) {
	scanner.cursor++
	if scanner.cursor >= len(scanner.results) {
		return nil, nil
	}
	selectedResultRecord := scanner.results[scanner.cursor]
	key := selectedResultRecord.ID

	// remove the reserved fields from CouchDB JSON and return the value and version
	kv, err := couchDocToKeyValue(&couchdb.CouchDoc{JSONValue: selectedResultRecord.Value, Attachments: selectedResultRecord.Attachments})
	if err != nil {
		return nil, err
	}
	return &statedb.VersionedKV{
		CompositeKey:   statedb.CompositeKey{Namespace: scanner.namespace, Key: key},
		VersionedValue: *kv.VersionedValue}, nil
}

func (scanner *queryScanner) Close() {
	scanner = nil
}
