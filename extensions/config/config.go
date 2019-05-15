/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config

import (
	"path/filepath"
	"time"

	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/spf13/viper"
)

const confCouchDB = "CouchDB"
const confGoleveldb = "goleveldb"
const confMissingPvtdataKeysStore = "missingPvtdataKeysStore"
const confStateKeyleveldb = "stateKeyLeveldb"
const confBlockCacheSize = "ledger.blockchain.blockCacheSize"
const confKVCacheSize = "ledger.blockchain.kvCacheSize"
const confPvtDataCacheSize = "ledger.blockchain.pvtDataStorage.cacheSize"
const confKVCacheBlocksToLive = "ledger.blockchain.kvCacheBlocksToLive"
const confKVCacheNonDurableSize = "ledger.blockchain.kvCacheNonDurableSize"
const confBlockStorage = "ledger.blockchain.blockStorage"
const confHistoryStorage = "ledger.state.historyStorage"
const confTransientStorage = "ledger.blockchain.transientStorage"
const confConfigHistoryStorage = "ledger.blockchain.configHistoryStorage"
const confRoles = "ledger.roles"
const confConcurrentBlockWrites = "ledger.concurrentBlockWrites"
const confValidationMinWaitTime = "ledger.blockchain.validation.minwaittime"
const confTransientDataLeveldb = "transientDataLeveldb"
const confTransientDataCleanupIntervalTime = "ledger.transientdata.cleanupExpired.Interval"
const confTransientDataCacheSize = "ledger.transientdata.cacheSize"
const confTransientDataPullTimeout = "peer.gossip.transientData.pullTimeout"
const confOLCollLeveldb = "offLedgerLeveldb"
const confOLCollCleanupIntervalTime = "offledger.cleanupExpired.Interval"
const confOLCollMaxPeersForRetrieval = "offledger.maxpeers"
const confOLCollPullTimeout = "offledger.gossip.pullTimeout"
const confOLCollCacheSize = "offledger.cacheSize"

// TODO: Dynamic configuration
// Configure channels that will be observed for sidetree txn
const confObserverChannels = "ledger.sidetree.observer.channels"

// TODO: couchDB config should be in a common section rather than being under state.
const confCouchDBMaxIdleConns = "ledger.state.couchDBConfig.maxIdleConns"
const confCouchDBMaxIdleConnsPerHost = "ledger.state.couchDBConfig.maxIdleConnsPerHost"
const confCouchDBIdleConnTimeout = "ledger.state.couchDBConfig.idleConnTimeout"
const confCouchDBKeepAliveTimeout = "ledger.state.couchDBConfig.keepAliveTimeout"
const confCouchDBHTTPTraceEnabled = "ledger.state.couchDBConfig.httpTraceEnabled"
const confPeerRestServerPort = "peer.restserver.port"
const confPeerIdentityHubEnabled = "peer.identityhub.enabled"

//default values
const defaultValidationMinWaitTime = 50 * time.Millisecond
const defaultTransientDataCleanupIntervalTime = 5 * time.Second
const defaultTransientDataCacheSize = 100000
const defaultTransientDataPullTimeout = 5 * time.Second
const defaultOLCollCleanupIntervalTime = 5 * time.Second
const defaultOLCollMaxPeersForRetrieval = 2
const defaultOLCollPullTimeout = 5 * time.Second
const defaultOLCollCacheSize = 10000
const defaultRestServerPort = 443
const defaultIdentityHubEnabled = false

const confBlockStorageAttachTxn = "ledger.blockchain.blockStorage.attachTransaction"

// BlockStorageProvider holds the configuration names of the available storage providers
type BlockStorageProvider int

const (
	// FilesystemLedgerStorage stores blocks in a raw file with a LevelDB index (default)
	FilesystemLedgerStorage BlockStorageProvider = iota
	// CouchDBLedgerStorage stores blocks in CouchDB
	CouchDBLedgerStorage
)

// PvtDataStorageProvider holds the configuration names of the available storage providers
type PvtDataStorageProvider int

const (
	// LevelDBPvtDataStorage stores private data in LevelDB (default)
	LevelDBPvtDataStorage PvtDataStorageProvider = iota
	// CouchDBPvtDataStorage stores private data in CouchDB
	CouchDBPvtDataStorage
)

// HistoryStorageProvider holds the configuration names of the available history storage providers
type HistoryStorageProvider int

const (
	// LevelDBHistoryStorage stores history in LevelDB (default)
	LevelDBHistoryStorage HistoryStorageProvider = iota
	// CouchDBHistoryStorage stores history in CouchDB
	CouchDBHistoryStorage
)

// TransientStorageProvider holds the configuration names of the available transient storage providers
type TransientStorageProvider int

const (
	// LevelDBTransientStorage stores transient data in LevelDB (default)
	LevelDBTransientStorage TransientStorageProvider = iota
	// CouchDBTransientStorage stores transient data in CouchDB
	CouchDBTransientStorage
	// MemoryTransientStorage stores transient data in Memory
	MemoryTransientStorage
)

// ConfigurationHistoryStorageProvider holds the configuration names of the available config history storage providers
type ConfigurationHistoryStorageProvider int

const (
	// LevelDBConfigHistoryStorage stores config history data in LevelDB (default)
	LevelDBConfigHistoryStorage ConfigurationHistoryStorageProvider = iota
	// CouchDBConfigHistoryStorage stores config history data in CouchDB
	CouchDBConfigHistoryStorage
)

// GetStateKeyLevelDBPath returns the filesystem path that is used to maintain the state key level db
func GetStateKeyLevelDBPath() string {
	return filepath.Join(ledgerconfig.GetRootPath(), confStateKeyleveldb)
}

// GetMissingPvtdataKeysStorePath returns the filesystem path that is used for permanent storage of private write-sets
func GetMissingPvtdataKeysStorePath() string {
	return filepath.Join(ledgerconfig.GetRootPath(), confMissingPvtdataKeysStore)
}

// GetCouchDBMaxIdleConns returns the number of idle connections to hold in the connection pool for couchDB.
func GetCouchDBMaxIdleConns() int {
	// TODO: this probably be the default golang version (100)
	const defaultMaxIdleConns = 1000
	if !viper.IsSet(confCouchDBMaxIdleConns) {
		return defaultMaxIdleConns
	}
	return viper.GetInt(confCouchDBMaxIdleConns)
}

// GetCouchDBMaxIdleConnsPerHost returns the number of idle connections to allow per host in the connection pool for couchDB.
func GetCouchDBMaxIdleConnsPerHost() int {
	// TODO: this probably be the default golang version (http.DefaultMaxIdleConnsPerHost)
	const defaultMaxIdleConnsPerHost = 100
	if !viper.IsSet(confCouchDBMaxIdleConnsPerHost) {
		return defaultMaxIdleConnsPerHost
	}
	return viper.GetInt(confCouchDBMaxIdleConnsPerHost)
}

// GetCouchDBIdleConnTimeout returns the duration before closing an idle connection.
func GetCouchDBIdleConnTimeout() time.Duration {
	const defaultIdleConnTimeout = 90 * time.Second
	if !viper.IsSet(confCouchDBIdleConnTimeout) {
		return defaultIdleConnTimeout
	}
	return viper.GetDuration(confCouchDBIdleConnTimeout)
}

// GetCouchDBKeepAliveTimeout returns the duration for keep alive.
func GetCouchDBKeepAliveTimeout() time.Duration {
	const defaultKeepAliveTimeout = 30 * time.Second
	if !viper.IsSet(confCouchDBKeepAliveTimeout) {
		return defaultKeepAliveTimeout
	}
	return viper.GetDuration(confCouchDBKeepAliveTimeout)
}

// GetBlockStoreProvider returns the block storage provider specified in the configuration
func GetBlockStoreProvider() BlockStorageProvider {
	blockStorageConfig := viper.GetString(confBlockStorage)
	switch blockStorageConfig {
	case confCouchDB:
		return CouchDBLedgerStorage
	default:
		fallthrough
	case "filesystem":
		return FilesystemLedgerStorage
	}
}

// GetBlockCacheSize returns the number of blocks to keep the in the LRU cache
func GetBlockCacheSize() int {
	blockCacheSize := viper.GetInt(confBlockCacheSize)
	if !viper.IsSet(confBlockCacheSize) {
		blockCacheSize = 5
	}
	return blockCacheSize
}

// GetPvtDataCacheSize returns the number of pvt data per block to keep the in the LRU cache
func GetPvtDataCacheSize() int {
	pvtDataCacheSize := viper.GetInt(confPvtDataCacheSize)
	if !viper.IsSet(confPvtDataCacheSize) {
		pvtDataCacheSize = 10
	}
	return pvtDataCacheSize
}

// GetKVCacheSize returns the cache size
func GetKVCacheSize() int {
	kvCacheSize := viper.GetInt(confKVCacheSize)
	if !viper.IsSet(confKVCacheSize) {
		kvCacheSize = 64 * 1024
	}
	return kvCacheSize
}

// GetKVCacheBlocksToLive returns the cache blocks to live
func GetKVCacheBlocksToLive() uint64 {
	if !viper.IsSet(confKVCacheBlocksToLive) {
		return 120
	}
	return uint64(viper.GetInt(confKVCacheBlocksToLive))
}

// GetKVCacheNonDurableSize returns the cache non durable size
func GetKVCacheNonDurableSize() int {
	if !viper.IsSet(confKVCacheNonDurableSize) {
		return 64 * 1024
	}
	return viper.GetInt(confKVCacheNonDurableSize)
}

// GetTransientStoreProvider returns the transient storage provider specified in the configuration
func GetTransientStoreProvider() TransientStorageProvider {
	transientStorageConfig := viper.GetString(confTransientStorage)
	switch transientStorageConfig {
	case confCouchDB:
		return CouchDBTransientStorage
	case "Memory":
		return MemoryTransientStorage
	default:
		fallthrough
	case confGoleveldb:
		return LevelDBTransientStorage
	}
}

// GetConfigHistoryStoreProvider returns the config history storage provider specified in the configuration
func GetConfigHistoryStoreProvider() ConfigurationHistoryStorageProvider {
	configHistoryStorageConfig := viper.GetString(confConfigHistoryStorage)
	switch configHistoryStorageConfig {
	case confCouchDB:
		return CouchDBConfigHistoryStorage
	default:
		fallthrough
	case confGoleveldb:
		return LevelDBConfigHistoryStorage
	}
}

// GetHistoryStoreProvider returns the history storage provider specified in the configuration
func GetHistoryStoreProvider() HistoryStorageProvider {
	historyStorageConfig := viper.GetString(confHistoryStorage)
	switch historyStorageConfig {
	case confCouchDB:
		return CouchDBHistoryStorage
	default:
		fallthrough
	case confGoleveldb:
		return LevelDBHistoryStorage
	}
}

// CouchDBHTTPTraceEnabled returns true if HTTP tracing is enabled for Couch DB
func CouchDBHTTPTraceEnabled() bool {
	return viper.GetBool(confCouchDBHTTPTraceEnabled)
}

// GetValidationMinWaitTime is used by the committer in distributed validation and is the minimum
// time to wait for Tx validation responses from other validators.
func GetValidationMinWaitTime() time.Duration {
	timeout := viper.GetDuration(confValidationMinWaitTime)
	if timeout == 0 {
		return defaultValidationMinWaitTime
	}
	return timeout
}

// GetConcurrentBlockWrites is how many concurrent writes to db
func GetConcurrentBlockWrites() int {
	concurrentWrites := viper.GetInt(confConcurrentBlockWrites)
	if !viper.IsSet(confConcurrentBlockWrites) {
		return 1
	}
	return concurrentWrites
}

// GetTransientDataLevelDBPath returns the filesystem path that is used to maintain the transient data level db
func GetTransientDataLevelDBPath() string {
	return filepath.Join(ledgerconfig.GetRootPath(), confTransientDataLeveldb)
}

// GetTransientDataExpiredIntervalTime is time when background routine check expired transient data in db to cleanup.
func GetTransientDataExpiredIntervalTime() time.Duration {
	timeout := viper.GetDuration(confTransientDataCleanupIntervalTime)
	if timeout == 0 {
		return defaultTransientDataCleanupIntervalTime
	}
	return timeout
}

// GetTransientDataCacheSize returns the size of the transient data cache
func GetTransientDataCacheSize() int {
	size := viper.GetInt(confTransientDataCacheSize)
	if size <= 0 {
		return defaultTransientDataCacheSize
	}
	return size
}

// GetRoles returns the roles of the peer. Empty return value indicates that the peer has all roles.
func GetRoles() string {
	return viper.GetString(confRoles)
}

// GetRestServerPort returns the port used for the rest server in the peer
func GetRestServerPort() int {
	if !viper.IsSet(confPeerRestServerPort) {
		return defaultRestServerPort
	}
	return viper.GetInt(confPeerRestServerPort)
}

// GetIdentityHubEnabled returns whether the identity hub is enabled in this peer
func GetIdentityHubEnabled() bool {
	if !viper.IsSet(confPeerIdentityHubEnabled) {
		return defaultIdentityHubEnabled
	}
	return viper.GetBool(confPeerIdentityHubEnabled)
}

// GetObserverChannels returns the channels that will be observed for sidetree txn.
func GetObserverChannels() string {
	return viper.GetString(confObserverChannels)
}

// GetOLCollLevelDBPath returns the filesystem path that is used to maintain the off-ledger level db
func GetOLCollLevelDBPath() string {
	return filepath.Join(ledgerconfig.GetRootPath(), confOLCollLeveldb)
}

// GetOLCollExpirationCheckInterval is time when background routine check expired collection data in db to cleanup.
func GetOLCollExpirationCheckInterval() time.Duration {
	timeout := viper.GetDuration(confOLCollCleanupIntervalTime)
	if timeout == 0 {
		return defaultOLCollCleanupIntervalTime
	}
	return timeout
}

// GetOLCollMaxPeersForRetrieval returns the number of peers that should be messaged
// to retrieve collection data that is not stored locally.
func GetOLCollMaxPeersForRetrieval() int {
	maxPeers := viper.GetInt(confOLCollMaxPeersForRetrieval)
	if maxPeers <= 0 {
		maxPeers = defaultOLCollMaxPeersForRetrieval
	}
	return maxPeers
}

// GetTransientDataPullTimeout is the amount of time a peer waits for a response from another peer for transient data.
func GetTransientDataPullTimeout() time.Duration {
	timeout := viper.GetDuration(confTransientDataPullTimeout)
	if timeout == 0 {
		timeout = defaultTransientDataPullTimeout
	}
	return timeout
}

// GetOLCollPullTimeout is the amount of time a peer waits for a response from another peer for transient data.
func GetOLCollPullTimeout() time.Duration {
	timeout := viper.GetDuration(confOLCollPullTimeout)
	if timeout == 0 {
		timeout = defaultOLCollPullTimeout
	}
	return timeout
}

// GetOLCollCacheSize returns the size of the off-ledger cache
func GetOLCollCacheSize() int {
	size := viper.GetInt(confOLCollCacheSize)
	if size <= 0 {
		return defaultOLCollCacheSize
	}
	return size
}

// GetBlockStorageAttachTxn returns whether or not the block storage provider should attach a copy
// of the transaction to the transaction ID index.
// TODO: this was made configurable to make it easier to measure the performance & storage differences.
// TODO: based on the analysis, we might remove this configuration.
func GetBlockStorageAttachTxn() bool {
	return viper.GetBool(confBlockStorageAttachTxn)
}
