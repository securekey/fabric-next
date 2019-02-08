/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/
package ledgerconfig

import (
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/hyperledger/fabric/core/config"
	"github.com/spf13/viper"
)

//IsCouchDBEnabled exposes the useCouchDB variable
func IsCouchDBEnabled() bool {
	stateDatabase := viper.GetString("ledger.state.stateDatabase")
	if stateDatabase == "CouchDB" {
		return true
	}
	return false
}

const confPeerFileSystemPath = "peer.fileSystemPath"
const confLedgersData = "ledgersData"
const confLedgerProvider = "ledgerProvider"
const confStateleveldb = "stateLeveldb"
const confStateKeyleveldb = "stateKeyLeveldb"
const confHistoryLeveldb = "historyLeveldb"
const confBookkeeper = "bookkeeper"
const confConfigHistory = "configHistory"
const confChains = "chains"
const confPvtdataStore = "pvtdataStore"
const confMissingPvtdataKeysStore = "missingPvtdataKeysStore"
const confTotalQueryLimit = "ledger.state.totalQueryLimit"
const confInternalQueryLimit = "ledger.state.couchDBConfig.internalQueryLimit"
const confEnableHistoryDatabase = "ledger.history.enableHistoryDatabase"
const confMaxBatchSize = "ledger.state.couchDBConfig.maxBatchUpdateSize"
const confAutoWarmIndexes = "ledger.state.couchDBConfig.autoWarmIndexes"
const confWarmIndexesAfterNBlocks = "ledger.state.couchDBConfig.warmIndexesAfterNBlocks"
const confBlockCacheSize = "ledger.blockchain.blockCacheSize"
const confKVCacheSize = "ledger.blockchain.kvCacheSize"
const confPvtDataCacheSize = "ledger.blockchain.pvtDataCacheSize"
const confKVCacheBlocksToLive = "ledger.blockchain.kvCacheBlocksToLive"
const confKVCacheNonDurableSize = "ledger.blockchain.kvCacheNonDurableSize"
const confBlockStorage = "ledger.blockchain.blockStorage"
const confPvtDataStorage = "ledger.blockchain.pvtDataStorage"
const confHistoryStorage = "ledger.state.historyStorage"
const confTransientStorage = "ledger.blockchain.transientStorage"
const confConfigHistoryStorage = "ledger.blockchain.configHistoryStorage"
const confRoles = "ledger.roles"
const confConcurrentBlockWrites = "ledger.concurrentBlockWrites"
const confValidationMinWaitTime = "ledger.blockchain.validation.minwaittime"

// TODO: couchDB config should be in a common section rather than being under state.
const confCouchDBMaxIdleConns = "ledger.state.couchDBConfig.maxIdleConns"
const confCouchDBMaxIdleConnsPerHost = "ledger.state.couchDBConfig.maxIdleConnsPerHost"
const confCouchDBIdleConnTimeout = "ledger.state.couchDBConfig.idleConnTimeout"
const confCouchDBKeepAliveTimeout = "ledger.state.couchDBConfig.keepAliveTimeout"
const confCouchDBHTTPTraceEnabled = "ledger.state.couchDBConfig.httpTraceEnabled"
const defaultValidationMinWaitTime = 50 * time.Millisecond

var confCollElgProcMaxDbBatchSize = &conf{"ledger.pvtdataStore.collElgProcMaxDbBatchSize", 5000}
var confCollElgProcDbBatchesInterval = &conf{"ledger.pvtdataStore.collElgProcDbBatchesInterval", 1000}

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
	// LevelDBPvtDataStorage stores transient data in LevelDB (default)
	LevelDBTransientStorage TransientStorageProvider = iota
	// CouchDBTransientStorage stores transient data in CouchDB
	CouchDBTransientStorage
	// MemoryTransientStorage stores transient data in Memory
	MemoryTransientStorage
)

// ConfigHistoryStorageProvider holds the configuration names of the available config history storage providers
type ConfigHistoryStorageProvider int

const (
	// LevelDBConfigHistoryStorage stores config history data in LevelDB (default)
	LevelDBConfigHistoryStorage ConfigHistoryStorageProvider = iota
	// CouchDBConfigHistoryStorage stores config history data in CouchDB
	CouchDBConfigHistoryStorage
)

// GetRootPath returns the filesystem path.
// All ledger related contents are expected to be stored under this path
func GetRootPath() string {
	sysPath := config.GetPath(confPeerFileSystemPath)
	return filepath.Join(sysPath, confLedgersData)
}

// GetLedgerProviderPath returns the filesystem path for storing ledger ledgerProvider contents
func GetLedgerProviderPath() string {
	return filepath.Join(GetRootPath(), confLedgerProvider)
}

// GetStateLevelDBPath returns the filesystem path that is used to maintain the state level db
func GetStateLevelDBPath() string {
	return filepath.Join(GetRootPath(), confStateleveldb)
}

// GetStateKeyLevelDBPath returns the filesystem path that is used to maintain the state key level db
func GetStateKeyLevelDBPath() string {
	return filepath.Join(GetRootPath(), confStateKeyleveldb)
}

// GetHistoryLevelDBPath returns the filesystem path that is used to maintain the history level db
func GetHistoryLevelDBPath() string {
	return filepath.Join(GetRootPath(), confHistoryLeveldb)
}

// GetBlockStorePath returns the filesystem path that is used for the chain block stores
func GetBlockStorePath() string {
	return filepath.Join(GetRootPath(), confChains)
}

// GetPvtdataStorePath returns the filesystem path that is used for permanent storage of private write-sets
func GetPvtdataStorePath() string {
	return filepath.Join(GetRootPath(), confPvtdataStore)
}

// GetMissingPvtdataKeysStorePath returns the filesystem path that is used for permanent storage of private write-sets
func GetMissingPvtdataKeysStorePath() string {
	return filepath.Join(GetRootPath(), confMissingPvtdataKeysStore)
}

// GetInternalBookkeeperPath returns the filesystem path that is used for bookkeeping the internal stuff by by KVledger (such as expiration time for pvt)
func GetInternalBookkeeperPath() string {
	return filepath.Join(GetRootPath(), confBookkeeper)
}

// GetConfigHistoryPath returns the filesystem path that is used for maintaining history of chaincodes collection configurations
func GetConfigHistoryPath() string {
	return filepath.Join(GetRootPath(), confConfigHistory)
}

// GetMaxBlockfileSize returns maximum size of the block file
func GetMaxBlockfileSize() int {
	return 64 * 1024 * 1024
}

// GetTotalQueryLimit exposes the totalLimit variable
func GetTotalQueryLimit() int {
	totalQueryLimit := viper.GetInt(confTotalQueryLimit)
	// if queryLimit was unset, default to 10000
	if !viper.IsSet(confTotalQueryLimit) {
		totalQueryLimit = 10000
	}
	return totalQueryLimit
}

// GetInternalQueryLimit exposes the queryLimit variable
func GetInternalQueryLimit() int {
	internalQueryLimit := viper.GetInt(confInternalQueryLimit)
	// if queryLimit was unset, default to 1000
	if !viper.IsSet(confInternalQueryLimit) {
		internalQueryLimit = 1000
	}
	return internalQueryLimit
}

//GetMaxBatchUpdateSize exposes the maxBatchUpdateSize variable
func GetMaxBatchUpdateSize() int {
	maxBatchUpdateSize := viper.GetInt(confMaxBatchSize)
	// if maxBatchUpdateSize was unset, default to 500
	if !viper.IsSet(confMaxBatchSize) {
		maxBatchUpdateSize = 500
	}
	return maxBatchUpdateSize
}

// GetPvtdataStorePurgeInterval returns the interval in the terms of number of blocks
// when the purge for the expired data would be performed
func GetPvtdataStorePurgeInterval() uint64 {
	purgeInterval := viper.GetInt("ledger.pvtdataStore.purgeInterval")
	if purgeInterval <= 0 {
		purgeInterval = 100
	}
	return uint64(purgeInterval)
}

// GetPvtdataSkipPurgeForCollections returns the list of collections that will be expired but not purged
func GetPvtdataSkipPurgeForCollections() []string {
	skipPurgeForCollections := viper.GetString("ledger.pvtdataStore.skipPurgeForCollections")
	return strings.Split(skipPurgeForCollections, ",")
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

// GetPvtdataStoreCollElgProcMaxDbBatchSize returns the maximum db batch size for converting
// the ineligible missing data entries to eligible missing data entries
func GetPvtdataStoreCollElgProcMaxDbBatchSize() int {
	collElgProcMaxDbBatchSize := viper.GetInt(confCollElgProcMaxDbBatchSize.Name)
	if collElgProcMaxDbBatchSize <= 0 {
		collElgProcMaxDbBatchSize = confCollElgProcMaxDbBatchSize.DefaultVal
	}
	return collElgProcMaxDbBatchSize
}

// GetPvtdataStoreCollElgProcDbBatchesInterval returns the minimum duration (in milliseconds) between writing
// two consecutive db batches for converting the ineligible missing data entries to eligible missing data entries
func GetPvtdataStoreCollElgProcDbBatchesInterval() int {
	collElgProcDbBatchesInterval := viper.GetInt(confCollElgProcDbBatchesInterval.Name)
	if collElgProcDbBatchesInterval <= 0 {
		collElgProcDbBatchesInterval = confCollElgProcDbBatchesInterval.DefaultVal
	}
	return collElgProcDbBatchesInterval
}

//IsHistoryDBEnabled exposes the historyDatabase variable
func IsHistoryDBEnabled() bool {
	return viper.GetBool(confEnableHistoryDatabase)
}

// IsQueryReadsHashingEnabled enables or disables computing of hash
// of range query results for phantom item validation
func IsQueryReadsHashingEnabled() bool {
	return true
}

// GetMaxDegreeQueryReadsHashing return the maximum degree of the merkle tree for hashes of
// of range query results for phantom item validation
// For more details - see description in kvledger/txmgmt/rwset/query_results_helper.go
func GetMaxDegreeQueryReadsHashing() uint32 {
	return 50
}

//IsAutoWarmIndexesEnabled exposes the autoWarmIndexes variable
func IsAutoWarmIndexesEnabled() bool {
	//Return the value set in core.yaml, if not set, the return true
	if viper.IsSet(confAutoWarmIndexes) {
		return viper.GetBool(confAutoWarmIndexes)
	}
	return true
}

//GetWarmIndexesAfterNBlocks exposes the warmIndexesAfterNBlocks variable
func GetWarmIndexesAfterNBlocks() int {
	warmAfterNBlocks := viper.GetInt(confWarmIndexesAfterNBlocks)
	// if warmIndexesAfterNBlocks was unset, default to 1
	if !viper.IsSet(confWarmIndexesAfterNBlocks) {
		warmAfterNBlocks = 1
	}
	return warmAfterNBlocks
}

type conf struct {
	Name       string
	DefaultVal int
}

// GetBlockStoreProvider returns the block storage provider specified in the configuration
func GetBlockStoreProvider() BlockStorageProvider {
	blockStorageConfig := viper.GetString(confBlockStorage)
	switch blockStorageConfig {
	case "CouchDB":
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
func GetKVCacheSize() int {
	kvCacheSize := viper.GetInt(confKVCacheSize)
	if !viper.IsSet(confKVCacheSize) {
		kvCacheSize = 64 * 1024
	}
	return kvCacheSize
}
func GetKVCacheBlocksToLive() uint64 {
	if !viper.IsSet(confKVCacheBlocksToLive) {
		return 120
	}
	return uint64(viper.GetInt(confKVCacheBlocksToLive))
}
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
	case "CouchDB":
		return CouchDBTransientStorage
	case "Memory":
		return MemoryTransientStorage
	default:
		fallthrough
	case "goleveldb":
		return LevelDBTransientStorage
	}
}

// GetConfigHistoryStoreProvider returns the config history storage provider specified in the configuration
func GetConfigHistoryStoreProvider() ConfigHistoryStorageProvider {
	configHistoryStorageConfig := viper.GetString(confConfigHistoryStorage)
	switch configHistoryStorageConfig {
	case "CouchDB":
		return CouchDBConfigHistoryStorage
	default:
		fallthrough
	case "goleveldb":
		return LevelDBConfigHistoryStorage
	}
}

// GetHistoryStoreProvider returns the history storage provider specified in the configuration
func GetHistoryStoreProvider() HistoryStorageProvider {
	historyStorageConfig := viper.GetString(confHistoryStorage)
	switch historyStorageConfig {
	case "CouchDB":
		return CouchDBHistoryStorage
	default:
		fallthrough
	case "goleveldb":
		return LevelDBHistoryStorage
	}
}

// GetPvtDataStoreProvider returns the private data storage provider specified in the configuration
func GetPvtDataStoreProvider() PvtDataStorageProvider {
	pvtDataStorageConfig := viper.GetString(confPvtDataStorage)
	switch pvtDataStorageConfig {
	case "CouchDB":
		return CouchDBPvtDataStorage
	default:
		fallthrough
	case "goleveldb":
		return LevelDBPvtDataStorage
	}
}

// Role is the role of the peer
type Role string

const (
	// CommitterRole indicates that the peer commits data to the ledger
	CommitterRole Role = "committer"
	// EndorserRole indicates that the peer endorses transaction proposals
	EndorserRole Role = "endorser"
	// ValidatorRole indicates that the peer validates the block
	ValidatorRole Role = "validator"
)

var initOnce sync.Once
var roles map[Role]struct{}

// HasRole returns true if the peer has the given role
func HasRole(role Role) bool {
	initOnce.Do(func() {
		roles = getRoles()
	})

	if len(roles) == 0 {
		// No roles were explicitly set, therefore the peer is assumed to have all roles.
		return true
	}

	_, ok := roles[role]
	return ok
}

// IsCommitter returns true if the peer is a committer, otherwise the peer does not commit to the DB
func IsCommitter() bool {
	return HasRole(CommitterRole)
}

// IsEndorser returns true if the peer is an endorser
func IsEndorser() bool {
	return HasRole(EndorserRole)
}

// IsValidator returns true if the peer is a validator
func IsValidator() bool {
	return HasRole(ValidatorRole)
}

// Roles returns the roles for the peer
func Roles() []Role {
	var ret []Role
	for role := range roles {
		ret = append(ret, role)
	}
	return ret
}

// RolesAsString returns the roles for the peer
func RolesAsString() []string {
	var ret []string
	for role := range roles {
		ret = append(ret, string(role))
	}
	return ret
}
func getRoles() map[Role]struct{} {
	exists := struct{}{}
	strRoles := viper.GetString(confRoles)
	if strRoles == "" {
		// The peer has all roles by default
		return map[Role]struct{}{}
	}
	roles := make(map[Role]struct{})
	for _, r := range strings.Split(strRoles, ",") {
		roles[Role(r)] = exists
	}
	return roles
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
