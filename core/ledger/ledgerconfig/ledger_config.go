/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ledgerconfig

import (
	"path/filepath"
	"strings"
	"sync"

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
const confHistoryLeveldb = "historyLeveldb"
const confBookkeeper = "bookkeeper"
const confConfigHistory = "configHistory"
const confChains = "chains"
const confPvtdataStore = "pvtdataStore"
const confQueryLimit = "ledger.state.couchDBConfig.queryLimit"
const confEnableHistoryDatabase = "ledger.history.enableHistoryDatabase"
const confMaxBatchSize = "ledger.state.couchDBConfig.maxBatchUpdateSize"
const confAutoWarmIndexes = "ledger.state.couchDBConfig.autoWarmIndexes"
const confWarmIndexesAfterNBlocks = "ledger.state.couchDBConfig.warmIndexesAfterNBlocks"
const confBlockStorage = "ledger.blockchain.blockStorage"
const confPvtDataStorage = "ledger.blockchain.pvtDataStorage"
const confHistoryStorage = "ledger.state.historyStorage"
const confTransientStorage = "ledger.blockchain.transientStorage"
const confConfigHistoryStorage = "ledger.blockchain.configHistoryStorage"
const confBlockStorageAttachTxn = "ledger.blockchain.blockStorage.attachTransaction"
const confRoles = "ledger.roles"

// TODO: couchDB config should be in a common section rather than being under state.
const confCouchDBMaxIdleConns = "ledger.state.couchDBConfig.maxIdleConns"
const confCouchDBMaxIdleConnsPerHost = "ledger.state.couchDBConfig.maxIdleConnsPerHost"


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

//GetQueryLimit exposes the queryLimit variable
func GetQueryLimit() int {
	queryLimit := viper.GetInt(confQueryLimit)
	// if queryLimit was unset, default to 10000
	if !viper.IsSet(confQueryLimit) {
		queryLimit = 10000
	}
	return queryLimit
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

// GetTransientStoreProvider returns the transient storage provider specified in the configuration
func GetTransientStoreProvider() TransientStorageProvider {
	transientStorageConfig := viper.GetString(confTransientStorage)

	switch transientStorageConfig {
	case "CouchDB":
		return CouchDBTransientStorage
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

// GetBlockStorageAttachTxn returns whether or not the block storage provider should attach a copy
// of the transaction to the transaction ID index.
// TODO: this was made configurable to make it easier to measure the performance & storage differences.
// TODO: based on the analysis, we might remove this configuration.
func GetBlockStorageAttachTxn() bool {
	return viper.GetBool(confBlockStorageAttachTxn)
}

// Role is the role of the peer
type Role string

const (
	// CommitterRole indicates that the peer commits data to the ledger
	CommitterRole Role = "committer"
	// EndorserRole indicates that the peer endorses transaction proposals
	EndorserRole Role = "endorser"
)

var initOnce sync.Once
var roles map[Role]struct{}

// HasRole returns true if the peer has the given role
func HasRole(role Role) bool {
	initOnce.Do(func() {
		roles = getRoles()
	})
	_, ok := roles[role]
	return ok
}

//IsCommitter returns true if the peer is a committer, otherwise the peer does not commit to the DB
func IsCommitter() bool {
	return HasRole(CommitterRole)
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
		return map[Role]struct{}{
			EndorserRole:  exists,
			CommitterRole: exists,
		}
	}

	roles := make(map[Role]struct{})
	for _, r := range strings.Split(strRoles, ",") {
		roles[Role(r)] = exists
	}
	return roles
}
