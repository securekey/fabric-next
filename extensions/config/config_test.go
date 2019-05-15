/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package config

import (
	"os"
	"testing"
	"time"

	ledgertestutil "github.com/hyperledger/fabric/core/ledger/testutil"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestGetBlockStoreProviderDefault(t *testing.T) {

	oldVal := viper.Get(confBlockStorage)
	defer viper.Set(confBlockStorage, oldVal)

	viper.Set(confBlockStorage, "")
	provider := GetBlockStoreProvider()
	assert.Equal(t, provider, FilesystemLedgerStorage)

	viper.Set(confBlockStorage, "CouchDB")
	provider = GetBlockStoreProvider()
	assert.Equal(t, provider, CouchDBLedgerStorage)

	viper.Set(confBlockStorage, "filesystem")
	provider = GetBlockStoreProvider()
	assert.Equal(t, provider, FilesystemLedgerStorage)
}

func TestGetStateKeyLevelDBPath(t *testing.T) {
	oldVal := viper.Get("peer.fileSystemPath")
	defer viper.Set("peer.fileSystemPath", oldVal)

	expected := "xyz123/ledgersData/stateKeyLeveldb"
	viper.Set("peer.fileSystemPath", "xyz123")
	val := GetStateKeyLevelDBPath()
	assert.Equal(t, expected, val)

}

func TestGetMissingPvtdataKeysStorePatht(t *testing.T) {
	oldVal := viper.Get("peer.fileSystemPath")
	defer viper.Set("peer.fileSystemPath", oldVal)

	expected := "xyz123/ledgersData/missingPvtdataKeysStore"
	viper.Set("peer.fileSystemPath", "xyz123")
	val := GetMissingPvtdataKeysStorePath()
	assert.Equal(t, expected, val)

}

func TestGetCouchDBMaxIdleConns(t *testing.T) {
	oldVal := viper.Get(confCouchDBMaxIdleConns)
	defer viper.Set(confCouchDBMaxIdleConns, oldVal)

	val := GetCouchDBMaxIdleConns()
	assert.Equal(t, val, 1000)

	viper.Set(confCouchDBMaxIdleConns, 2000)
	val = GetCouchDBMaxIdleConns()
	assert.Equal(t, val, 2000)

}

func TestGetCouchDBMaxIdleConnsPerHost(t *testing.T) {
	oldVal := viper.Get(confCouchDBMaxIdleConnsPerHost)
	defer viper.Set(confCouchDBMaxIdleConnsPerHost, oldVal)

	val := GetCouchDBMaxIdleConnsPerHost()
	assert.Equal(t, val, 100)

	viper.Set(confCouchDBMaxIdleConnsPerHost, 200)
	val = GetCouchDBMaxIdleConnsPerHost()
	assert.Equal(t, val, 200)

}

func TestGetCouchDBIdleConnTimeout(t *testing.T) {
	oldVal := viper.Get(confCouchDBIdleConnTimeout)
	defer viper.Set(confCouchDBIdleConnTimeout, oldVal)

	val := GetCouchDBIdleConnTimeout()
	assert.Equal(t, val, 90*time.Second)

	viper.Set(confCouchDBIdleConnTimeout, 99*time.Second)
	val = GetCouchDBIdleConnTimeout()
	assert.Equal(t, val, 99*time.Second)

}

func TestGetCouchDBKeepAliveTimeout(t *testing.T) {
	oldVal := viper.Get(confCouchDBKeepAliveTimeout)
	defer viper.Set(confCouchDBKeepAliveTimeout, oldVal)

	val := GetCouchDBKeepAliveTimeout()
	assert.Equal(t, val, 30*time.Second)

	viper.Set(confCouchDBKeepAliveTimeout, 33*time.Second)
	val = GetCouchDBKeepAliveTimeout()
	assert.Equal(t, val, 33*time.Second)

}

func TestGetBlockStoreProviderFilesystem(t *testing.T) {
	setUpCoreYAMLConfig()
	defer ledgertestutil.ResetConfigToDefaultValues()
	viper.Set("ledger.blockchain.blockStorage", "filesystem")
	provider := GetBlockStoreProvider()
	assert.Equal(t, provider, FilesystemLedgerStorage)
}

func TestGetBlockStoreProviderCouchDB(t *testing.T) {
	setUpCoreYAMLConfig()
	defer ledgertestutil.ResetConfigToDefaultValues()
	viper.Set("ledger.blockchain.blockStorage", "CouchDB")
	provider := GetBlockStoreProvider()
	assert.Equal(t, provider, CouchDBLedgerStorage)
}

func TestGetBlockCacheSize(t *testing.T) {
	oldVal := viper.Get(confBlockCacheSize)
	defer viper.Set(confBlockCacheSize, oldVal)

	val := GetBlockCacheSize()
	assert.Equal(t, val, 5)

	viper.Set(confBlockCacheSize, 9)
	val = GetBlockCacheSize()
	assert.Equal(t, val, 9)

}

func TestGetPvtDataCacheSize(t *testing.T) {
	oldVal := viper.Get(confPvtDataCacheSize)
	defer viper.Set(confPvtDataCacheSize, oldVal)

	val := GetPvtDataCacheSize()
	assert.Equal(t, val, 10)

	viper.Set(confPvtDataCacheSize, 99)
	val = GetPvtDataCacheSize()
	assert.Equal(t, val, 99)

}

func TestGetKVCacheSize(t *testing.T) {
	oldVal := viper.Get(confKVCacheSize)
	defer viper.Set(confKVCacheSize, oldVal)

	val := GetKVCacheSize()
	assert.Equal(t, val, 64*1024)

	viper.Set(confKVCacheSize, 128*1024)
	val = GetKVCacheSize()
	assert.Equal(t, val, 128*1024)

}

func TestGetKVCacheBlocksToLive(t *testing.T) {
	oldVal := viper.Get(confKVCacheBlocksToLive)
	defer viper.Set(confKVCacheBlocksToLive, oldVal)

	val := GetKVCacheBlocksToLive()
	assert.Equal(t, val, uint64(120))

	viper.Set(confKVCacheBlocksToLive, 60)
	val = GetKVCacheBlocksToLive()
	assert.Equal(t, val, uint64(60))

}

func TestGetKVCacheNonDurableSize(t *testing.T) {
	oldVal := viper.Get(confKVCacheNonDurableSize)
	defer viper.Set(confKVCacheNonDurableSize, oldVal)

	val := GetKVCacheNonDurableSize()
	assert.Equal(t, val, 64*1024)

	viper.Set(confKVCacheNonDurableSize, 128*1024)
	val = GetKVCacheNonDurableSize()
	assert.Equal(t, val, 128*1024)

}

func TestGetConfigHistoryStoreProvider(t *testing.T) {
	oldVal := viper.Get(confConfigHistoryStorage)
	defer viper.Set(confConfigHistoryStorage, oldVal)

	viper.Set(confConfigHistoryStorage, "CouchDB")
	val := GetConfigHistoryStoreProvider()
	assert.Equal(t, CouchDBConfigHistoryStorage, val)

	viper.Set(confConfigHistoryStorage, "goleveldb")
	val = GetConfigHistoryStoreProvider()
	assert.Equal(t, LevelDBConfigHistoryStorage, val)

	viper.Set(confConfigHistoryStorage, "INVALID")
	val = GetConfigHistoryStoreProvider()
	assert.Equal(t, LevelDBConfigHistoryStorage, val)

}

func TestGetTransientStoreProvider(t *testing.T) {
	oldVal := viper.Get(confTransientStorage)
	defer viper.Set(confTransientStorage, oldVal)

	viper.Set(confTransientStorage, "CouchDB")
	val := GetTransientStoreProvider()
	assert.Equal(t, CouchDBTransientStorage, val)

	viper.Set(confTransientStorage, "Memory")
	val = GetTransientStoreProvider()
	assert.Equal(t, MemoryTransientStorage, val)

	viper.Set(confTransientStorage, "goleveldb")
	val = GetTransientStoreProvider()
	assert.Equal(t, LevelDBTransientStorage, val)

	viper.Set(confTransientStorage, "INVALID")
	val = GetTransientStoreProvider()
	assert.Equal(t, LevelDBTransientStorage, val)

}

func TestGetHistoryStoreProvider(t *testing.T) {
	oldVal := viper.Get(confHistoryStorage)
	defer viper.Set(confHistoryStorage, oldVal)

	viper.Set(confHistoryStorage, "CouchDB")
	val := GetHistoryStoreProvider()
	assert.Equal(t, CouchDBHistoryStorage, val)

	viper.Set(confHistoryStorage, "goleveldb")
	val = GetHistoryStoreProvider()
	assert.Equal(t, LevelDBHistoryStorage, val)

	viper.Set(confHistoryStorage, "INVALID")
	val = GetHistoryStoreProvider()
	assert.Equal(t, LevelDBHistoryStorage, val)

}

func TestGetHistoryStoreProviderDefault(t *testing.T) {
	provider := GetHistoryStoreProvider()
	assert.Equal(t, provider, LevelDBHistoryStorage)
}

func TestGetHistoryStoreProviderLevelDB(t *testing.T) {
	setUpCoreYAMLConfig()
	defer ledgertestutil.ResetConfigToDefaultValues()
	viper.Set("ledger.state.historyStorage", "goleveldb")
	provider := GetHistoryStoreProvider()
	assert.Equal(t, provider, LevelDBHistoryStorage)
}

func TestGetHistoryStoreProviderCouchDB(t *testing.T) {
	setUpCoreYAMLConfig()
	defer ledgertestutil.ResetConfigToDefaultValues()
	viper.Set("ledger.state.historyStorage", "CouchDB")
	provider := GetHistoryStoreProvider()
	assert.Equal(t, provider, CouchDBHistoryStorage)
}

func TestCouchDBHTTPTraceEnabled(t *testing.T) {
	oldVal := viper.Get(confCouchDBHTTPTraceEnabled)
	defer viper.Set(confCouchDBHTTPTraceEnabled, oldVal)

	viper.Set(confCouchDBHTTPTraceEnabled, true)
	assert.True(t, CouchDBHTTPTraceEnabled())

	viper.Set(confCouchDBHTTPTraceEnabled, false)
	assert.False(t, CouchDBHTTPTraceEnabled())

}

func TestGetValidationMinWaitTime(t *testing.T) {
	oldVal := viper.Get(confValidationMinWaitTime)
	defer viper.Set(confValidationMinWaitTime, oldVal)

	viper.Set(confValidationMinWaitTime, 0)
	assert.Equal(t, defaultValidationMinWaitTime, GetValidationMinWaitTime())

	viper.Set(confValidationMinWaitTime, 10*time.Second)
	assert.Equal(t, 10*time.Second, GetValidationMinWaitTime())
}

func TestGetConcurrentBlockWrites(t *testing.T) {
	oldVal := viper.Get(confConcurrentBlockWrites)
	defer viper.Set(confConcurrentBlockWrites, oldVal)

	assert.Equal(t, 1, GetConcurrentBlockWrites())
	viper.Set(confConcurrentBlockWrites, 7)
	assert.Equal(t, 7, GetConcurrentBlockWrites())
}

func TestGetTransientDataCacheSize(t *testing.T) {
	oldVal := viper.Get(confTransientDataCacheSize)
	defer viper.Set(confTransientDataCacheSize, oldVal)

	os.Setenv("CORE_LEDGER_TRANSIENTDATA_CACHESIZE", "27")
	setUpCoreYAMLConfig()

	assert.Equal(t, 27, GetTransientDataCacheSize())

	viper.Set(confTransientDataCacheSize, 0)
	assert.Equal(t, defaultTransientDataCacheSize, GetTransientDataCacheSize())

	viper.Set(confTransientDataCacheSize, 10)
	assert.Equal(t, 10, GetTransientDataCacheSize())
}

func TestGetTransientDataPullTimeout(t *testing.T) {
	oldVal := viper.Get(confTransientDataPullTimeout)
	defer viper.Set(confTransientDataPullTimeout, oldVal)

	os.Setenv("CORE_PEER_GOSSIP_TRANSIENTDATA_PULLTIMEOUT", "11s")
	setUpCoreYAMLConfig()

	assert.Equal(t, 11*time.Second, GetTransientDataPullTimeout())

	viper.Set(confTransientDataPullTimeout, "")
	assert.Equal(t, defaultTransientDataPullTimeout, GetTransientDataPullTimeout())

	viper.Set(confTransientDataPullTimeout, 111*time.Second)
	assert.Equal(t, 111*time.Second, GetTransientDataPullTimeout())
}

func TestGetTransientDataExpiredIntervalTime(t *testing.T) {
	oldVal := viper.Get(confTransientDataCleanupIntervalTime)
	defer viper.Set(confTransientDataCleanupIntervalTime, oldVal)

	os.Setenv("CORE_LEDGER_TRANSIENTDATA_CLEANUPEXPIRED_INTERVAL", "11s")
	setUpCoreYAMLConfig()

	assert.Equal(t, 11*time.Second, GetTransientDataExpiredIntervalTime())

	viper.Set(confTransientDataCleanupIntervalTime, "")
	assert.Equal(t, defaultTransientDataCleanupIntervalTime, GetTransientDataExpiredIntervalTime())

	viper.Set(confTransientDataCleanupIntervalTime, 111*time.Second)
	assert.Equal(t, 111*time.Second, GetTransientDataExpiredIntervalTime())
}

func TestGetDCASExpiredIntervalTime(t *testing.T) {
	oldVal := viper.Get(confOLCollCleanupIntervalTime)
	defer viper.Set(confOLCollCleanupIntervalTime, oldVal)

	os.Setenv("CORE_OFFLEDGER_CLEANUPEXPIRED_INTERVAL", "11s")
	setUpCoreYAMLConfig()

	assert.Equal(t, 11*time.Second, GetOLCollExpirationCheckInterval())

	viper.Set(confOLCollCleanupIntervalTime, "")
	assert.Equal(t, defaultOLCollCleanupIntervalTime, GetOLCollExpirationCheckInterval())

	viper.Set(confOLCollCleanupIntervalTime, 111*time.Second)
	assert.Equal(t, 111*time.Second, GetOLCollExpirationCheckInterval())
}

func TestGetDCASLevelDBPath(t *testing.T) {
	oldVal := viper.Get("peer.fileSystemPath")
	defer viper.Set("peer.fileSystemPath", oldVal)

	viper.Set("peer.fileSystemPath", "/tmp123")
	setUpCoreYAMLConfig()

	assert.Equal(t, "/tmp123/ledgersData/offLedgerLeveldb", GetOLCollLevelDBPath())
}

func TestGetDCASPullTimeout(t *testing.T) {
	oldVal := viper.Get(confOLCollPullTimeout)
	defer viper.Set(confOLCollPullTimeout, oldVal)

	os.Setenv("CORE_OFFLEDGER_GOSSIP_PULLTIMEOUT", "11s")
	setUpCoreYAMLConfig()

	assert.Equal(t, 11*time.Second, GetOLCollPullTimeout())

	viper.Set(confOLCollPullTimeout, "")
	assert.Equal(t, defaultOLCollPullTimeout, GetOLCollPullTimeout())

	viper.Set(confOLCollPullTimeout, 111*time.Second)
	assert.Equal(t, 111*time.Second, GetOLCollPullTimeout())
}

func TestGetDCASMaxPeersForRetrieval(t *testing.T) {
	oldVal := viper.Get(confOLCollMaxPeersForRetrieval)
	defer viper.Set(confOLCollMaxPeersForRetrieval, oldVal)

	os.Setenv("CORE_OFFLEDGER_MAXPEERS", "3")
	setUpCoreYAMLConfig()

	assert.Equal(t, 3, GetOLCollMaxPeersForRetrieval())

	viper.Set(confOLCollMaxPeersForRetrieval, "")
	assert.Equal(t, defaultOLCollMaxPeersForRetrieval, GetOLCollMaxPeersForRetrieval())

	viper.Set(confOLCollMaxPeersForRetrieval, 7)
	assert.Equal(t, 7, GetOLCollMaxPeersForRetrieval())
}

func TestGetDCASCacheSize(t *testing.T) {
	oldVal := viper.Get(confOLCollCacheSize)
	defer viper.Set(confOLCollCacheSize, oldVal)

	os.Setenv("CORE_OFFLEDGER_CACHESIZE", "27")
	setUpCoreYAMLConfig()

	assert.Equal(t, 27, GetOLCollCacheSize())

	viper.Set(confOLCollCacheSize, 0)
	assert.Equal(t, defaultOLCollCacheSize, GetOLCollCacheSize())

	viper.Set(confOLCollCacheSize, 10)
	assert.Equal(t, 10, GetOLCollCacheSize())
}

func TestGetRestServerPort(t *testing.T) {
	oldVal := viper.Get(confPeerRestServerPort)
	defer viper.Set(confPeerRestServerPort, oldVal)

	viper.Set(confPeerRestServerPort, 443)
	val := GetRestServerPort()
	assert.Equal(t, defaultRestServerPort, val)

	viper.Set(confPeerRestServerPort, 3000)
	val = GetRestServerPort()
	assert.Equal(t, val, 3000)
}

func TestGetIdentityHubEnabled(t *testing.T) {
	oldVal := viper.Get(confPeerIdentityHubEnabled)
	defer viper.Set(confPeerIdentityHubEnabled, oldVal)

	viper.Set(confPeerIdentityHubEnabled, false)
	val := GetIdentityHubEnabled()
	assert.Equal(t, defaultIdentityHubEnabled, val)

	viper.Set(confPeerIdentityHubEnabled, true)
	val = GetIdentityHubEnabled()
	assert.True(t, val)
}

func TestGetRoles(t *testing.T) {
	oldVal := viper.Get(confRoles)
	defer viper.Set(confRoles, oldVal)

	roles := "endorser,observer"
	viper.Set(confRoles, roles)
	assert.Equal(t, roles, GetRoles())
}

func TestGetObservingChannels(t *testing.T) {
	oldVal := viper.Get(confObserverChannels)
	defer viper.Set(confObserverChannels, oldVal)

	channels := "ch1,ch2"
	viper.Set(confObserverChannels, channels)
	assert.Equal(t, channels, GetObserverChannels())
}

func setUpCoreYAMLConfig() {
	//call a helper method to load the core.yaml
	ledgertestutil.SetupCoreYAMLConfig()
}
