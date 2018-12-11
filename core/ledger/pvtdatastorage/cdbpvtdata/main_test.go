/*
Copyright IBM Corp, SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbpvtdata

import (
	"fmt"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/ledger/pvtdatastorage"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/hyperledger/fabric/integration/runner"
	"github.com/spf13/viper"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hyperledger/fabric/core/ledger/pvtdatapolicy"

	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/stretchr/testify/assert"
)

var mainCouchDBDef *couchdb.CouchDBDef

func TestMain(m *testing.M) {
	flogging.SetModuleLevel("pvtdatastorage", "debug")
	viper.Set("peer.fileSystemPath", "/tmp/fabric/core/ledger/pvtdatastorage")
	viper.Set("ledger.pvtdataStore.purgeInterval", 2)
	def, cleanup := startCouchDB()
	defer cleanup()

	mainCouchDBDef = def
	os.Exit(m.Run())
}

// Start a CouchDB test instance.
// Use the cleanup function to stop it.
func startCouchDB() (couchDbDef *couchdb.CouchDBDef, cleanup func()) {
	couchDB := &runner.CouchDB{}
	if err := couchDB.Start(); err != nil {
		err := fmt.Errorf("failed to start couchDB: %s", err)
		panic(err)
	}
	// FIXME: the CouchDB container is not being stopped.
	return &couchdb.CouchDBDef{
		URL:                 couchDB.Address(),
		MaxRetries:          3,
		Password:            "",
		Username:            "",
		MaxRetriesOnStartup: 3,
		RequestTimeout:      35 * time.Second,
	}, func() { couchDB.Stop() }
}

// StoreEnv provides the  store env for testing
type StoreEnv struct {
	t                 testing.TB
	TestStoreProvider pvtdatastorage.Provider
	TestStore         pvtdatastorage.Store
	ledgerid          string
	btlPolicy         pvtdatapolicy.BTLPolicy
}

// NewTestStoreEnv construct a StoreEnv for testing
func NewTestStoreEnv(t *testing.T, ledgerid string, btlPolicy pvtdatapolicy.BTLPolicy) *StoreEnv {
	removeStorePath(t)
	assert := assert.New(t)
	testStoreProvider, err := newProviderWithDBDef(mainCouchDBDef)
	assert.NoError(err, "Provider should be created successfully")
	lid := strings.ToLower(ledgerid)
	testStore, err := testStoreProvider.OpenStore(lid)
	testStore.Init(btlPolicy)
	assert.NoError(err)
	return &StoreEnv{t, testStoreProvider, testStore, lid, btlPolicy}
}

// CloseAndReopen closes and opens the store provider
func (env *StoreEnv) CloseAndReopen() {
	var err error
	env.TestStoreProvider.Close()
	testStoreProvider, err := newProviderWithDBDef(mainCouchDBDef)
	assert.NoError(env.t, err, "Provider should be created successfully")
	env.TestStoreProvider = testStoreProvider
	env.TestStore, err = env.TestStoreProvider.OpenStore(env.ledgerid)
	env.TestStore.Init(env.btlPolicy)
	assert.NoError(env.t, err)
}

// Cleanup cleansup the  store env after testing
func (env *StoreEnv) Cleanup() {
	env.TestStoreProvider.Close()
	removeStorePath(env.t)
}

func removeStorePath(t testing.TB) {
	dbPath := ledgerconfig.GetPvtdataStorePath()
	if err := os.RemoveAll(dbPath); err != nil {
		t.Fatalf("Err: %s", err)
		t.FailNow()
	}
}
