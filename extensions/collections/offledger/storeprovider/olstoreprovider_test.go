/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package storeprovider

import (
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/integration/runner"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore(t *testing.T) {
	channel1 := "channel1"
	channel2 := "channel2"

	f := NewProviderFactory()
	require.NotNil(t, f)

	s1, err := f.OpenStore(channel1)
	require.NotNil(t, s1)
	assert.NoError(t, err)
	assert.Equal(t, "*storeprovider.store", reflect.TypeOf(s1).String())
	assert.Equal(t, s1, f.StoreForChannel(channel1))

	s2, err := f.OpenStore(channel2)
	require.NotNil(t, s2)
	assert.NoError(t, err)
	assert.NotEqual(t, s1, s2)
	assert.Equal(t, s2, f.StoreForChannel(channel2))

	assert.NotPanics(t, func() {
		f.Close()
	})
}

func TestMain(m *testing.M) {
	viper.Set("offledger.cleanupExpired.Interval", "100ms")

	// Switch to CouchDB
	couchAddress, cleanup := couchDBSetup()
	defer cleanup()

	// CouchDB configuration
	viper.Set("ledger.state.couchDBConfig.couchDBAddress", couchAddress)
	viper.Set("ledger.state.couchDBConfig.username", "")
	viper.Set("ledger.state.couchDBConfig.password", "")
	viper.Set("ledger.state.couchDBConfig.maxRetries", 3)
	viper.Set("ledger.state.couchDBConfig.maxRetriesOnStartup", 20)
	viper.Set("ledger.state.couchDBConfig.requestTimeout", time.Second*35)
	viper.Set("ledger.state.couchDBConfig.createGlobalChangesDB", true)

	//set the logging level to DEBUG to test debug only code
	flogging.ActivateSpec("couchdb=debug")
	os.Exit(m.Run())
}

func removePath(t testing.TB, path string) {
	if err := os.RemoveAll(path); err != nil {
		t.Fatalf("Err: %s", err)
	}
}

func couchDBSetup() (addr string, cleanup func()) {
	externalCouch, set := os.LookupEnv("COUCHDB_ADDR")
	if set {
		return externalCouch, func() {}
	}

	couchDB := &runner.CouchDB{}
	if err := couchDB.Start(); err != nil {
		err := fmt.Errorf("Failed to start couchDB: %s", err)
		panic(err)
	}
	return couchDB.Address(), func() {
		if err := couchDB.Stop(); err != nil {
			panic(err.Error())
		}
	}
}
