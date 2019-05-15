/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package storeprovider

import (
	"os"
	"reflect"
	"testing"

	"github.com/hyperledger/fabric/extensions/config"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransientDataStore(t *testing.T) {
	defer removeDBPath(t)
	channel1 := "channel1"
	channel2 := "channel2"

	f := NewProviderFactory()
	require.NotNil(t, f)
	removeDBPath(t)

	s1, err := f.OpenStore(channel1)
	require.NotNil(t, s1)
	assert.NoError(t, err)
	assert.Equal(t, "*store.store", reflect.TypeOf(s1).String())
	assert.Equal(t, s1, f.StoreForChannel(channel1))
	removeDBPath(t)

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
	removeDBPath(nil)
	viper.Set("peer.fileSystemPath", "/tmp/fabric/ledgertests/transientdatadb")
	os.Exit(m.Run())
}

func removeDBPath(t testing.TB) {
	removePath(t, config.GetTransientDataLevelDBPath())
}

func removePath(t testing.TB, path string) {
	if err := os.RemoveAll(path); err != nil {
		t.Fatalf("Err: %s", err)
		t.FailNow()
	}
}
