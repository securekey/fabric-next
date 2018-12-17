/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbblkstorage

import (
	"context"
	"fmt"
	"github.com/hyperledger/fabric/common/ledger/blkstorage/mocks"
	"testing"
	"time"

	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/hyperledger/fabric/integration/runner"
	"github.com/stretchr/testify/assert"
)

func TestRestart(t *testing.T) {
	def, cleanup := startCouchDB()
	defer cleanup()

	testRestart(t, def, "ledgera", true)
	testRestart(t, def, "ledgerb", false)
}

func testRestart(t *testing.T, def *couchdb.CouchDBDef, ledgerID string, blockIndexEnabled bool) {
	provider, err := newProvider(def, blockIndexEnabled)
	assert.NoError(t, err)

	s, err := provider.OpenBlockStore(ledgerID)
	assert.NoError(t, err)
	store := s.(*cdbBlockStore)

	bi, err := store.GetBlockchainInfo()
	assert.NoError(t, err)
	assert.Equal(t, bi.Height, uint64(0))

	addBlock(store, 0)
	bi, err = store.GetBlockchainInfo()
	assert.NoError(t, err)
	assert.Equal(t, bi.Height, uint64(1))

	addBlock(store, 1)
	bi, err = store.GetBlockchainInfo()
	assert.NoError(t, err)
	assert.Equal(t, bi.Height, uint64(2))

	store.Shutdown()

	for i := 0; i < 20; i++ {
		s, err = provider.OpenBlockStore(ledgerID)
		assert.NoError(t, err)
		store = s.(*cdbBlockStore)

		bi, err = store.GetBlockchainInfo()
		assert.NoError(t, err)
		assert.Equal(t, bi.Height, uint64(i+2))

		addBlock(store, uint64(i+2))
		store.Shutdown()
		time.Sleep(10 * time.Millisecond)
	}
}

func addBlock(store *cdbBlockStore, number uint64) {
	b := mocks.CreateSimpleMockBlock(number)
	store.AddBlock(b)
	store.CheckpointBlock(b)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	store.WaitForBlock(ctx, number)
	cancel()
}

// Start a CouchDB test instance.
// Use the cleanup function to stop it.
func startCouchDB() (couchDbDef *couchdb.CouchDBDef, cleanup func()) {
	couchDB := &runner.CouchDB{}
	if err := couchDB.Start(); err != nil {
		err := fmt.Errorf("failed to start couchDB: %s", err)
		panic(err)
	}
	return &couchdb.CouchDBDef{
		URL:                 couchDB.Address(),
		MaxRetries:          3,
		Password:            "",
		Username:            "",
		MaxRetriesOnStartup: 3,
		RequestTimeout:      35 * time.Second,
	}, func() { couchDB.Stop() }
}
