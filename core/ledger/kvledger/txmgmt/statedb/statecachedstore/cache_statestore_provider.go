/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package statecachedstore

import (
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("statecache")

type CachedStateProvider struct {
	dbProvider    statedb.VersionedDBProvider
	indexProvider statedb.StateKeyIndexProvider
}

// NewProvider creates a new StateStoreProvider that combines a cache (+ index) provider and a backing storage provider
func NewProvider(dbProvider statedb.VersionedDBProvider, indexProvider statedb.StateKeyIndexProvider) *CachedStateProvider {
	p := CachedStateProvider{
		dbProvider:    dbProvider,
		indexProvider: indexProvider,
	}
	return &p
}

// GetDBHandle gets the handle to a named database
func (provider *CachedStateProvider) GetDBHandle(dbName string) (statedb.VersionedDB, error) {

	dbProvider, err := provider.dbProvider.GetDBHandle(dbName)
	if err != nil {
		return nil, errors.Wrap(err, "dbProvider GetDBHandle failed")
	}

	stateKeyIndex, err := provider.indexProvider.OpenStateKeyIndex(dbName)
	if err != nil {
		return nil, errors.Wrap(err, "indexProvider OpenStateKeyIndex failed")
	}

	return newCachedBlockStore(dbProvider, stateKeyIndex), nil
}

// Close cleans up the Provider
func (provider *CachedStateProvider) Close() {
	provider.dbProvider.Close()
}
