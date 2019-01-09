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
	dbProvider            statedb.VersionedDBProvider
}

// NewProvider creates a new StateStoreProvider that combines a cache (+ index) provider and a backing storage provider
func NewProvider(dbProvider statedb.VersionedDBProvider) *CachedStateProvider {
	p := CachedStateProvider{
		dbProvider:            dbProvider,
	}
	return &p
}

// GetDBHandle gets the handle to a named database
func (provider *CachedStateProvider) GetDBHandle(dbName string) (*cachedStateStore , error) {

	dbProvider, err := provider.dbProvider.GetDBHandle(dbName)
	if err != nil {
		return nil, errors.Wrap(err, "dbProvider GetDBHandle failed")
	}

	return newCachedBlockStore(dbProvider, dbName), nil
}

// Close cleans up the Provider
func (provider *CachedStateProvider) Close() {
	provider.dbProvider.Close()
}
