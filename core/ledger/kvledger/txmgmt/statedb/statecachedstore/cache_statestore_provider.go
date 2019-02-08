/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package statecachedstore

import (
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb/statekeyindex"
	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("statecache")

type CachedStateProvider struct {
	dbProvider            statedb.VersionedDBProvider
	stateKeyIndexProvider statekeyindex.StateKeyIndexProvider
}

// NewProvider creates a new StateStoreProvider that combines a cache (+ index) provider and a backing storage provider
func NewProvider(dbProvider statedb.VersionedDBProvider, stateKeyIndexProvider statekeyindex.StateKeyIndexProvider) *CachedStateProvider {
	p := CachedStateProvider{
		dbProvider:            dbProvider,
		stateKeyIndexProvider: stateKeyIndexProvider,
	}
	return &p
}

// GetDBHandle gets the handle to a named database
func (provider *CachedStateProvider) GetDBHandle(dbName string) (statedb.VersionedDB , error) {

	vdb, err := provider.dbProvider.GetDBHandle(dbName)
	if err != nil {
		return nil, errors.Wrap(err, "dbProvider GetDBHandle failed")
	}

	stateIdx, err := statekeyindex.NewProvider().OpenStateKeyIndex(dbName)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open the stateindex for db %s", dbName)
	}

	return newCachedStateStore(vdb, stateIdx, dbName), nil
}

// Close cleans up the Provider
func (provider *CachedStateProvider) Close() {
	provider.dbProvider.Close()
	provider.stateKeyIndexProvider.Close()
}
