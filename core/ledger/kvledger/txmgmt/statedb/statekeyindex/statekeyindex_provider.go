/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package statekeyindex

import (
	"sync"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
)

var logger = flogging.MustGetLogger("statekeyindex")
var instance *LevelStateKeyIndexProvider
var lock sync.Mutex

// MemBlockCacheProvider provides block cache in memory
type LevelStateKeyIndexProvider struct {
	leveldbProvider *leveldbhelper.Provider
}

// NewProvider constructs a filesystem based block store provider
func NewProvider() *LevelStateKeyIndexProvider {
	if instance != nil {
		return instance
	}
	lock.Lock()
	if instance != nil {
		lock.Unlock()
		return instance
	}
	dbPath := ledgerconfig.GetStateLevelDBPath()
	logger.Debugf("constructing LevelStateKeyIndexProvider dbPath=%s", dbPath)
	instance = &LevelStateKeyIndexProvider{leveldbhelper.NewProvider(&leveldbhelper.Conf{DBPath: dbPath})}
	lock.Unlock()
	return instance
}

// OpenStateKeyIndex opens the block cache for the given dbname id
func (p *LevelStateKeyIndexProvider) OpenStateKeyIndex(dbName string) (statedb.StateKeyIndex, error) {
	indexStore := p.leveldbProvider.GetDBHandle(dbName)
	return newStateKeyIndex(indexStore, dbName), nil
}

// Close cleans up the Provider
func (p *LevelStateKeyIndexProvider) Close() {
	lock.Lock()
	p.leveldbProvider.Close()
	instance = nil
	lock.Unlock()
}
