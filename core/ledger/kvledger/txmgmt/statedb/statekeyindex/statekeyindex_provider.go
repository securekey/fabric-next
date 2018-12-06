/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package statekeyindex

import (
	"sync"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
)

var logger = flogging.MustGetLogger("statekeyindex")
var instance *LevelStateKeyIndexProvider
var lock sync.Mutex

// StateIndexProvider provides an handle to a StateIndex
type StateKeyIndexProvider interface {
	OpenStateKeyIndex(id string) (StateKeyIndex, error)
	Close()
}

// StateKeyIndex - an interface for persisting and retrieving keys
type StateKeyIndex interface {
	AddIndex(indexUpdates []*IndexUpdate) error
	DeleteIndex(keys []CompositeKey) error
	GetIterator(namespace string, startKey string, endKey string) *leveldbhelper.Iterator
	// Returns a previously indexed Metadata if found.
	GetMetadata(key *CompositeKey) (Metadata, bool, error)
	Close()
}

// TODO remove this CompositeKey and reuse statedb.CompositeKey instead.
// CompositeKey encloses Namespace and Key components
type CompositeKey struct {
	Namespace string
	Key       string
}

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
	dbPath := ledgerconfig.GetStateKeyLevelDBPath()
	logger.Debugf("constructing LevelStateKeyIndexProvider dbPath=%s", dbPath)
	instance = &LevelStateKeyIndexProvider{leveldbhelper.NewProvider(&leveldbhelper.Conf{DBPath: dbPath})}
	lock.Unlock()
	return instance
}

// OpenStateKeyIndex opens the block cache for the given dbname id
func (p *LevelStateKeyIndexProvider) OpenStateKeyIndex(dbName string) (StateKeyIndex, error) {
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

// IndexReady implementations provide a channel through which they can be notified to resume work
// after updates have been applied to it.
// Eg.: the statecouchdb package uses the statekeyindex to store the new revisions of the CouchDB documents
// it is saving or updating, but it requires the key's metadata to have been already inserted by the kvLedger
// in a previous phase.
type IndexReady interface {
	// Channel through which notifications can be sent.
	IndexReadyChan() chan struct{}
}
