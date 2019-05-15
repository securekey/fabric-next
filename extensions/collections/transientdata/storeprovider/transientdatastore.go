/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package storeprovider

import (
	"sync"

	"github.com/hyperledger/fabric/extensions/collections/api/transientdata"
	"github.com/hyperledger/fabric/extensions/collections/transientdata/storeprovider/store"
)

// NewProviderFactory returns a new transient data store provider factory
func NewProviderFactory() *StoreProvider {
	return &StoreProvider{
		stores: make(map[string]transientdata.Store),
	}
}

// StoreProvider is a transient data store provider
type StoreProvider struct {
	stores map[string]transientdata.Store
	transientdata.StoreProvider
	sync.RWMutex
}

// StoreForChannel returns the transient data store for the given channel
func (sp *StoreProvider) StoreForChannel(channelID string) transientdata.Store {
	sp.RLock()
	defer sp.RUnlock()
	return sp.stores[channelID]
}

// OpenStore opens the transient data store for the given channel
func (sp *StoreProvider) OpenStore(channelID string) (transientdata.Store, error) {
	sp.Lock()
	defer sp.Unlock()
	if sp.StoreProvider == nil {
		sp.StoreProvider = store.NewProvider()
	}
	store, err := sp.StoreProvider.OpenStore(channelID)
	if err == nil {
		sp.stores[channelID] = store
	}
	return store, err
}

// Close shuts down all of the stores
func (sp *StoreProvider) Close() {
	for _, s := range sp.stores {
		s.Close()
	}
}
