/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package storeprovider

import (
	"sync"

	olapi "github.com/hyperledger/fabric/extensions/collections/api/offledger"
	storeapi "github.com/hyperledger/fabric/extensions/collections/api/store"
	tdataapi "github.com/hyperledger/fabric/extensions/collections/api/transientdata"
	"github.com/hyperledger/fabric/extensions/collections/offledger/dcas"
	offledgerstoreprovider "github.com/hyperledger/fabric/extensions/collections/offledger/storeprovider"
	"github.com/hyperledger/fabric/extensions/collections/transientdata/storeprovider"
	cb "github.com/hyperledger/fabric/protos/common"
)

// NewProviderFactory returns a new store provider factory
func NewProviderFactory() *StoreProvider {
	return &StoreProvider{
		transientDataProvider: newTransientDataProviderFactory(),
		olProvider:            newOffLedgerProviderFactory(),
		stores:                make(map[string]*store),
	}
}

// StoreProvider is a store provider that creates delegating stores.
// A delegating store delegates requests to collection-specific store.
// For example, transient data store, Off-ledger store, etc.
type StoreProvider struct {
	transientDataProvider tdataapi.StoreProvider
	olProvider            olapi.StoreProvider
	stores                map[string]*store
	sync.RWMutex
}

// StoreForChannel returns the store for the given channel
func (sp *StoreProvider) StoreForChannel(channelID string) storeapi.Store {
	sp.RLock()
	defer sp.RUnlock()
	return sp.stores[channelID]
}

// OpenStore opens the store for the given channel
func (sp *StoreProvider) OpenStore(channelID string) (storeapi.Store, error) {
	sp.Lock()
	defer sp.Unlock()

	store, ok := sp.stores[channelID]
	if !ok {
		tdataStore, err := sp.transientDataProvider.OpenStore(channelID)
		if err != nil {
			return nil, err
		}
		olStore, err := sp.olProvider.OpenStore(channelID)
		if err != nil {
			return nil, err
		}
		store = newDelegatingStore(channelID, tdataStore, olStore)
		sp.stores[channelID] = store
	}
	return store, nil
}

// Close shuts down all of the stores
func (sp *StoreProvider) Close() {
	for _, s := range sp.stores {
		s.Close()
	}
}

// newTransientDataProviderFactory may be overridden in unit tests
var newTransientDataProviderFactory = func() tdataapi.StoreProvider {
	return storeprovider.NewProviderFactory()
}

// newOffLedgerProviderFactory may be overridden in unit tests
var newOffLedgerProviderFactory = func() olapi.StoreProvider {
	return offledgerstoreprovider.NewProviderFactory(
		offledgerstoreprovider.WithCollectionType(
			cb.CollectionType_COL_DCAS, offledgerstoreprovider.WithDecorator(dcas.Decorator),
		),
	)
}
