/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package retriever

import (
	"context"
	"sync"

	"github.com/hyperledger/fabric/core/common/privdata"
	"github.com/hyperledger/fabric/core/ledger"
	olapi "github.com/hyperledger/fabric/extensions/collections/api/offledger"
	storeapi "github.com/hyperledger/fabric/extensions/collections/api/store"
	supportapi "github.com/hyperledger/fabric/extensions/collections/api/support"
	tdataapi "github.com/hyperledger/fabric/extensions/collections/api/transientdata"
	"github.com/hyperledger/fabric/extensions/collections/offledger"
	"github.com/hyperledger/fabric/extensions/collections/offledger/dcas"
	"github.com/hyperledger/fabric/extensions/collections/transientdata"
	supp "github.com/hyperledger/fabric/extensions/common/support"
	gossipapi "github.com/hyperledger/fabric/extensions/gossip/api"
	cb "github.com/hyperledger/fabric/protos/common"
)

// Provider is a transient data provider.
type Provider struct {
	transientDataProvider tdataapi.Provider
	offLedgerProvider     olapi.Provider
	retrievers            map[string]*retriever
	mutex                 sync.RWMutex
}

// NewProvider returns a new transient data provider
func NewProvider(
	storeProvider func(channelID string) storeapi.Store,
	ledgerProvider func(channelID string) ledger.PeerLedger,
	gossipProvider func() supportapi.GossipAdapter,
	blockPublisherProvider func(channelID string) gossipapi.BlockPublisher) storeapi.Provider {

	support := supp.New(ledgerProvider, blockPublisherProvider)

	tdataStoreProvider := func(channelID string) tdataapi.Store { return storeProvider(channelID) }
	offLedgerStoreProvider := func(channelID string) olapi.Store { return storeProvider(channelID) }

	return &Provider{
		transientDataProvider: getTransientDataProvider(tdataStoreProvider, support, gossipProvider),
		offLedgerProvider:     getOffLedgerProvider(offLedgerStoreProvider, support, gossipProvider),
		retrievers:            make(map[string]*retriever),
	}
}

// RetrieverForChannel returns the collection retriever for the given channel
func (p *Provider) RetrieverForChannel(channelID string) storeapi.Retriever {
	p.mutex.RLock()
	r, ok := p.retrievers[channelID]
	p.mutex.RUnlock()

	if !ok {
		p.mutex.Lock()
		defer p.mutex.Unlock()

		r, ok = p.retrievers[channelID]
		if !ok {
			r = &retriever{
				transientDataRetriever: p.transientDataProvider.RetrieverForChannel(channelID),
				dcasRetriever:          p.offLedgerProvider.RetrieverForChannel(channelID),
			}
			p.retrievers[channelID] = r
		}
	}

	return r
}

type retriever struct {
	transientDataRetriever tdataapi.Retriever
	dcasRetriever          olapi.Retriever
}

func (r *retriever) GetTransientData(ctxt context.Context, key *storeapi.Key) (*storeapi.ExpiringValue, error) {
	return r.transientDataRetriever.GetTransientData(ctxt, key)
}

// GetTransientDataMultipleKeys gets the values for the multiple transient data items in a single call
func (r *retriever) GetTransientDataMultipleKeys(ctxt context.Context, key *storeapi.MultiKey) (storeapi.ExpiringValues, error) {
	return r.transientDataRetriever.GetTransientDataMultipleKeys(ctxt, key)
}

// GetData gets the value for the given data item
func (r *retriever) GetData(ctxt context.Context, key *storeapi.Key) (*storeapi.ExpiringValue, error) {
	return r.dcasRetriever.GetData(ctxt, key)
}

// GetDataMultipleKeys gets the values for the multiple data items in a single call
func (r *retriever) GetDataMultipleKeys(ctxt context.Context, key *storeapi.MultiKey) (storeapi.ExpiringValues, error) {
	return r.dcasRetriever.GetDataMultipleKeys(ctxt, key)
}

type support interface {
	Config(channelID, ns, coll string) (*cb.StaticCollectionConfig, error)
	Policy(channel, ns, collection string) (privdata.CollectionAccessPolicy, error)
	BlockPublisher(channelID string) gossipapi.BlockPublisher
}

var getTransientDataProvider = func(storeProvider func(channelID string) tdataapi.Store, support support, gossipProvider func() supportapi.GossipAdapter) tdataapi.Provider {
	return transientdata.NewProvider(storeProvider, support, gossipProvider)
}

var getOffLedgerProvider = func(storeProvider func(channelID string) olapi.Store, support support, gossipProvider func() supportapi.GossipAdapter) olapi.Provider {
	return offledger.NewProvider(storeProvider, support, gossipProvider,
		offledger.WithValidator(cb.CollectionType_COL_DCAS, dcas.Validator),
	)
}
