/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package splitter

import (
	"github.com/hyperledger/fabric/core/transientstore"
	"github.com/hyperledger/fabric/core/transientstore/cdbtransientdata"
)

type Provider struct {
	pa transientstore.StoreProvider
	pb transientstore.StoreProvider
}

// NewProvider instantiates a transient data storage provider backed by CouchDB
func NewProvider() (*Provider, error) {
	pa := transientstore.NewStoreProvider()
	pb, err := cdbtransientdata.NewProvider()
	if err != nil {
		return nil, err
	}

	provider := Provider{pa, pb}
	return &provider, nil
}

// OpenStore creates a handle to the transient data store for the given ledger ID
func (p *Provider) OpenStore(ledgerid string) (transientstore.Store, error) {
	sa, err := p.pa.OpenStore(ledgerid)
	if err != nil {
		return nil, err
	}
	sb, err := p.pb.OpenStore(ledgerid)
	if err != nil {
		return nil, err
	}

	return &store{sa, sb}, nil
}

// Close cleans up the provider
func (p *Provider) Close() {
	p.pa.Close()
	p.pb.Close()
}
