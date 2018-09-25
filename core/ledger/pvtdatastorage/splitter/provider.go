/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package splitter

import (
	"github.com/hyperledger/fabric/core/ledger/pvtdatastorage"
	"github.com/hyperledger/fabric/core/ledger/pvtdatastorage/cdbpvtdata"
)

type Provider struct {
	pa pvtdatastorage.Provider
	pb pvtdatastorage.Provider
}

// NewProvider instantiates a private data storage provider backed by CouchDB
func NewProvider() *Provider {
	pa := pvtdatastorage.NewProvider()
	pb := cdbpvtdata.NewProvider()

	return &Provider{pa, pb}
}


// OpenStore creates a handle to the private data store for the given ledger ID
func (p *Provider) OpenStore(ledgerid string) (pvtdatastorage.Store, error) {
	sa, err := p.pa.OpenStore(ledgerid)
	if err != nil {
		return nil, err
	}
	sb, err := p.pa.OpenStore(ledgerid)
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
