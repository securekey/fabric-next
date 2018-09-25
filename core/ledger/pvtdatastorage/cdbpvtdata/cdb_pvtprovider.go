/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbpvtdata

import "github.com/hyperledger/fabric/core/ledger/pvtdatastorage"

type Provider struct {
}

// NewProvider instantiates a private data storage provider backed by CouchDB
func NewProvider() *Provider {
	return &Provider{}
}


// OpenStore creates a handle to the private data store for the given ledger ID
func (p *Provider) OpenStore(ledgerid string) (pvtdatastorage.Store, error) {
	return &store{}, nil
}

// Close cleans up the provider
func (p *Provider) Close() {
}
