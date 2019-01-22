/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package memtransientdata

import (
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/transientstore"
)

var logger = flogging.MustGetLogger("transientstore.mem")

type Provider struct {
}

// NewProvider instantiates a transient data storage provider backed by Memory
func NewProvider() *Provider {
	logger.Debugf("constructing CouchDB mem data storage provider")

	return &Provider{}
}

// OpenStore creates a handle to the transient data store for the given ledger ID
func (p *Provider) OpenStore(ledgerid string) (transientstore.Store, error) {
	return newStore()
}

// Close cleans up the provider
func (p *Provider) Close() {
}
