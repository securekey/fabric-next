/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package splitter

import (
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/common/ledger/blkstorage/cdbblkstorage"
	"github.com/hyperledger/fabric/common/ledger/blkstorage/fsblkstorage"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
)

var logger = flogging.MustGetLogger("peer")

// SplitterBlockstoreProvider provides block storage in CouchDB
type SplitterBlockstoreProvider struct {
	bspa blkstorage.BlockStoreProvider
	bspb blkstorage.BlockStoreProvider
}

// NewProvider creates a new CouchDB BlockStoreProvider
func NewProvider(indexConfig *blkstorage.IndexConfig) (blkstorage.BlockStoreProvider, error) {
	bspa := fsblkstorage.NewProvider(fsblkstorage.NewConf(ledgerconfig.GetBlockStorePath(), ledgerconfig.GetMaxBlockfileSize()), indexConfig)
	bspb, err := cdbblkstorage.NewProvider(indexConfig)
	if err != nil {
		return nil, err
	}

	return &SplitterBlockstoreProvider{bspa, bspb}, nil
}

// CreateBlockStore creates a block store instance for the given ledger ID
func (p *SplitterBlockstoreProvider) CreateBlockStore(ledgerid string) (blkstorage.BlockStore, error) {
	return p.OpenBlockStore(ledgerid)
}

// OpenBlockStore opens the block store for the given ledger ID
func (p *SplitterBlockstoreProvider) OpenBlockStore(ledgerid string) (blkstorage.BlockStore, error) {
	bsa, err := p.bspa.OpenBlockStore(ledgerid)
	if err != nil {
		return nil, err
	}
	bsb, err := p.bspb.OpenBlockStore(ledgerid)
	if err != nil {
		return nil, err
	}

	return newSplitterBlockStore(bsa, bsb), nil
}

// Exists returns whether or not the given ledger ID exists
func (p *SplitterBlockstoreProvider) Exists(ledgerid string) (bool, error) {
	p.bspb.Exists(ledgerid)
	return p.bspa.Exists(ledgerid)
}

// List returns the available ledger IDs
func (p *SplitterBlockstoreProvider) List() ([]string, error) {
	p.bspb.List()
	return p.bspa.List()
}

// Close cleans up the Provider
func (p *SplitterBlockstoreProvider) Close() {
	p.bspb.Close()
	p.bspa.Close()
}
