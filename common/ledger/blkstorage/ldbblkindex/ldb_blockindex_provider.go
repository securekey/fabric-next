/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ldbblkindex

import (
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
)

var logger = flogging.MustGetLogger("memblkcache")

// MemBlockCacheProvider provides block cache in memory
type LevelBlockIndexProvider struct {
	indexConfig *blkstorage.IndexConfig
}

// NewProvider constructs a filesystem based block store provider
func NewProvider(indexConfig *blkstorage.IndexConfig) *LevelBlockIndexProvider {
	return &LevelBlockIndexProvider{indexConfig}
}

// OpenBlockStore opens the block cache for the given ledger ID
func (p *LevelBlockIndexProvider) OpenBlockIndex(ledgerid string) (blkstorage.BlockIndex, error) {
	return nil, nil
}

// Exists returns whether or not the given ledger ID exists
func (p *LevelBlockIndexProvider) Exists(ledgerid string) (bool, error) {
	return false, nil
}

// List returns the available ledger IDs
func (p *LevelBlockIndexProvider) List() ([]string, error) {
	return nil, nil
}

// Close cleans up the Provider
func (p *LevelBlockIndexProvider) Close() {
}
