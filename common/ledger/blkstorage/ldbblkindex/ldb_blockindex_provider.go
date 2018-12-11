/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ldbblkindex

import (
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/common/ledger/util"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
)

var logger = flogging.MustGetLogger("ldbblkindex")

// MemBlockCacheProvider provides block cache in memory
type LevelBlockIndexProvider struct {
	conf        *Conf
	indexConfig *blkstorage.IndexConfig
	leveldbProvider *leveldbhelper.Provider
}

// NewProvider constructs a filesystem based block store provider
func NewProvider(conf *Conf, indexConfig *blkstorage.IndexConfig) *LevelBlockIndexProvider {
	ldbp := leveldbhelper.NewProvider(&leveldbhelper.Conf{DBPath: conf.getIndexDir()})

	return &LevelBlockIndexProvider{conf, indexConfig, ldbp}
}

// OpenBlockStore opens the block cache for the given ledger ID
func (p *LevelBlockIndexProvider) OpenBlockIndex(ledgerid string) (blkstorage.BlockIndex, error) {
	indexStore := p.leveldbProvider.GetDBHandle(ledgerid)
	return newBlockIndex(p.indexConfig, indexStore)
}

// Exists returns whether or not the given ledger ID exists
func (p *LevelBlockIndexProvider) Exists(ledgerid string) (bool, error) {
	exists, _, err := util.FileExists(p.conf.getLedgerBlockDir(ledgerid))
	return exists, err
}

// List returns the available ledger IDs
func (p *LevelBlockIndexProvider) List() ([]string, error) {
	return util.ListSubdirs(p.conf.getChainsDir())
}

// Close cleans up the Provider
func (p *LevelBlockIndexProvider) Close() {
	p.leveldbProvider.Close()
}
