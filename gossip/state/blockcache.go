/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"sync"

	"github.com/hyperledger/fabric/protos/common"
)

type blockCache struct {
	blockByNumber map[uint64]*common.Block
	mutex         sync.Mutex
}

func newBlockCache() *blockCache {
	return &blockCache{
		blockByNumber: make(map[uint64]*common.Block),
	}
}

func (b *blockCache) Add(block *common.Block) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.blockByNumber[block.Header.Number] = block
}

func (b *blockCache) Remove(blockNum uint64) *common.Block {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	block, ok := b.blockByNumber[blockNum]
	if ok {
		delete(b.blockByNumber, blockNum)
	}
	return block
}
