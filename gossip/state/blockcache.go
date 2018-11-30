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
	blocks []*common.Block
	mutex  sync.RWMutex
}

func newBlockCache() *blockCache {
	return &blockCache{}
}

// Add adds the given block to the cache
func (b *blockCache) Add(block *common.Block) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.blocks = append(b.blocks, block)
}

// Remove removes the given block number and also all blocks before that
// Returns the block for the given number or nil if not found
func (b *blockCache) Remove(blockNum uint64) *common.Block {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	var block *common.Block
	removeToIndex := -1
	for i := 0; i < len(b.blocks); i++ {
		blk := b.blocks[i]
		if blk.Header.Number <= blockNum {
			removeToIndex = i
			if blk.Header.Number == blockNum {
				block = blk
				break
			}
		}
	}

	if removeToIndex >= 0 {
		// Since blocks are added in order, remove all blocks before i
		b.blocks = b.blocks[removeToIndex+1:]
	}

	return block
}

// Size returns the size of the cache
func (b *blockCache) Size() int {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	return len(b.blocks)
}
