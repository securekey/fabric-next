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

	var blocksToRemove []uint64
	var block *common.Block
	for _, blk := range b.blocks {
		if blk.Header.Number <= blockNum {
			if blk.Header.Number == blockNum {
				block = blk
			}
			blocksToRemove = append(blocksToRemove, blk.Header.Number)
		}
	}

	for _, num := range blocksToRemove {
		b.remove(num)
	}

	return block
}

// Size returns the size of the cache
func (b *blockCache) Size() int {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	return len(b.blocks)
}

func (b *blockCache) remove(blockNum uint64) {
	for i, blk := range b.blocks {
		if blk.Header.Number == blockNum {
			b.blocks = append(b.blocks[:i], b.blocks[i+1:]...)
			return
		}
	}
}
