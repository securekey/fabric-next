/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cachedblkstore

import (
	"context"
	"github.com/hyperledger/fabric/common/ledger"
)

// blocksItr - an iterator for iterating over a sequence of blocks
type blocksItr struct {
	cachedBlockStore     *cachedBlockStore
	maxBlockNumAvailable uint64
	blockNumToRetrieve   uint64
	ctx                  context.Context
	cancel               context.CancelFunc
}

func newBlockItr(cachedBlockStore *cachedBlockStore, startBlockNum uint64) *blocksItr {
	ctx, cancel := context.WithCancel(context.Background())
	return &blocksItr{cachedBlockStore, cachedBlockStore.LastBlockNumber(), startBlockNum, ctx, cancel}
}

// Next moves the cursor to next block and returns true iff the iterator is not exhausted
func (itr *blocksItr) Next() (ledger.QueryResult, error) {
	if itr.maxBlockNumAvailable < itr.blockNumToRetrieve {
		itr.maxBlockNumAvailable = itr.cachedBlockStore.WaitForBlock(itr.ctx, itr.blockNumToRetrieve)
	}
	select {
	case <-itr.ctx.Done():
		return nil, nil
	default:
	}

	nextBlock, err := itr.cachedBlockStore.RetrieveBlockByNumber(itr.blockNumToRetrieve)
	if err != nil {
		return nil, err
	}
	itr.blockNumToRetrieve++
	return nextBlock, nil
}

// Close releases any resources held by the iterator
func (itr *blocksItr) Close() {
	itr.cancel()
}
