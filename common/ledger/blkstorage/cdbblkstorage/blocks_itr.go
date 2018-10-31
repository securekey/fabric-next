/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbblkstorage

import (
	"context"
	"github.com/hyperledger/fabric/common/ledger"
)

// blocksItr - an iterator for iterating over a sequence of blocks
type blocksItr struct {
	cdbBlockStore        *cdbBlockStore
	maxBlockNumAvailable uint64
	blockNumToRetrieve   uint64
	ctx                  context.Context
	cancel               context.CancelFunc
}

func newBlockItr(cdbBlockStore *cdbBlockStore, startBlockNum uint64) *blocksItr {
	ctx, cancel := context.WithCancel(context.Background())
	return &blocksItr{cdbBlockStore, cdbBlockStore.LastBlockNumber(), startBlockNum, ctx, cancel}
}

// Next moves the cursor to next block and returns true iff the iterator is not exhausted
func (itr *blocksItr) Next() (ledger.QueryResult, error) {
	if itr.maxBlockNumAvailable < itr.blockNumToRetrieve {
		itr.maxBlockNumAvailable = itr.cdbBlockStore.WaitForBlock(itr.ctx, itr.blockNumToRetrieve)
	}
	// If we still haven't met the condition, the iterator has been closed.
	if itr.maxBlockNumAvailable < itr.blockNumToRetrieve {
		return nil, nil
	}

	nextBlock, err := itr.cdbBlockStore.RetrieveBlockByNumber(itr.blockNumToRetrieve)
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
