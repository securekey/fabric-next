/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package fsblkstorage

import (
	"context"
	"github.com/hyperledger/fabric/common/ledger"
	"sync"
)

// blocksItr - an iterator for iterating over a sequence of blocks
type blocksItr struct {
	mgr                  *blockfileMgr
	maxBlockNumAvailable uint64
	blockNumToRetrieve   uint64
	stream               *blockStream
	streamMtx            sync.Mutex
	ctx                  context.Context
	cancel               context.CancelFunc
}

func newBlockItr(mgr *blockfileMgr, startBlockNum uint64) *blocksItr {
	ctx, cancel := context.WithCancel(context.Background())
	return &blocksItr{mgr, mgr.lastBlockNumber(), startBlockNum, nil, sync.Mutex{}, ctx, cancel}
}
// Next moves the cursor to next block and returns true iff the iterator is not exhausted
func (itr *blocksItr) Next() (ledger.QueryResult, error) {
	if itr.maxBlockNumAvailable < itr.blockNumToRetrieve {
		itr.maxBlockNumAvailable = itr.mgr.waitForBlock(itr.ctx,itr.blockNumToRetrieve)
	}
	select {
	case <-itr.ctx.Done():
		return nil, nil
	default:
	}

	itr.streamMtx.Lock()
	defer itr.streamMtx.Unlock()
	if itr.stream == nil {
		logger.Debugf("Initializing block stream for iterator. itr.maxBlockNumAvailable=%d", itr.maxBlockNumAvailable)
		if err := itr.initStream(); err != nil {
			return nil, err
		}
	}
	nextBlockBytes, err := itr.stream.nextBlockBytes()
	if err != nil {
		return nil, err
	}
	itr.blockNumToRetrieve++
	return deserializeBlock(nextBlockBytes)
}

func (itr *blocksItr) initStream() error {
	var lp *fileLocPointer
	var err error
	if lp, err = itr.mgr.index.getBlockLocByBlockNum(itr.blockNumToRetrieve); err != nil {
		return err
	}
	if itr.stream, err = newBlockStream(itr.mgr.rootDir, lp.fileSuffixNum, int64(lp.offset), -1); err != nil {
		return err
	}
	return nil
}

// Close releases any resources held by the iterator
func (itr *blocksItr) Close() {
	itr.cancel()

	itr.streamMtx.Lock()
	if itr.stream != nil {
		itr.stream.close()
	}
	itr.streamMtx.Unlock()
}
