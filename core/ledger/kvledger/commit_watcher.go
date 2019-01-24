/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package kvledger

import (
	"fmt"
	"time"

	"github.com/hyperledger/fabric/common/metrics"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/core/ledger/pvtdatapolicy"
	"github.com/hyperledger/fabric/protos/common"
)

//commitWatcher - implementation for BlockCommitted interface which gets notified when a block gets committed from txmanager
type commitWatcher struct {
	commitDoneCh chan *ledger.BlockAndPvtData
}

func (c *commitWatcher) OnBlockCommit(blockAndPvtData *ledger.BlockAndPvtData) {
	c.commitDoneCh <- blockAndPvtData
}

// TODO: merge into BlockStore interface
type blockCommitNotifier interface {
	BlockCommitted() (uint64, chan struct{})
}

// commitWatcher gets notified when each commit is done and it performs required cache cleanup
func (l *kvLedger) commitWatcher(btlPolicy pvtdatapolicy.BTLPolicy) {
	// TODO: merge interfaces
	store, ok := l.blockStore.BlockStore.(blockCommitNotifier)
	if !ok {
		panic("commitWatcher using an incompatible blockStore")
	}

	blockNo, nextBlockCh := store.BlockCommitted()
	pvtdataAndBlock, commitDoneCh := l.txtmgmt.BlockCommitted()

	type blockCommitProgress struct {
		nextBlock                                   *common.Block
		commitStartTime                             time.Time
		stateCommittedDuration, elapsedBlockStorage time.Duration
		blockCommitted, stateCommitted              bool
	}
	commitProgress := make(map[uint64]*blockCommitProgress)

	checkForDone := func(cp *blockCommitProgress) {
		if cp.blockCommitted && cp.stateCommitted {
			elapsedCommitWithPvtData := time.Since(cp.commitStartTime)

			metrics.RootScope.Gauge(fmt.Sprintf("kvledger_%s_committed_block_number", metrics.FilterMetricName(l.ledgerID))).Update(float64(blockNo))
			metrics.RootScope.Timer(fmt.Sprintf("kvledger_%s_committed_duration", metrics.FilterMetricName(l.ledgerID))).Record(elapsedCommitWithPvtData)
			if metrics.IsDebug() {
				metrics.RootScope.Timer(fmt.Sprintf("kvledger_%s_committed_state_duration", metrics.FilterMetricName(l.ledgerID))).Record(cp.stateCommittedDuration)
				metrics.RootScope.Timer(fmt.Sprintf("kvledger_%s_committed_block_duration", metrics.FilterMetricName(l.ledgerID))).Record(cp.elapsedBlockStorage)
			}

			// TODO: more precise start times for elapsedBlockStorage and stateCommittedDuration
			logger.Infof("[%s] Committed block [%d] with %d transaction(s) in %dms (block_commit=%dms state_commit=%dms)",
				l.ledgerID, blockNo, len(cp.nextBlock.Data.Data), elapsedCommitWithPvtData/time.Millisecond, cp.elapsedBlockStorage/time.Millisecond, cp.stateCommittedDuration/time.Millisecond)

			delete(commitProgress, cp.nextBlock.GetHeader().GetNumber())
		}
	}

	for {
		select {
		case <-l.doneCh: // kvledger is shutting down.
			close(l.stoppedCommitCh)
			return
		case <-commitDoneCh: // State has been committed - unpin keys from cache
			pvtdataAndBlock, commitDoneCh = l.txtmgmt.BlockCommitted()

			block := pvtdataAndBlock.Block
			pvtData := pvtdataAndBlock.BlockPvtData
			cp, ok := commitProgress[block.Header.Number]
			if !ok {
				panic(fmt.Sprintf("unexpected block committed [%d]", block.Header.Number))
			}

			logger.Debugf("*** cleaning up pinned tx in cache for cacheBlock %d channelID %s\n", block.Header.Number, l.ledgerID)
			validatedTxOps, pvtDataHashedKeys, txValidationFlags, err := l.getKVFromBlock(block, btlPolicy)
			if err != nil {
				logger.Errorf(" failed to clear pinned tx for committed block %d : %s", pvtdataAndBlock.Block.Header.GetNumber(), err)
			}
			pvtDataKeys, _, err := getPrivateDataKV(block.Header.Number, l.ledgerID, pvtData, txValidationFlags, btlPolicy)
			if err != nil {
				logger.Errorf(" failed to clear pinned tx for committed block %d : %s", pvtdataAndBlock.Block.Header.GetNumber(), err)
			}

			l.kvCacheProvider.OnTxCommit(validatedTxOps, pvtDataKeys, pvtDataHashedKeys)

			cp.stateCommittedDuration = time.Since(cp.commitStartTime)
			cp.stateCommitted = true
			checkForDone(cp)
		case <-nextBlockCh: // A block has been committed to storage.

			blockNo, nextBlockCh = store.BlockCommitted()
			if !ledgerconfig.IsCommitter() { // TODO: refactor AddBlock to do similar
				continue
			}

			cp, ok := commitProgress[blockNo]
			if !ok {
				panic(fmt.Sprintf("unexpected block committed [%d, %d]", blockNo, cp.nextBlock.Header.Number))
			}

			cp.elapsedBlockStorage = time.Since(cp.commitStartTime)
			cp.blockCommitted = true
			checkForDone(cp)
		case pvtdataAndBlock := <-l.commitCh: // Process next block through commit workflow (should be last case statement).
			cp := blockCommitProgress{
				nextBlock:       pvtdataAndBlock.Block,
				commitStartTime: time.Now(),
			}
			commitProgress[pvtdataAndBlock.Block.GetHeader().GetNumber()] = &cp

			err := l.commitWithPvtData(pvtdataAndBlock)
			if err != nil {
				panic(err)
			}
		}
	}
}
