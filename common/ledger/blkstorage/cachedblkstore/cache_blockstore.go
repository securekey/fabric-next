/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cachedblkstore

import (
	"context"
	"fmt"
	"sync"

	"github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	cledger "github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	ledgerUtil "github.com/hyperledger/fabric/core/ledger/util"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
	"github.com/hyperledger/fabric/protos/utils"
	"github.com/pkg/errors"
)

type cachedBlockStore struct {
	blockStore blockStoreWithCheckpoint
	blockIndex blkstorage.BlockIndex
	blockCache blkstorage.BlockCache

	bcInfo         *common.BlockchainInfo
	cpInfoSig      chan struct{}
	cpInfoMtx      sync.RWMutex
	blockStoreCh   chan *common.Block
	checkpointCh   chan *common.Block
	writerClosedCh chan struct{}
	doneCh         chan struct{}
	blockReadyCh   chan bool
}

func newCachedBlockStore(blockStore blockStoreWithCheckpoint, blockIndex blkstorage.BlockIndex, blockCache blkstorage.BlockCache) (*cachedBlockStore, error) {
	s := cachedBlockStore{
		blockStore:     blockStore,
		blockIndex:     blockIndex,
		blockCache:     blockCache,
		cpInfoSig:      make(chan struct{}),
		cpInfoMtx:      sync.RWMutex{},
		blockStoreCh:   make(chan *common.Block),
		checkpointCh:   make(chan *common.Block),
		writerClosedCh: make(chan struct{}),
		doneCh:         make(chan struct{}),
		blockReadyCh:   make(chan bool),
	}

	curBcInfo, err := blockStore.GetBlockchainInfo()
	if err != nil {
		return nil, err
	}
	s.bcInfo = curBcInfo

	concurrentBlockWrites := ledgerconfig.GetConcurrentBlockWrites()
	for x := 0; x < concurrentBlockWrites; x++ {
		go s.blockWriter()
	}

	return &s, nil
}

// AddBlock adds a new block
func (s *cachedBlockStore) AddBlock(block *common.Block) error {
	/*stopWatch := metrics.StopWatch("cached_block_store_add_block_duration")
	defer stopWatch()*/

	err := s.blockCache.AddBlock(block)
	if err != nil {
		blockNumber := block.GetHeader().GetNumber()
		return errors.WithMessage(err, fmt.Sprintf("block was not cached [%d]", blockNumber))
	}

	// TODO: This is a quick patch - needs to ensure that there are no gaps as well.
	blockNumber := block.GetHeader().GetNumber()
	if blockNumber > uint64(ledgerconfig.GetConcurrentBlockWrites()) {
		waitForBlock := blockNumber - uint64(ledgerconfig.GetConcurrentBlockWrites())

		// Wait for underlying storage to complete commit on previous block.
		logger.Debugf("waiting for previous block to checkpoint [%d]", waitForBlock)
		//stopWatchWaitBlock := metrics.StopWatch("cached_block_store_add_block_wait_block_duration")
		s.blockStore.WaitForBlock(context.Background(), waitForBlock)
		//stopWatchWaitBlock()
		logger.Debugf("ready to store incoming block [%d]", blockNumber)
	}

	//stopWatchWaitQueue := metrics.StopWatch("cached_block_store_add_block_wait_queue_duration")
	s.blockStoreCh <- block
	//stopWatchWaitQueue()

	return nil
}

func (s *cachedBlockStore) CheckpointBlock(block *common.Block) error {
	<-s.blockReadyCh
	s.checkpointCh <- block

	s.cpInfoMtx.Lock()
	s.bcInfo = createBlockchainInfo(block)
	close(s.cpInfoSig)
	s.cpInfoSig = make(chan struct{})
	s.cpInfoMtx.Unlock()

	return nil
}

func (s *cachedBlockStore) blockWriter() {
	const panicMsg = "block processing failure"

	for {
		select {
		case <-s.doneCh:
			close(s.writerClosedCh)
			return
		case block := <-s.blockStoreCh:
			//stopWatch := metrics.StopWatch("cached_block_store_blockWriter_blockStore")
			blockNumber := block.GetHeader().GetNumber()
			logger.Debugf("processing block for storage [%d]", blockNumber)

			err := s.blockStore.AddBlock(block)
			if err != nil {
				logger.Errorf("block was not added [%d, %s]", blockNumber, err)
				//stopWatch()
				panic(panicMsg)
			}

			err = s.blockIndex.AddBlock(block)
			if err != nil {
				logger.Errorf("block was not indexed [%d, %s]", blockNumber, err)
				//stopWatch()
				panic(panicMsg)
			}
			s.blockReadyCh <- true
			//stopWatch()
		case block := <-s.checkpointCh:
			//stopWatch := metrics.StopWatch("cached_block_store_blockCheckPoint_processing")
			blockNumber := block.GetHeader().GetNumber()
			logger.Debugf("processing block checkpoint [%d]", blockNumber)
			err := s.blockStore.CheckpointBlock(block)
			if err != nil {
				blockNumber := block.GetHeader().GetNumber()
				logger.Errorf("block was not added [%d, %s]", blockNumber, err)
				//stopWatch()
				panic(panicMsg)
			}

			ok := s.blockCache.OnBlockStored(blockNumber)
			if !ok {
				logger.Errorf("block cache does not contain block [%d]", blockNumber)
				//stopWatch()
				panic(panicMsg)
			}
			//stopWatch()
		}
	}
}

// GetBlockchainInfo returns the current info about blockchain
func (s *cachedBlockStore) GetBlockchainInfo() (*common.BlockchainInfo, error) {
	s.cpInfoMtx.RLock()
	defer s.cpInfoMtx.RUnlock()
	return s.bcInfo, nil
}

// RetrieveBlocks returns an iterator that can be used for iterating over a range of blocks
func (s *cachedBlockStore) RetrieveBlocks(startNum uint64) (ledger.ResultsIterator, error) {
	return newBlockItr(s, startNum), nil
}

// RetrieveBlockByHash returns the block for given block-hash
func (s *cachedBlockStore) RetrieveBlockByHash(blockHash []byte) (*common.Block, error) {
	b, ok := s.blockCache.LookupBlockByHash(blockHash)
	if ok {
		return b, nil
	}

	b, err := s.blockStore.RetrieveBlockByHash(blockHash)
	if err != nil {
		return nil, err
	}

	s.blockCache.AddBlock(b)
	return b, nil
}

// RetrieveBlockByNumber returns the block at a given blockchain height
func (s *cachedBlockStore) RetrieveBlockByNumber(blockNum uint64) (*common.Block, error) {
	b, ok := s.blockCache.LookupBlockByNumber(blockNum)
	if ok {
		return b, nil
	}

	b, err := s.blockStore.RetrieveBlockByNumber(blockNum)
	if err != nil {
		return nil, err
	}

	s.blockCache.AddBlock(b)
	return b, nil
}

// RetrieveTxByBlockNumTranNum returns a transaction for given block number and transaction number
func (s *cachedBlockStore) RetrieveTxByBlockNumTranNum(blockNum uint64, tranNum uint64) (*common.Envelope, error) {
	b, ok := s.blockCache.LookupBlockByNumber(blockNum)
	if ok {
		return extractEnvelopeFromBlock(b, tranNum)
	}

	b, err := s.blockStore.RetrieveBlockByNumber(blockNum)
	if err != nil {
		return nil, err
	}

	e, err := extractEnvelopeFromBlock(b, tranNum)
	if err != nil {
		return nil, err
	}

	s.blockCache.AddBlock(b)
	return e, nil
}

// RetrieveTxByID returns a transaction for given transaction id
func (s *cachedBlockStore) RetrieveTxByID(txID string, hints ...cledger.SearchHint) (*common.Envelope, error) {
	loc, err := s.retrieveTxLoc(txID, hints...)
	if err != nil {
		return nil, err
	}

	return s.RetrieveTxByBlockNumTranNum(loc.BlockNumber(), loc.TxNumber())
}

func (s *cachedBlockStore) retrieveTxLoc(txID string, hints ...cledger.SearchHint) (blkstorage.TxLoc, error) {
	loc, ok := s.blockCache.LookupTxLoc(txID)
	if ok {
		return loc, nil
	} else if searchCacheOnly(hints...) {
		return nil, cledger.NotFoundInIndexErr(txID)
	}

	return s.blockIndex.RetrieveTxLoc(txID)
}

// Returns true if the 'RecentOnly' search hint is passed.
func searchCacheOnly(hints ...cledger.SearchHint) bool {
	for _, hint := range hints {
		if hint == cledger.RecentOnly {
			return true
		}
	}
	return false
}

func extractEnvelopeFromBlock(block *common.Block, tranNum uint64) (*common.Envelope, error) {
	blockData := block.GetData()
	envelopes := blockData.GetData()
	envelopesLen := uint64(len(envelopes))
	if envelopesLen-1 < tranNum {
		blockNum := block.GetHeader().GetNumber()
		return nil, errors.Errorf("transaction number is invalid [%d, %d, %d]", blockNum, envelopesLen, tranNum)
	}

	return utils.GetEnvelopeFromBlock(envelopes[tranNum])
}

// RetrieveBlockByTxID returns a block for a given transaction ID
func (s *cachedBlockStore) RetrieveBlockByTxID(txID string) (*common.Block, error) {
	loc, err := s.retrieveTxLoc(txID)
	if err != nil {
		return nil, err
	}

	return s.RetrieveBlockByNumber(loc.BlockNumber())
}

// RetrieveTxValidationCodeByTxID returns a TX validation code for a given transaction ID
func (s *cachedBlockStore) RetrieveTxValidationCodeByTxID(txID string) (peer.TxValidationCode, error) {
	loc, ok := s.blockCache.LookupTxLoc(txID)
	if ok {
		block, ok := s.blockCache.LookupBlockByNumber(loc.BlockNumber())
		if ok {
			return extractTxValidationCode(block, loc.TxNumber()), nil
		}
	}

	// Note: in this case, the block is not added to the cache so we always hit the index for old txn validation codes
	// TODO: make an explicit cache for txn validation codes?
	return s.blockIndex.RetrieveTxValidationCodeByTxID(txID)
}

func (s *cachedBlockStore) LastBlockNumber() uint64 {
	s.cpInfoMtx.RLock()
	defer s.cpInfoMtx.RUnlock()
	return s.bcInfo.GetHeight() - 1
}

func (s *cachedBlockStore) BlockCommitted() (uint64, chan struct{}) {
	return s.blockStore.BlockCommitted()
}

func (s *cachedBlockStore) WaitForBlock(ctx context.Context, blockNum uint64) uint64 {
	var lastBlockNumber uint64

BlockLoop:
	for {
		s.cpInfoMtx.RLock()
		sigCh := s.cpInfoSig
		lastBlockNumber := s.bcInfo.GetHeight() - 1
		s.cpInfoMtx.RUnlock()

		if lastBlockNumber >= blockNum {
			break
		}

		logger.Debugf("waiting for newer blocks [%d, %d]", lastBlockNumber, blockNum)
		select {
		case <-ctx.Done():
			break BlockLoop
		case <-sigCh:
		}
	}

	logger.Debugf("finished waiting for blocks [%d, %d]", lastBlockNumber, blockNum)
	return lastBlockNumber
}

func extractTxValidationCode(block *common.Block, txNumber uint64) peer.TxValidationCode {
	blockMetadata := block.GetMetadata()
	txValidationFlags := ledgerUtil.TxValidationFlags(blockMetadata.GetMetadata()[common.BlockMetadataIndex_TRANSACTIONS_FILTER])
	return txValidationFlags.Flag(int(txNumber))
}

// Shutdown closes the storage instance
func (s *cachedBlockStore) Shutdown() {
	close(s.doneCh)
	<-s.writerClosedCh

	s.blockCache.Shutdown()
	s.blockIndex.Shutdown()
	s.blockStore.Shutdown()
}

func createBlockchainInfo(block *common.Block) *common.BlockchainInfo {
	hash := block.GetHeader().Hash()
	number := block.GetHeader().GetNumber()
	bi := common.BlockchainInfo{
		Height:            number + 1,
		CurrentBlockHash:  hash,
		PreviousBlockHash: block.Header.PreviousHash,
	}
	return &bi
}
