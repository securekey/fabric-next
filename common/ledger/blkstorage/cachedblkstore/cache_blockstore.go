/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cachedblkstore

import (
	"context"
	"fmt"
	"github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	ledgerUtil "github.com/hyperledger/fabric/core/ledger/util"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
	"github.com/hyperledger/fabric/protos/utils"
	"github.com/pkg/errors"
	"sync/atomic"
)

const (
	checkpointBlockInterval = 1 // number of blocks between checkpoints.
	blockStorageQueueLen    = checkpointBlockInterval
)

type cachedBlockStore struct {
	blockStore     blockStoreWithCheckpoint
	blockIndex     blkstorage.BlockIndex
	blockCache     blkstorage.BlockCache

	bcInfo         atomic.Value
	blockStoreCh   chan *common.Block
	checkpointCh   chan *common.Block
	writerClosedCh chan struct {}
	doneCh         chan struct {}
}

func newCachedBlockStore(blockStore blockStoreWithCheckpoint, blockIndex blkstorage.BlockIndex, blockCache blkstorage.BlockCache) (*cachedBlockStore, error) {
	s := cachedBlockStore{
		blockStore: blockStore,
		blockIndex: blockIndex,
		blockCache: blockCache,
		blockStoreCh: make(chan *common.Block, blockStorageQueueLen),
		checkpointCh: make(chan *common.Block, blockStorageQueueLen),
		writerClosedCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}

	curBcInfo, err := blockStore.GetBlockchainInfo()
	if err != nil {
		return nil, err
	}
	s.bcInfo.Store(curBcInfo)

	go s.blockWriter()

	return &s, nil
}

// AddBlock adds a new block
func (s *cachedBlockStore) AddBlock(block *common.Block) error {
	err := s.blockCache.AddBlock(block)
	if err != nil {
		blockNumber := block.GetHeader().GetNumber()
		return errors.WithMessage(err, fmt.Sprintf("block was not cached [%d]", blockNumber))
	}

	blockNumber := block.GetHeader().GetNumber()
	if blockNumber != 0 {
		logger.Debugf("waiting for previous block to checkpoint [%d]", blockNumber - checkpointBlockInterval)
		s.blockStore.WaitForBlock(context.Background(), blockNumber - checkpointBlockInterval)
		logger.Debugf("ready to store incoming block [%d]", blockNumber)
	}
	s.blockStoreCh <- block

	return nil
}

func (s *cachedBlockStore) CheckpointBlock(block *common.Block) error {
	s.checkpointCh <- block

	updatedBcInfo := createBlockchainInfo(block)
	s.bcInfo.Store(updatedBcInfo)

	return nil
}

func (s *cachedBlockStore) blockWriter() {
	const panicMsg = "block processing failure"

	for {
		select {
		case <- s.doneCh:
			close(s.writerClosedCh)
			return
		case block := <- s.blockStoreCh:
			//startBlockStorage := time.Now()
			blockNumber := block.GetHeader().GetNumber()
			logger.Debugf("processing block for storage [%d]", blockNumber)

			err := s.blockStore.AddBlock(block)
			if err != nil {
				logger.Errorf("block was not added [%d, %s]", blockNumber, err)
				panic(panicMsg)
			}

			err = s.blockIndex.AddBlock(block)
			if err != nil {
				logger.Errorf("block was not indexed [%d, %s]", blockNumber, err)
				panic(panicMsg)
			}

			ok := s.blockCache.OnBlockStored(blockNumber)
			if !ok {
				logger.Errorf("block cache does not contain block [%d]", blockNumber)
				panic(panicMsg)
			}
			//elapsedBlockStorage := time.Since(startBlockStorage) / time.Millisecond // duration in ms
			//logger.Debugf("Stored block [%d] in %dms", block.Header.Number, elapsedBlockStorage)
		case block := <- s.checkpointCh:
			blockNumber := block.GetHeader().GetNumber()
			logger.Debugf("processing block checkpoint [%d]", blockNumber)
			err := s.blockStore.CheckpointBlock(block)
			if err != nil {
				blockNumber := block.GetHeader().GetNumber()
				logger.Errorf("block was not added [%d, %s]", blockNumber, err)
				panic(panicMsg)
			}
		}
	}
}

// GetBlockchainInfo returns the current info about blockchain
func (s *cachedBlockStore) GetBlockchainInfo() (*common.BlockchainInfo, error) {
	return s.bcInfo.Load().(*common.BlockchainInfo), nil
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

	b, err :=  s.blockStore.RetrieveBlockByHash(blockHash)
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
func (s *cachedBlockStore) RetrieveTxByID(txID string) (*common.Envelope, error) {
	loc, err := s.retrieveTxLoc(txID)
	if err != nil {
		return nil, err
	}

	return s.RetrieveTxByBlockNumTranNum(loc.BlockNumber(), loc.TxNumber())
}

func (s *cachedBlockStore) retrieveTxLoc(txID string) (blkstorage.TxLoc, error) {
	loc, ok := s.blockCache.LookupTxLoc(txID)
	if ok {
		return loc, nil
	}

	return s.blockIndex.RetrieveTxLoc(txID)
}

func extractEnvelopeFromBlock(block *common.Block, tranNum uint64) (*common.Envelope, error) {
	blockData := block.GetData()
	envelopes := blockData.GetData()
	envelopesLen := uint64(len(envelopes))
	if envelopesLen - 1 < tranNum {
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

func extractTxValidationCode(block *common.Block, txNumber uint64) peer.TxValidationCode {
	blockMetadata := block.GetMetadata()
	txValidationFlags := ledgerUtil.TxValidationFlags(blockMetadata.GetMetadata()[common.BlockMetadataIndex_TRANSACTIONS_FILTER])
	return txValidationFlags.Flag(int(txNumber))
}

// Shutdown closes the storage instance
func (s *cachedBlockStore) Shutdown() {
	close(s.doneCh)
	<- s.writerClosedCh

	s.blockCache.Shutdown()
	s.blockIndex.Shutdown()
	s.blockStore.Shutdown()
}

func createBlockchainInfo(block *common.Block) *common.BlockchainInfo {
	hash := block.GetHeader().Hash()
	number := block.GetHeader().GetNumber()
	bi := common.BlockchainInfo {
		Height:            number + 1,
		CurrentBlockHash:  hash,
		PreviousBlockHash: block.Header.PreviousHash,
	}
	return &bi
}