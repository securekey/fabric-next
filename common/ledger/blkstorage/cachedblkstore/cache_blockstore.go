/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cachedblkstore

import (
	"github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	ledgerUtil "github.com/hyperledger/fabric/core/ledger/util"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
	"github.com/hyperledger/fabric/protos/utils"
	"github.com/pkg/errors"
)

const (
	defMaxBlockWriterLen = 10
)

type cachedBlockStore struct {
	blockStore     blockStoreWithCheckpoint
	blockIndex     blkstorage.BlockIndex
	blockCache     blkstorage.BlockCache

	blockWriterCh  chan *common.Block
	writerClosedCh chan struct {}
	doneCh         chan struct {}
}

func newCachedBlockStore(blockStore blockStoreWithCheckpoint, blockIndex blkstorage.BlockIndex, blockCache blkstorage.BlockCache) *cachedBlockStore {
	s := cachedBlockStore{
		blockStore: blockStore,
		blockIndex: blockIndex,
		blockCache: blockCache,
		blockWriterCh: make(chan *common.Block, defMaxBlockWriterLen),
		writerClosedCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}

	go blockWriter(s.blockWriterCh, s.doneCh, s.writerClosedCh, s.blockStore, s.blockIndex, s.blockCache)

	return &s
}

// AddBlock adds a new block
func (s *cachedBlockStore) AddBlock(block *common.Block) error {
	err := s.blockCache.AddBlock(block)
	if err != nil {
		blockNumber := block.GetHeader().GetNumber()
		logger.Warningf("block was not cached [%d, %s]", blockNumber, err)
	}

	s.blockWriterCh <- block

	return nil
}

func blockWriter(ch chan *common.Block, done chan struct {}, closed chan struct {}, blockStore blockStoreWithCheckpoint, blockIndex blkstorage.BlockIndex, blockCache blkstorage.BlockCache) {
	handleAddBlockFailed := func(block *common.Block) {
		blockNumber := block.GetHeader().GetNumber()
		err := blockCache.OnBlockStored(blockNumber, true)
		if err != nil {
			logger.Warningf("block was not cached [%d, %s]", blockNumber, err)
		}
	}

	for {
		select {
		case <- done:
			close(closed)
			return
		case block := <- ch:
			err := blockStore.AddBlock(block)
			if err != nil {
				blockNumber := block.GetHeader().GetNumber()
				logger.Errorf("block was not added [%d, %s]", blockNumber, err)
				handleAddBlockFailed(block)
			}

			err = blockIndex.AddBlock(block)
			if err != nil {
				blockNumber := block.GetHeader().GetNumber()
				logger.Errorf("block was not indexed [%d, %s]", blockNumber, err)
				handleAddBlockFailed(block)
			}

			blockNumber := block.GetHeader().GetNumber()
			err = blockCache.OnBlockStored(blockNumber, true)
			if err != nil {
				logger.Warningf("block cache was not notified of storage [%d, %s]", blockNumber, err)
			}
		}
	}
}

func (s *cachedBlockStore) CheckpointBlock(block *common.Block) error {
	// TODO: Change to be the current height of the cache (more explicitly).
	return s.blockStore.CheckpointBlock(block)
}

// GetBlockchainInfo returns the current info about blockchain
func (s *cachedBlockStore) GetBlockchainInfo() (*common.BlockchainInfo, error) {
	// TODO: Change to be the current height of the cache (more explicitly).
	return s.blockStore.GetBlockchainInfo()
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