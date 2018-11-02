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


type cachedBlockStore struct {
	blockStore blockStoreWithCheckpoint
	blockIndex blkstorage.BlockIndex
	blockCache blkstorage.BlockCache
}

func newCachedBlockStore(blockStore blockStoreWithCheckpoint, blockIndex blkstorage.BlockIndex, blockCache blkstorage.BlockCache) *cachedBlockStore {
	s := cachedBlockStore{
		blockStore: blockStore,
		blockIndex: blockIndex,
		blockCache: blockCache,
	}

	return &s
}

// AddBlock adds a new block
func (s *cachedBlockStore) AddBlock(block *common.Block) error {
	err := s.blockStore.AddBlock(block)
	if err != nil {
		return err
	}

	err = s.blockIndex.AddBlock(block)
	if err != nil {
		return errors.WithMessage(err, "block was not indexed")
	}

	err = s.blockCache.AddBlock(block)
	if err != nil {
		blockNumber := block.GetHeader().GetNumber()
		logger.Warningf("block was not cached [%d, %s]", blockNumber, err)
	}
	return nil
}

func (s *cachedBlockStore) CheckpointBlock(block *common.Block) error {
	return s.blockStore.CheckpointBlock(block)
}

// GetBlockchainInfo returns the current info about blockchain
func (s *cachedBlockStore) GetBlockchainInfo() (*common.BlockchainInfo, error) {
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

	return s.blockIndex.RetrieveTxValidationCodeByTxID(txID)
}

func extractTxValidationCode(block *common.Block, txNumber uint64) peer.TxValidationCode {
	blockMetadata := block.GetMetadata()
	txValidationFlags := ledgerUtil.TxValidationFlags(blockMetadata.GetMetadata()[common.BlockMetadataIndex_TRANSACTIONS_FILTER])
	return txValidationFlags.Flag(int(txNumber))
}

// Shutdown closes the storage instance
func (s *cachedBlockStore) Shutdown() {
	s.blockCache.Shutdown()
	s.blockIndex.Shutdown()
	s.blockStore.Shutdown()
}