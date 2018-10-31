/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cachedblkstore

import (
	"github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
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

	//err = s.blockIndex.AddBlock(block)
	//if err != nil {
	//	return errors.WithMessage(err, "block was not indexed")
	//}

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

	return s.blockStore.RetrieveBlockByHash(blockHash)
}

// RetrieveBlockByNumber returns the block at a given blockchain height
func (s *cachedBlockStore) RetrieveBlockByNumber(blockNum uint64) (*common.Block, error) {
	b, ok := s.blockCache.LookupBlockByNumber(blockNum)
	if ok {
		return b, nil
	}

	return s.blockStore.RetrieveBlockByNumber(blockNum)
}

// RetrieveTxByID returns a transaction for given transaction id
func (s *cachedBlockStore) RetrieveTxByID(txID string) (*common.Envelope, error) {
	return s.blockStore.RetrieveTxByID(txID)
}

// RetrieveTxByBlockNumTranNum returns a transaction for given block ID and transaction ID
func (s *cachedBlockStore) RetrieveTxByBlockNumTranNum(blockNum uint64, tranNum uint64) (*common.Envelope, error) {
	return s.blockStore.RetrieveTxByBlockNumTranNum(blockNum, tranNum)
}

// RetrieveBlockByTxID returns a block for a given transaction ID
func (s *cachedBlockStore) RetrieveBlockByTxID(txID string) (*common.Block, error) {
	return s.blockStore.RetrieveBlockByTxID(txID)
}

// RetrieveTxValidationCodeByTxID returns a TX validation code for a given transaction ID
func (s *cachedBlockStore) RetrieveTxValidationCodeByTxID(txID string) (peer.TxValidationCode, error) {
	return s.blockStore.RetrieveTxValidationCodeByTxID(txID)
}

// Shutdown closes the storage instance
func (s *cachedBlockStore) Shutdown() {
	s.blockCache.Shutdown()
	s.blockStore.Shutdown()
}