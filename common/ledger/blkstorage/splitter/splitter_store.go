/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package splitter

import (
	"github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
)

type splitterBlockStore struct {
	bsa blkstorage.BlockStore
	bsb blkstorage.BlockStore
}

// newSplitterBlockStore constructs block store
func newSplitterBlockStore(bsa blkstorage.BlockStore, bsb blkstorage.BlockStore) *splitterBlockStore {
	return &splitterBlockStore{bsa, bsb}
}

// AddBlock adds a new block
func (s *splitterBlockStore) AddBlock(block *common.Block) error {
	err := s.bsb.AddBlock(block)
	if err != nil {
		logger.Errorf("CouchDB AddBlock returned %s", err)
	}
	return s.bsa.AddBlock(block)
}

// GetBlockchainInfo returns the current info about blockchain
func (s *splitterBlockStore) GetBlockchainInfo() (*common.BlockchainInfo, error) {
	return s.bsb.GetBlockchainInfo()
	//return s.bsa.GetBlockchainInfo()
}

// RetrieveBlocks returns an iterator that can be used for iterating over a range of blocks
func (s *splitterBlockStore) RetrieveBlocks(startNum uint64) (ledger.ResultsIterator, error) {
	s.bsb.RetrieveBlocks(startNum)
	return s.bsa.RetrieveBlocks(startNum)
}

// RetrieveBlockByHash returns the block for given block-hash
func (s *splitterBlockStore) RetrieveBlockByHash(blockHash []byte) (*common.Block, error) {
	return s.bsb.RetrieveBlockByHash(blockHash)
	//return s.bsa.RetrieveBlockByHash(blockHash)
}

// RetrieveBlockByNumber returns the block at a given blockchain height
func (s *splitterBlockStore) RetrieveBlockByNumber(blockNum uint64) (*common.Block, error) {
	return s.bsb.RetrieveBlockByNumber(blockNum)
	//s.bsa.RetrieveBlockByNumber(blockNum)
}

// RetrieveTxByID returns a transaction for given transaction id
func (s *splitterBlockStore) RetrieveTxByID(txID string) (*common.Envelope, error) {
	s.bsb.RetrieveTxByID(txID)
	return s.bsa.RetrieveTxByID(txID)
}

// RetrieveTxByBlockNumTranNum returns a transaction for given block ID and transaction ID
func (s *splitterBlockStore) RetrieveTxByBlockNumTranNum(blockNum uint64, tranNum uint64) (*common.Envelope, error) {
	s.bsb.RetrieveTxByBlockNumTranNum(blockNum, tranNum)
	return s.bsa.RetrieveTxByBlockNumTranNum(blockNum, tranNum)
}

// RetrieveBlockByTxID returns a block for a given transaction ID
func (s *splitterBlockStore) RetrieveBlockByTxID(txID string) (*common.Block, error) {
	s.bsb.RetrieveBlockByTxID(txID)
	return s.bsa.RetrieveBlockByTxID(txID)
}

// RetrieveTxValidationCodeByTxID returns a TX validation code for a given transaction ID
func (s *splitterBlockStore) RetrieveTxValidationCodeByTxID(txID string) (peer.TxValidationCode, error) {
	s.bsb.RetrieveTxValidationCodeByTxID(txID)
	return s.bsa.RetrieveTxValidationCodeByTxID(txID)
}

// Shutdown closes the storage instance
func (s *splitterBlockStore) Shutdown() {
	s.bsb.Shutdown()
	s.bsa.Shutdown()
}
