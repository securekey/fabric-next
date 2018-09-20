/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbblkstorage

import (
	"github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
	"github.com/pkg/errors"
)

// cdbBlockStore ...
type cdbBlockStore struct {
	id string
}

// newCDBBlockStore constructs block store based on CouchDB
func newCDBBlockStore(id string, indexConfig *blkstorage.IndexConfig) *cdbBlockStore {
	return &cdbBlockStore{id}
}

// AddBlock adds a new block
func (store *cdbBlockStore) AddBlock(block *common.Block) error {
	return nil
}

// GetBlockchainInfo returns the current info about blockchain
func (store *cdbBlockStore) GetBlockchainInfo() (*common.BlockchainInfo, error) {
	return nil, errors.New("not implemented")
}

// RetrieveBlocks returns an iterator that can be used for iterating over a range of blocks
func (store *cdbBlockStore) RetrieveBlocks(startNum uint64) (ledger.ResultsIterator, error) {
	return nil, errors.New("not implemented")
}

// RetrieveBlockByHash returns the block for given block-hash
func (store *cdbBlockStore) RetrieveBlockByHash(blockHash []byte) (*common.Block, error) {
	return nil, errors.New("not implemented")
}

// RetrieveBlockByNumber returns the block at a given blockchain height
func (store *cdbBlockStore) RetrieveBlockByNumber(blockNum uint64) (*common.Block, error) {
	return nil, errors.New("not implemented")
}

// RetrieveTxByID returns a transaction for given transaction id
func (store *cdbBlockStore) RetrieveTxByID(txID string) (*common.Envelope, error) {
	return nil, errors.New("not implemented")
}

// RetrieveTxByBlockNumTranNum returns a transaction for given block ID and transaction ID
func (store *cdbBlockStore) RetrieveTxByBlockNumTranNum(blockNum uint64, tranNum uint64) (*common.Envelope, error) {
	return nil, errors.New("not implemented")
}

// RetrieveBlockByTxID returns a block for a given transaction ID
func (store *cdbBlockStore) RetrieveBlockByTxID(txID string) (*common.Block, error) {
	return nil, errors.New("not implemented")
}

// RetrieveTxValidationCodeByTxID returns a TX validation code for a given transaction ID
func (store *cdbBlockStore) RetrieveTxValidationCodeByTxID(txID string) (peer.TxValidationCode, error) {
	return peer.TxValidationCode_INVALID_ENDORSER_TRANSACTION, errors.New("not implemented")
}

// Shutdown closes the storage instance
func (store *cdbBlockStore) Shutdown() {
}