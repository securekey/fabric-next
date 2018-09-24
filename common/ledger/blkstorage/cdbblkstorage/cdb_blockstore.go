/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbblkstorage

import (
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"sync/atomic"

	"github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
	"github.com/pkg/errors"
)

// cdbBlockStore ...
type cdbBlockStore struct {
	db       *couchdb.CouchDatabase
	ledgerID string
	//cpInfoCond *sync.Cond
	bcInfo atomic.Value
	cp     *checkpoint
}

// newCDBBlockStore constructs block store based on CouchDB
func newCDBBlockStore(db *couchdb.CouchDatabase, ledgerID string, indexConfig *blkstorage.IndexConfig) *cdbBlockStore {
	cdbBlockStore := &cdbBlockStore{db: db, ledgerID: ledgerID}

	cdbBlockStore.cp = newCheckpoint(db)

	// cp = checkpointInfo, retrieve from the database the last block number that was written to that db.
	cpInfo := cdbBlockStore.cp.getCheckpointInfo()
	err := cdbBlockStore.cp.saveCurrentInfo(cpInfo)
	if err != nil {
		panic(fmt.Sprintf("Could not save cpInfo info to db: %s", err))
	}
	// Create a checkpoint condition (event) variable, for the  goroutine waiting for
	// or announcing the occurrence of an event.
	//cdbBlockStore.cpInfoCond = sync.NewCond(&sync.Mutex{})
	// init BlockchainInfo for external API's
	bcInfo := &common.BlockchainInfo{
		Height:            0,
		CurrentBlockHash:  nil,
		PreviousBlockHash: nil}

	if !cpInfo.isChainEmpty {
		//If start up is a restart of an existing storage, update BlockchainInfo for external API's
		lastBlockHeader, err := cdbBlockStore.RetrieveBlockByHash(cpInfo.lastBlockHash)
		if err != nil {
			panic(fmt.Sprintf("Could not retrieve header of the last block form file: %s", err))
		}
		lastBlockHash := lastBlockHeader.Header.Hash()
		previousBlockHash := lastBlockHeader.Header.PreviousHash
		bcInfo = &common.BlockchainInfo{
			Height:            lastBlockHeader.Header.Number + 1,
			CurrentBlockHash:  lastBlockHash,
			PreviousBlockHash: previousBlockHash}
	}

	cdbBlockStore.bcInfo.Store(bcInfo)
	return cdbBlockStore
}

// AddBlock adds a new block
func (s *cdbBlockStore) AddBlock(block *common.Block) error {
	doc, err := blockToCouchDoc(block)
	if err != nil {
		return errors.WithMessage(err, "converting block to couchDB document failed")
	}

	id := blockNumberToKey(block.GetHeader().Number)

	rev, err := s.db.SaveDoc(id, "", doc)
	if err != nil {
		return errors.WithMessage(err, "adding block to couchDB failed")
	}
	blockHash := block.GetHeader().Hash()
	//Update the checkpoint info with the results of adding the new block
	newCPInfo := &checkpointInfo{
		isChainEmpty:  false,
		lastBlockHash: blockHash}
	//save the checkpoint information in the database
	if err = s.cp.saveCurrentInfo(newCPInfo); err != nil {
		return errors.WithMessage(err, "adding cpInfo to couchDB failed")
	}
	//update the checkpoint info (for storage) and the blockchain info (for APIs) in the manager
	//s.updateCheckpoint(newCPInfo)
	s.updateBlockchainInfo(blockHash, block)
	logger.Debugf("block added to couchDB [%d, %s]", block.GetHeader().Number, rev)
	return nil
}

// GetBlockchainInfo returns the current info about blockchain
func (s *cdbBlockStore) GetBlockchainInfo() (*common.BlockchainInfo, error) {
	return s.bcInfo.Load().(*common.BlockchainInfo), nil
}

// RetrieveBlocks returns an iterator that can be used for iterating over a range of blocks
func (s *cdbBlockStore) RetrieveBlocks(startNum uint64) (ledger.ResultsIterator, error) {
	return nil, errors.New("not implemented")
}

// RetrieveBlockByHash returns the block for given block-hash
func (s *cdbBlockStore) RetrieveBlockByHash(blockHash []byte) (*common.Block, error) {
	blockHashHex := hex.EncodeToString(blockHash)
	const queryFmt = `
	{
		"selector": {
			"` + blockHashField + `": {
				"$eq": "%s"
			}
		},
		"use_index": ["_design/` + blockHashIndexDoc + `", "` + blockHashIndexName + `"]
	}`

	block, err := retrieveBlockQuery(s.db, fmt.Sprintf(queryFmt, blockHashHex))
	if err != nil {
		return nil, errors.WithMessage(err, fmt.Sprintf("block not found by hash []", blockHashHex))
	}
	return block, nil
}

// RetrieveBlockByNumber returns the block at a given blockchain height
func (s *cdbBlockStore) RetrieveBlockByNumber(blockNum uint64) (*common.Block, error) {
	// interpret math.MaxUint64 as a request for last block
	if blockNum == math.MaxUint64 {
		bcinfo, err := s.GetBlockchainInfo()
		if err != nil {
			return nil, errors.WithMessage(err, "retrieval of blockchain info failed")
		}
		blockNum = bcinfo.Height - 1
	}

	id := blockNumberToKey(blockNum)

	doc, _, err := s.db.ReadDoc(id)
	if err != nil {
		return nil, errors.WithMessage(err, fmt.Sprintf("retrieval of block from couchDB failed [%d]", blockNum))
	}
	if doc == nil {
		return nil, errors.Errorf("block does not exist [%d]", blockNum)
	}

	block, err := couchDocToBlock(doc)
	if err != nil {
		return nil, errors.WithMessage(err, fmt.Sprintf("unmarshal of block from couchDB failed [%d]", blockNum))
	}

	return block, nil
}

// RetrieveTxByID returns a transaction for given transaction id
func (s *cdbBlockStore) RetrieveTxByID(txID string) (*common.Envelope, error) {
	block, err := s.RetrieveBlockByTxID(txID)
	if err != nil {
		return nil, err
	}

	return extractTxnEnvelopeFromBlock(block, txID)
}

// RetrieveTxByBlockNumTranNum returns a transaction for given block ID and transaction ID
func (s *cdbBlockStore) RetrieveTxByBlockNumTranNum(blockNum uint64, tranNum uint64) (*common.Envelope, error) {
	block, err := s.RetrieveBlockByNumber(blockNum)
	if err != nil {
		return nil, err
	}

	txID := strconv.FormatUint(tranNum, 10)
	return extractTxnEnvelopeFromBlock(block, txID)
}

// RetrieveBlockByTxID returns a block for a given transaction ID
func (s *cdbBlockStore) RetrieveBlockByTxID(txID string) (*common.Block, error) {
	// TODO: array index (need to enable full text search in CouchDB build)
	const queryFmt = `
	{
		"selector": {
			"` + blockTxnsField + `": {
				"$elemMatch": {
					"` + blockTxnIDField + `": "%s"
				}
			}
		}
	}`

	block, err := retrieveBlockQuery(s.db, fmt.Sprintf(queryFmt, txID))
	if err == blkstorage.ErrNotFoundInIndex {
		return nil, err
	}
	if err != nil {
		return nil, errors.WithMessage(err, fmt.Sprintf("retrieval of block by transaction ID failed [%s]", txID))
	}
	return block, nil
}

// RetrieveTxValidationCodeByTxID returns a TX validation code for a given transaction ID
func (s *cdbBlockStore) RetrieveTxValidationCodeByTxID(txID string) (peer.TxValidationCode, error) {
	return peer.TxValidationCode_INVALID_ENDORSER_TRANSACTION, errors.New("not implemented")
}

// Shutdown closes the storage instance
func (s *cdbBlockStore) Shutdown() {
}

func (s *cdbBlockStore) updateBlockchainInfo(latestBlockHash []byte, latestBlock *common.Block) {
	currentBCInfo, err := s.GetBlockchainInfo()
	if err != nil {
		logger.Errorf("getblockchainInfo return error %s", err)
		return
	}
	newBCInfo := &common.BlockchainInfo{
		Height:            currentBCInfo.Height + 1,
		CurrentBlockHash:  latestBlockHash,
		PreviousBlockHash: latestBlock.Header.PreviousHash}
	if latestBlock.Header.Number != currentBCInfo.Height {
		logger.Warningf("latestBlock block height %d not equal to currentBCInfo block height %d", latestBlock.Header.Number, currentBCInfo.Height)
	}
	s.bcInfo.Store(newBCInfo)
}

//func (s *cdbBlockStore) updateCheckpoint(cpInfo *checkpointInfo) {
//	s.cpInfoCond.L.Lock()
//	defer s.cpInfoCond.L.Unlock()
//	logger.Debugf("Broadcasting about update checkpointInfo: %s", cpInfo)
//	s.cpInfoCond.Broadcast()
//}
