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

	"github.com/pkg/errors"

	"sync"

	"github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	ledgerUtil "github.com/hyperledger/fabric/core/ledger/util"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
	"github.com/hyperledger/fabric/protos/utils"
)

type cdbBlockStore struct {
	blockStore *couchdb.CouchDatabase
	ledgerID   string
	cpInfo     *checkpointInfo
	cpInfoCond *sync.Cond
	cp         *checkpoint
	cache      *blockCache
}
// newCDBBlockStore constructs block store based on CouchDB
func newCDBBlockStore(blockStore *couchdb.CouchDatabase, ledgerID string, indexConfig *blkstorage.IndexConfig) *cdbBlockStore {
	cp := newCheckpoint(blockStore)

	blockCache := newBlockCache()

	cdbBlockStore := &cdbBlockStore{
		blockStore: blockStore,
		ledgerID:   ledgerID,
		cp:         cp,
		cache: blockCache,
	}

	// cp = checkpointInfo, retrieve from the database the last block number that was written to that db.
	cpInfo, err := cdbBlockStore.cp.getCheckpointInfo()
	if err != nil {
		panic(fmt.Sprintf("Could not get block file info for current block file from db: %s", err))
	}
	if ledgerconfig.IsCommitter() {
		err = cdbBlockStore.cp.saveCurrentInfo(cpInfo)
		if err != nil {
			panic(fmt.Sprintf("Could not save cpInfo info to db: %s", err))
		}
	}
	// Update the manager with the checkpoint info and the file writer
	cdbBlockStore.cpInfo = cpInfo

	// Create a checkpoint condition (event) variable, for the  goroutine waiting for
	// or announcing the occurrence of an event.
	cdbBlockStore.cpInfoCond = sync.NewCond(&sync.Mutex{})

	return cdbBlockStore
}

// AddBlock adds a new block
func (s *cdbBlockStore) AddBlock(block *common.Block) error {
	s.cache.Add(block)

	if !ledgerconfig.IsCommitter() {
		// Nothing to do if not a committer
		return nil
	}

	logger.Debugf("Storing block %d", block.Header.Number)
	err := s.storeBlock(block)
	if err != nil {
		return err
	}
	return nil
}

func (s *cdbBlockStore) storeBlock(block *common.Block) error {
	doc, err := blockToCouchDoc(block)
	if err != nil {
		return errors.WithMessage(err, "converting block to couchDB document failed")
	}

	id := blockNumberToKey(block.GetHeader().GetNumber())
	rev, err := s.blockStore.UpdateDoc(id, "", doc)
	if err != nil {
		return errors.WithMessage(err, "adding block to couchDB failed")
	}
	logger.Debugf("block added to couchDB [%d, %s]", block.GetHeader().GetNumber(), rev)
	return nil
}

func (s *cdbBlockStore) CheckpointBlock(block *common.Block) error {
	logger.Debugf("[%s] Updating checkpoint for block [%d]", s.ledgerID, block.Header.Number)

	//Update the checkpoint info with the results of adding the new block
	newCPInfo := &checkpointInfo{
		isChainEmpty:    false,
		lastBlockNumber: block.Header.Number}

	if ledgerconfig.IsCommitter() {
		logger.Debugf("Saving checkpoint info for block %d", block.Header.Number)
		//save the checkpoint information in the database
		err := s.cp.saveCurrentInfo(newCPInfo)
		if err != nil {
			return errors.WithMessage(err, "adding cpInfo to couchDB failed")
		}
	} else {
		logger.Debugf("Not saving checkpoint info for block %d since I'm not a committer. Just publishing the block.", block.Header.Number)
	}

	//update the checkpoint info (for storage) and the blockchain info (for APIs) in the manager
	s.updateCheckpoint(newCPInfo)
	return nil
}

// GetBlockchainInfo returns the current info about blockchain
func (s *cdbBlockStore) GetBlockchainInfo() (*common.BlockchainInfo, error) {
	cpInfo, err := s.cp.getCheckpointInfo()
	if err != nil {
		return nil, err
	}

	s.cpInfo = cpInfo
	bcInfo := &common.BlockchainInfo{
		Height: 0,
	}
	if !cpInfo.isChainEmpty {
		//If start up is a restart of an existing storage, update BlockchainInfo for external API's
		lastBlock, err := s.RetrieveBlockByNumber(cpInfo.lastBlockNumber)
		if err != nil {
			return nil, fmt.Errorf("RetrieveBlockByNumber return error: %s", err)
		}

		lastBlockHeader := lastBlock.GetHeader()
		lastBlockHash := lastBlockHeader.Hash()
		previousBlockHash := lastBlockHeader.GetPreviousHash()
		bcInfo = &common.BlockchainInfo{
			Height:            lastBlockHeader.GetNumber() + 1,
			CurrentBlockHash:  lastBlockHash,
			PreviousBlockHash: previousBlockHash,
		}
	}
	return bcInfo, nil
}

// RetrieveBlocks returns an iterator that can be used for iterating over a range of blocks
func (s *cdbBlockStore) RetrieveBlocks(startNum uint64) (ledger.ResultsIterator, error) {
	return newBlockItr(s, startNum), nil
}

// RetrieveBlockByHash returns the block for given block-hash
func (s *cdbBlockStore) RetrieveBlockByHash(blockHash []byte) (*common.Block, error) {
	block, ok := s.cache.LookupByHash(blockHash)
	if ok {
		return block, nil
	}

	blockHashHex := hex.EncodeToString(blockHash)
	const queryFmt = `
	{
		"selector": {
			"` + blockHeaderField + `.` + blockHashField + `": {
				"$eq": "%s"
			}
		},
		"use_index": ["_design/` + blockHashIndexDoc + `", "` + blockHashIndexName + `"]
	}`
	block, err := retrieveBlockQuery(s.blockStore, fmt.Sprintf(queryFmt, blockHashHex))
	if err != nil {
		// note: allow ErrNotFoundInIndex to pass through
		return nil, err
	}

	s.cache.Add(block)

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

	block, ok := s.cache.LookupByNumber(blockNum)
	if ok {
		return block, nil
	}

	id := blockNumberToKey(blockNum)

	doc, _, err := s.blockStore.ReadDoc(id)
	if err != nil {
		return nil, errors.WithMessage(err, fmt.Sprintf("retrieval of block [%d] from couchDB [%s] failed", blockNum, s.ledgerID))
	}
	if doc == nil {
		return nil, blkstorage.ErrNotFoundInIndex
	}

	block, err = couchDocToBlock(doc)
	if err != nil {
		return nil, errors.WithMessage(err, fmt.Sprintf("unmarshal of block [%d] from couchDB [%s] failed", blockNum, s.ledgerID))
	}

	s.cache.Add(block)

	return block, nil
}

// RetrieveTxByID returns a transaction for given transaction id
func (s *cdbBlockStore) RetrieveTxByID(txID string) (*common.Envelope, error) {
	block, err := s.RetrieveBlockByTxID(txID)
	if err != nil {
		// note: allow ErrNotFoundInIndex to pass through
		return nil, err
	}

	return extractTxnEnvelopeFromBlock(block, txID)
}

// RetrieveTxByBlockNumTranNum returns a transaction for given block ID and transaction ID
func (s *cdbBlockStore) RetrieveTxByBlockNumTranNum(blockNum uint64, tranNum uint64) (*common.Envelope, error) {
	block, err := s.RetrieveBlockByNumber(blockNum)
	if err != nil {
		// note: allow ErrNotFoundInIndex to pass through
		return nil, err
	}

	txID := strconv.FormatUint(tranNum, 10)
	return extractTxnEnvelopeFromBlock(block, txID)
}

// RetrieveBlockByTxID returns a block for a given transaction ID
func (s *cdbBlockStore) RetrieveBlockByTxID(txID string) (*common.Block, error) {
	block, ok := s.cache.LookupByTxnID(txID)
	if ok {
		return block, nil
	}

	const queryFmt = `
	{
		"selector": {
			"` + blockTxnIDsField + `": {
				"$elemMatch": {
					"$eq": "%s"
				}
			}
		},
		"use_index": ["_design/` + blockTxnIndexDoc + `", "` + blockTxnIndexName + `"]
	}`
	block, err := retrieveBlockQuery(s.blockStore, fmt.Sprintf(queryFmt, txID))
	if err != nil {
		// note: allow ErrNotFoundInIndex to pass through
		return nil, err
	}
	s.cache.Add(block)

	code, err := s.RetrieveTxValidationCodeByTxID(txID)
	logger.Errorf("*** block: %#v", block)
	logger.Errorf("*** code: %d %s", code, err)

	return block, nil
}

// RetrieveTxValidationCodeByTxID returns a TX validation code for a given transaction ID
func (s *cdbBlockStore) RetrieveTxValidationCodeByTxID(txID string) (peer.TxValidationCode, error) {
	block, err := s.RetrieveBlockByTxID(txID)
	if err != nil {
		return peer.TxValidationCode_INVALID_OTHER_REASON, err
	}

	return extractTxnValidationCode(block, txID)
}

func extractTxnValidationCode(block *common.Block, txnID string) (peer.TxValidationCode, error) {
	blockData := block.GetData()
	blockMetadata := block.GetMetadata()
	txValidationFlags := ledgerUtil.TxValidationFlags(blockMetadata.GetMetadata()[common.BlockMetadataIndex_TRANSACTIONS_FILTER])

	for i, txEnvelopeBytes := range blockData.GetData() {
		envelope, err := utils.GetEnvelopeFromBlock(txEnvelopeBytes)
		if err != nil {
			return peer.TxValidationCode_INVALID_OTHER_REASON, err
		}

		iTxnID, err := extractTxIDFromEnvelope(envelope)
		if err != nil {
			return peer.TxValidationCode_INVALID_OTHER_REASON, errors.WithMessage(err, "transaction ID could not be extracted")
		}

		if iTxnID == txnID {
			return txValidationFlags.Flag(i), nil
		}
	}

	return peer.TxValidationCode_INVALID_OTHER_REASON, errors.New("transaction was not found in block")
}

// Shutdown closes the storage instance
func (s *cdbBlockStore) Shutdown() {
}

func (s *cdbBlockStore) updateCheckpoint(cpInfo *checkpointInfo) {
	s.cpInfoCond.L.Lock()
	defer s.cpInfoCond.L.Unlock()
	s.cpInfo = cpInfo
	logger.Debugf("Broadcasting checkpointInfo: %s", s.cpInfo)
	s.cpInfoCond.Broadcast()
}
