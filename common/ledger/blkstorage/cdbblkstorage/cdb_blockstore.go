/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbblkstorage

import (
	"context"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"sync"

	"github.com/pkg/errors"
	
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
	cpInfoSig  chan struct{}
	cpInfoMtx  *sync.RWMutex
	cpInfo     *checkpointInfo
	cp         *checkpoint
}
// newCDBBlockStore constructs block store based on CouchDB
func newCDBBlockStore(blockStore *couchdb.CouchDatabase, ledgerID string) *cdbBlockStore {
	cp := newCheckpoint(blockStore)

	cdbBlockStore := &cdbBlockStore{
		blockStore: blockStore,
		ledgerID:   ledgerID,
		cpInfoSig:  make(chan struct {}),
		cpInfoMtx:  &sync.RWMutex{},
		cp:         cp,
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

	return cdbBlockStore
}

// AddBlock adds a new block
func (s *cdbBlockStore) AddBlock(block *common.Block) error {
	if !ledgerconfig.IsCommitter() {
		// Nothing else to do if not a committer
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

	doc, _, err := s.blockStore.ReadDoc(id)
	if err != nil {
		return nil, errors.WithMessage(err, fmt.Sprintf("retrieval of block [%d] from couchDB [%s] failed", blockNum, s.ledgerID))
	}
	if doc == nil {
		return nil, blkstorage.ErrNotFoundInIndex
	}

	block, err := couchDocToBlock(doc)
	if err != nil {
		return nil, errors.WithMessage(err, fmt.Sprintf("unmarshal of block [%d] from couchDB [%s] failed", blockNum, s.ledgerID))
	}

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

	return block, nil
}

// RetrieveTxValidationCodeByTxID returns a TX validation code for a given transaction ID
func (s *cdbBlockStore) RetrieveTxValidationCodeByTxID(txID string) (peer.TxValidationCode, error) {
	block, err := s.RetrieveBlockByTxID(txID)
	if err != nil {
		return peer.TxValidationCode_INVALID_OTHER_REASON, err
	}

	// The transaction is still not in the cache - try to extract pos from the block itself (should be rare).
	pos, err := extractTxnBlockPos(block, txID)
	if err != nil {
		return peer.TxValidationCode_INVALID_OTHER_REASON, err
	}

	return extractTxnValidationCode(block, pos), nil
}

func extractTxnValidationCode(block *common.Block, txnPos int) peer.TxValidationCode {
	blockMetadata := block.GetMetadata()
	txValidationFlags := ledgerUtil.TxValidationFlags(blockMetadata.GetMetadata()[common.BlockMetadataIndex_TRANSACTIONS_FILTER])
	return txValidationFlags.Flag(txnPos)
}

func extractTxnBlockPos(block *common.Block, txnID string) (int, error) {
	blockData := block.GetData()

	for i, txEnvelopeBytes := range blockData.GetData() {
		envelope, err := utils.GetEnvelopeFromBlock(txEnvelopeBytes)
		if err != nil {
			return 0, err
		}

		iTxnID, err := extractTxIDFromEnvelope(envelope)
		if err != nil {
			return 0, errors.WithMessage(err, "transaction ID could not be extracted")
		}

		if iTxnID == txnID {
			return i, nil
		}
	}

	return 0, errors.New("transaction was not found in block")
}

// Shutdown closes the storage instance
func (s *cdbBlockStore) Shutdown() {
}

func (s *cdbBlockStore) updateCheckpoint(cpInfo *checkpointInfo) {
	s.cpInfoMtx.Lock()
	defer s.cpInfoMtx.Unlock()
	s.cpInfo = cpInfo
	logger.Debugf("broadcasting checkpoint update to waiting listeners [%#v]", s.cpInfo)
	close(s.cpInfoSig)
	s.cpInfoSig = make(chan struct{})
}

func (s *cdbBlockStore) LastBlockNumber() uint64 {
	s.cpInfoMtx.RLock()
	defer s.cpInfoMtx.RUnlock()

	return s.cpInfo.lastBlockNumber
}

func (s *cdbBlockStore) WaitForBlock(ctx context.Context, blockNum uint64) uint64 {
	var lastBlockNumber uint64

	BlockLoop:
	for {
		s.cpInfoMtx.RLock()
		lastBlockNumber = s.cpInfo.lastBlockNumber
		sigCh := s.cpInfoSig
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