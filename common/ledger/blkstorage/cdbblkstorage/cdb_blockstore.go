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
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
)

type cdbBlockStore struct {
	blockStore *couchdb.CouchDatabase
	txnStore   *couchdb.CouchDatabase
	ledgerID   string
	cpInfo     *checkpointInfo
	cpInfoCond *sync.Cond
	cp         *checkpoint
	attachTxn  bool
}

// newCDBBlockStore constructs block store based on CouchDB
func newCDBBlockStore(blockStore *couchdb.CouchDatabase, txnStore *couchdb.CouchDatabase, ledgerID string, indexConfig *blkstorage.IndexConfig) *cdbBlockStore {
	cp := newCheckpoint(blockStore)

	cdbBlockStore := &cdbBlockStore{
		blockStore: blockStore,
		txnStore:   txnStore,
		ledgerID:   ledgerID,
		cp:         cp,
		attachTxn:  ledgerconfig.GetBlockStorageAttachTxn(),
	}

	// cp = checkpointInfo, retrieve from the database the last block number that was written to that db.
	cpInfo := cdbBlockStore.cp.getCheckpointInfo()
	err := cdbBlockStore.cp.saveCurrentInfo(cpInfo)
	if err != nil {
		panic(fmt.Sprintf("Could not save cpInfo info to db: %s", err))
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
	err := s.storeBlock(block)
	if err != nil {
		return err
	}

	err = s.storeTransactions(block)
	if err != nil {
		return err
	}

	return s.checkpointBlock(block)
}

func (s *cdbBlockStore) storeBlock(block *common.Block) error {
	doc, err := blockToCouchDoc(block)
	if err != nil {
		return errors.WithMessage(err, "converting block to couchDB document failed")
	}

	id := blockNumberToKey(block.GetHeader().GetNumber())
	rev, err := s.blockStore.SaveDoc(id, "", doc)
	if err != nil {
		return errors.WithMessage(err, "adding block to couchDB failed")
	}
	logger.Debugf("block added to couchDB [%d, %s]", block.GetHeader().GetNumber(), rev)
	return nil
}

func (s *cdbBlockStore) storeTransactions(block *common.Block) error {
	docs, err := blockToTxnCouchDocs(block, s.attachTxn)
	if err != nil {
		return errors.WithMessage(err, "converting block to couchDB txn documents failed")
	}

	if len(docs) == 0 {
		return nil
	}

	_, err = s.txnStore.BatchUpdateDocuments(docs)
	if err != nil {
		return errors.WithMessage(err, "adding block to couchDB failed")
	}
	logger.Debugf("block transactions added to couchDB [%d]", block.GetHeader().GetNumber())
	return nil
}

func (s *cdbBlockStore) checkpointBlock(block *common.Block) error {
	//Update the checkpoint info with the results of adding the new block
	newCPInfo := &checkpointInfo{
		isChainEmpty:    false,
		lastBlockNumber: block.Header.Number}
	//save the checkpoint information in the database
	err := s.cp.saveCurrentInfo(newCPInfo)
	if err != nil {
		return errors.WithMessage(err, "adding cpInfo to couchDB failed")
	}
	//update the checkpoint info (for storage) and the blockchain info (for APIs) in the manager
	s.updateCheckpoint(newCPInfo)
	return nil
}

// GetBlockchainInfo returns the current info about blockchain
func (s *cdbBlockStore) GetBlockchainInfo() (*common.BlockchainInfo, error) {
	cpInfo := s.cp.getCheckpointInfo()
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
		return nil, errors.WithMessage(err, fmt.Sprintf("retrieval of block from couchDB failed [%d]", blockNum))
	}
	if doc == nil {
		return nil, blkstorage.ErrNotFoundInIndex
	}

	block, err := couchDocToBlock(doc)
	if err != nil {
		return nil, errors.WithMessage(err, fmt.Sprintf("unmarshal of block from couchDB failed [%d]", blockNum))
	}

	return block, nil
}

// RetrieveTxByID returns a transaction for given transaction id
func (s *cdbBlockStore) RetrieveTxByID(txID string) (*common.Envelope, error) {
	doc, _, err := s.txnStore.ReadDoc(txID)
	if err != nil {
		// note: allow ErrNotFoundInIndex to pass through
		return nil, err
	}
	if doc == nil {
		return nil, blkstorage.ErrNotFoundInIndex
	}

	// If this transaction includes the envelope as a valid attachment then can return immediately.
	if len(doc.Attachments) > 0 {
		attachedEnv, err := couchAttachmentsToTxnEnvelope(doc.Attachments)
		if err == nil {
			return attachedEnv, nil
		} else {
			logger.Debugf("transaction has attachment but failed to be extracted into envelope [%s]", err)
		}
	}

	// Otherwise, we need to extract the transaction from the block document.
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
	blockHash, err := s.retrieveBlockHashByTxID(txID)
	if err != nil {
		// note: allow ErrNotFoundInIndex to pass through
		return nil, err
	}

	return s.RetrieveBlockByHash(blockHash)
}

func (s *cdbBlockStore) retrieveBlockHashByTxID(txID string) ([]byte, error) {
	jsonResult, err := retrieveJSONQuery(s.txnStore, txID)
	if err != nil {
		// note: allow ErrNotFoundInIndex to pass through
		return nil, err
	}

	blockHashStoredUT, ok := jsonResult[txnBlockHashField]
	if !ok {
		return nil, errors.Errorf("block hash was not found for transaction ID [%s]", txID)
	}

	blockHashStored, ok := blockHashStoredUT.(string)
	if !ok {
		return nil, errors.Errorf("block hash has invalid type for transaction ID [%s]", txID)
	}

	blockHash, err := hex.DecodeString(blockHashStored)
	if err != nil {
		return nil, errors.Wrapf(err, "block hash was invalid for transaction ID [%s]", txID)
	}

	return blockHash, nil
}

// RetrieveTxValidationCodeByTxID returns a TX validation code for a given transaction ID
func (s *cdbBlockStore) RetrieveTxValidationCodeByTxID(txID string) (peer.TxValidationCode, error) {
	jsonResult, err := retrieveJSONQuery(s.txnStore, txID)
	if err != nil {
		// note: allow ErrNotFoundInIndex to pass through
		return peer.TxValidationCode_INVALID_OTHER_REASON, err
	}

	txnValidationCodeStoredUT, ok := jsonResult[txnValidationCode]
	if !ok {
		return peer.TxValidationCode_INVALID_OTHER_REASON, errors.Errorf("validation code was not found for transaction ID [%s]", txID)
	}

	txnValidationCodeStored, ok := txnValidationCodeStoredUT.(string)
	if !ok {
		return peer.TxValidationCode_INVALID_OTHER_REASON, errors.Errorf("validation code has invalid type for transaction ID [%s]", txID)
	}

	const sizeOfTxValidationCode = 32
	txnValidationCode, err := strconv.ParseInt(txnValidationCodeStored, txnValidationCodeBase, sizeOfTxValidationCode)
	if err != nil {
		return peer.TxValidationCode_INVALID_OTHER_REASON, errors.Wrapf(err, "validation code was invalid for transaction ID [%s]", txID)
	}

	return peer.TxValidationCode(txnValidationCode), nil
}

// Shutdown closes the storage instance
func (s *cdbBlockStore) Shutdown() {
}

func (s *cdbBlockStore) updateCheckpoint(cpInfo *checkpointInfo) {
	s.cpInfoCond.L.Lock()
	defer s.cpInfoCond.L.Unlock()
	s.cpInfo = cpInfo
	logger.Debugf("Broadcasting about update checkpointInfo: %s", cpInfo)
	s.cpInfoCond.Broadcast()
}
