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
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"

	"github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/common/metrics"
	cledger "github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	ledgerUtil "github.com/hyperledger/fabric/core/ledger/util"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
	"github.com/hyperledger/fabric/protos/utils"
)

type cdbBlock struct {
	ID  string
	Doc *couchdb.CouchDoc
}

type cdbBlockStore struct {
	blockStore        *couchdb.CouchDatabase
	ledgerID          string
	cpInfo            checkpointInfo
	pendingBlock      cdbBlock
	cpInfoSig         chan struct{}
	cpInfoMtx         *sync.RWMutex
	bcInfo            atomic.Value
	blockIndexEnabled bool
}

// newCDBBlockStore constructs block store based on CouchDB
func newCDBBlockStore(blockStore *couchdb.CouchDatabase, ledgerID string, blockIndexEnabled bool) *cdbBlockStore {
	cdbBlkStore := &cdbBlockStore{
		blockStore:        blockStore,
		ledgerID:          ledgerID,
		cpInfoSig:         make(chan struct{}),
		cpInfoMtx:         &sync.RWMutex{},
		blockIndexEnabled: blockIndexEnabled,
	}

	// cp = checkpointInfo, retrieve from the database the last block number that was written to that db.
	cpInfo, err := retrieveCheckpointInfo(blockStore)
	if err != nil {
		panic(fmt.Sprintf("Could not get block info from db: %s", err))
	}

	bi, err := createBlockchainInfo(blockStore, &cpInfo)
	if err != nil {
		panic(fmt.Sprintf("Unable to retrieve blockchain info from DB: %s", err))
	}
	cdbBlkStore.bcInfo.Store(bi)

	// Update the manager with the checkpoint info and the file writer
	cdbBlkStore.cpInfo = cpInfo

	return cdbBlkStore
}

// AddBlock adds a new block
func (s *cdbBlockStore) AddBlock(block *common.Block) error {
	if !ledgerconfig.IsCommitter() {
		// Nothing else to do if not a committer
		return nil
	}

	if metrics.IsDebug() {
		// Measure the whole
		stopWatch := metrics.RootScope.Timer("blkstorage_couchdb_addBlock_time").Start()
		defer stopWatch.Stop()
	}

	logger.Debugf("Preparing block for storage %d", block.Header.Number)
	pendingDoc, err := blockToCouchDoc(block)
	if err != nil {
		return errors.WithMessage(err, "converting block to couchDB document failed")
	}

	s.pendingBlock.ID = blockNumberToKey(block.GetHeader().GetNumber())
	s.pendingBlock.Doc = pendingDoc

	return nil
}

func (s *cdbBlockStore) CheckpointBlock(block *common.Block) error {
	logger.Debugf("[%s] Updating checkpoint for block [%d]", s.ledgerID, block.Header.Number)

	if metrics.IsDebug() {
		// Measure the whole
		stopWatch := metrics.RootScope.Timer("blkstorage_couchdb_checkpointBlock_time").Start()
		defer stopWatch.Stop()
	}

	if ledgerconfig.IsCommitter() {
		//save the checkpoint information in the database
		rev, err := s.blockStore.UpdateDoc(s.pendingBlock.ID, "", s.pendingBlock.Doc)
		if err != nil {
			return errors.WithMessage(err, "adding block to couchDB failed")
		}

		dbResponse, err := s.blockStore.EnsureFullCommit()
		if err != nil || dbResponse.Ok != true {
			logger.Errorf("full commit failed [%s]", err)
			return errors.WithMessage(err, "full commit failed")
		}

		logger.Debugf("block stored to couchDB [%d, %s]", block.GetHeader().GetNumber(), rev)
	} else {
		logger.Debugf("Not saving checkpoint info for block %d since I'm not a committer. Just publishing the block.", block.Header.Number)
	}

	curBcInfo := s.bcInfo.Load().(*common.BlockchainInfo)
	newBcInfo := updateBlockchainInfo(curBcInfo, block)
	s.bcInfo.Store(newBcInfo)

	//update the checkpoint info (for storage) and the blockchain info (for APIs) in the manager
	newCPInfo := checkpointInfo{
		isChainEmpty:    false,
		lastBlockNumber: block.Header.Number}
	s.updateCheckpoint(newCPInfo)

	return nil
}

// GetBlockchainInfo returns the current info about blockchain
func (s *cdbBlockStore) GetBlockchainInfo() (*common.BlockchainInfo, error) {
	if metrics.IsDebug() {
		// Measure the whole
		stopWatch := metrics.RootScope.Timer("blkstorage_couchdb_getBlockchainInfo_time").Start()
		defer stopWatch.Stop()
	}
	return s.bcInfo.Load().(*common.BlockchainInfo), nil
}

// RetrieveBlocks returns an iterator that can be used for iterating over a range of blocks
func (s *cdbBlockStore) RetrieveBlocks(startNum uint64) (ledger.ResultsIterator, error) {
	if metrics.IsDebug() {
		// Measure the whole
		stopWatch := metrics.RootScope.Timer("blkstorage_couchdb_retrieveBlocks_time").Start()
		defer stopWatch.Stop()
	}
	return newBlockItr(s, startNum), nil
}

// RetrieveBlockByHash returns the block for given block-hash
func (s *cdbBlockStore) RetrieveBlockByHash(blockHash []byte) (*common.Block, error) {
	if metrics.IsDebug() {
		// Measure the whole
		stopWatch := metrics.RootScope.Timer("blkstorage_couchdb_retrieveBlockByHash_time").Start()
		defer stopWatch.Stop()
	}
	blockHashHex := hex.EncodeToString(blockHash)
	const queryFmt = `
	{
		"selector": {
			"` + blockHeaderField + `.` + blockHashField + `": {
				"$eq": "%s"
			}
		}%s
	}`

	addHashIndex := ""
	if s.blockIndexEnabled {
		addHashIndex += `,
		"use_index": ["_design/` + blockHashIndexDoc + `", "` + blockHashIndexName + `"]`
	}

	block, err := retrieveBlockQuery(s.blockStore, fmt.Sprintf(queryFmt, blockHashHex, addHashIndex))
	if err != nil {
		// note: allow ErrNotFoundInIndex to pass through
		return nil, err
	}

	return block, nil
}

// RetrieveBlockByNumber returns the block at a given blockchain height
func (s *cdbBlockStore) RetrieveBlockByNumber(blockNum uint64) (*common.Block, error) {
	if metrics.IsDebug() {
		// Measure the whole
		stopWatch := metrics.RootScope.Timer("blkstorage_couchdb_retrieveBlockByNumber_time").Start()
		defer stopWatch.Stop()
	}

	// interpret math.MaxUint64 as a request for last block
	if blockNum == math.MaxUint64 {
		bcinfo, err := s.GetBlockchainInfo()
		if err != nil {
			return nil, errors.WithMessage(err, "retrieval of blockchain info failed")
		}
		blockNum = bcinfo.Height - 1
	}

	block, err := retrieveBlockByNumber(s.blockStore, blockNum)
	if err != nil {
		return nil, errors.WithMessage(err, fmt.Sprintf("retrieval of block [%d] from couchDB [%s] failed", blockNum, s.ledgerID))
	}
	return block, nil
}

func retrieveBlockByNumber(blockStore *couchdb.CouchDatabase, blockNum uint64) (*common.Block, error) {
	id := blockNumberToKey(blockNum)

	doc, _, err := blockStore.ReadDoc(id)
	if err != nil {
		return nil, err
	}
	if doc == nil {
		return nil, blkstorage.ErrNotFoundInIndex
	}

	block, err := couchDocToBlock(doc)
	if err != nil {
		return nil, err
	}

	return block, nil
}

// RetrieveTxByID returns a transaction for given transaction id
func (s *cdbBlockStore) RetrieveTxByID(txID string, _ ...cledger.SearchHint) (*common.Envelope, error) {
	if metrics.IsDebug() {
		// Measure the whole
		stopWatch := metrics.RootScope.Timer("blkstorage_couchdb_retrieveTxByID_time").Start()
		defer stopWatch.Stop()
	}

	block, err := s.RetrieveBlockByTxID(txID)
	if err != nil {
		// note: allow ErrNotFoundInIndex to pass through
		return nil, err
	}

	return extractTxnEnvelopeFromBlock(block, txID)
}

// RetrieveTxByBlockNumTranNum returns a transaction for given block number and transaction number
func (s *cdbBlockStore) RetrieveTxByBlockNumTranNum(blockNum uint64, tranNum uint64) (*common.Envelope, error) {
	if metrics.IsDebug() {
		// Measure the whole
		stopWatch := metrics.RootScope.Timer("blkstorage_couchdb_retrieveTxByBlockNumTranNum_time").Start()
		defer stopWatch.Stop()
	}

	block, err := s.RetrieveBlockByNumber(blockNum)
	if err != nil {
		// note: allow ErrNotFoundInIndex to pass through
		return nil, err
	}

	return extractEnvelopeFromBlock(block, tranNum)
}

func extractEnvelopeFromBlock(block *common.Block, tranNum uint64) (*common.Envelope, error) {
	blockData := block.GetData()
	envelopes := blockData.GetData()
	envelopesLen := uint64(len(envelopes))
	if envelopesLen-1 < tranNum {
		blockNum := block.GetHeader().GetNumber()
		return nil, errors.Errorf("transaction number is invalid [%d, %d, %d]", blockNum, envelopesLen, tranNum)
	}
	return utils.GetEnvelopeFromBlock(envelopes[tranNum])
}

// RetrieveBlockByTxID returns a block for a given transaction ID
func (s *cdbBlockStore) RetrieveBlockByTxID(txID string) (*common.Block, error) {
	if metrics.IsDebug() {
		// Measure the whole
		stopWatch := metrics.RootScope.Timer("blkstorage_couchdb_retrieveBlockByTxID_time").Start()
		defer stopWatch.Stop()
	}
	const queryFmt = `
	{
		"selector": {
			"` + blockTxnIDsField + `": {
				"$elemMatch": {
					"$eq": "%s"
				}
			}
		}%s
	}`

	addTxnIndex := ""
	if s.blockIndexEnabled {
		addTxnIndex += `,
		"use_index": ["_design/` + blockTxnIndexDoc + `", "` + blockTxnIndexName + `"]`
	}

	block, err := retrieveBlockQuery(s.blockStore, fmt.Sprintf(queryFmt, txID, addTxnIndex))
	if err != nil {
		// note: allow ErrNotFoundInIndex to pass through
		return nil, err
	}

	return block, nil
}

// RetrieveTxValidationCodeByTxID returns a TX validation code for a given transaction ID
func (s *cdbBlockStore) RetrieveTxValidationCodeByTxID(txID string) (peer.TxValidationCode, error) {
	if metrics.IsDebug() {
		// Measure the whole
		stopWatch := metrics.RootScope.Timer("blkstorage_couchdb_retrieveTxValidationCodeByTxID_time").Start()
		defer stopWatch.Stop()
	}
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

func (s *cdbBlockStore) updateCheckpoint(cpInfo checkpointInfo) {
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
