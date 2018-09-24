/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbblkstorage

import (
	"encoding/hex"
	"fmt"
	"github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
	"github.com/hyperledger/fabric/protos/utils"
	"github.com/pkg/errors"
	"math"
)

// cdbBlockStore ...
type cdbBlockStore struct {
	db *couchdb.CouchDatabase
	ledgerID string
}

// newCDBBlockStore constructs block store based on CouchDB
func newCDBBlockStore(db *couchdb.CouchDatabase, ledgerID string, indexConfig *blkstorage.IndexConfig) *cdbBlockStore {
	return &cdbBlockStore{db, ledgerID}
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
	logger.Debugf("block added to couchDB [%d, %s]", block.GetHeader().Number, rev)

	return nil
}

// GetBlockchainInfo returns the current info about blockchain
func (s *cdbBlockStore) GetBlockchainInfo() (*common.BlockchainInfo, error) {
	return nil, errors.New("not implemented")
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

	block, err := s.retrieveBlockQuery(fmt.Sprintf(queryFmt, blockHashHex))
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

	blockData := block.GetData()
	for _, txEnvelopeBytes := range blockData.GetData() {
		id, err := extractTxID(txEnvelopeBytes)
		if err != nil {
			return nil, err
		}
		if id != txID {
			continue
		}

		txEnvelope, err := utils.GetEnvelopeFromBlock(txEnvelopeBytes)
		if err != nil {
			return nil, err
		}

		return txEnvelope, nil
	}

	return nil, errors.Errorf("transaction not found [%s]", txID)
}

// RetrieveTxByBlockNumTranNum returns a transaction for given block ID and transaction ID
func (s *cdbBlockStore) RetrieveTxByBlockNumTranNum(blockNum uint64, tranNum uint64) (*common.Envelope, error) {
	return nil, errors.New("not implemented")
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

	block, err := s.retrieveBlockQuery(fmt.Sprintf(queryFmt, txID))
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

// RetrieveBlockByHash returns the block for given block-hash
func (s *cdbBlockStore) retrieveBlockQuery(query string) (*common.Block, error) {
	resultsP, err := s.db.QueryDocuments(query)
	if err != nil {
		return nil, err
	}
	results := *resultsP // remove unnecessary pointer (todo: should fix in source package)

	if len(results) != 1 {
		return nil, blkstorage.ErrNotFoundInIndex
	}

	if len(results[0].Attachments) == 0 {
		return nil, errors.New("block bytes not found")
	}

	return couchAttachmentsToBlock(results[0].Attachments)
}