/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbblkstorage

import (
	"encoding/hex"
	"sync"

	"github.com/golang/groupcache/lru"
	"github.com/pkg/errors"

	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/utils"
)

type blockCache struct {
	blocks         *lru.Cache
	hashToNumber   map[string]uint64
	numberToHash   map[uint64]string
	txnIDToNumber  map[string]uint64
	numberToTxnIDs map[uint64][]string
	mtx            sync.RWMutex
}

func newBlockCache() *blockCache {
	blockCacheSize := ledgerconfig.GetBlockCacheSize()

	blocks := lru.New(blockCacheSize)
	hashToNumber := make(map[string]uint64)
	numberToHash := make(map[uint64]string)
	txnIDToNumber := make(map[string]uint64)
	numberToTxnIDs := make(map[uint64][]string)
	mtx := sync.RWMutex{}

	c := blockCache{
		blocks,
		hashToNumber,
		numberToHash,
		txnIDToNumber,
		numberToTxnIDs,
		mtx,
	}

	c.blocks.OnEvicted = c.onEvicted

	return &c
}

func (c *blockCache) Add(block *common.Block) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	blockHashHex := hex.EncodeToString(block.GetHeader().Hash())
	blockNumber := block.GetHeader().GetNumber()

	c.hashToNumber[blockHashHex] = blockNumber
	c.numberToHash[blockNumber] = blockHashHex

	txnIDs, err := extractTxnIDsFromBlock(block)
	if err != nil {
		logger.Warningf("extracting transaction IDs from block failed [%s]", err)
	} else {
		for _, txnID := range txnIDs {
			logger.Warningf("adding txn id to cache [%s]", txnID)
			c.txnIDToNumber[txnID] = blockNumber
		}
		c.numberToTxnIDs[blockNumber] = txnIDs
	}

	c.blocks.Add(block.GetHeader().Number, block)
}

func extractTxnIDsFromBlock(block *common.Block) ([]string, error) {
	var txnIDs []string
	blockData := block.GetData()

	for _, txEnvelopeBytes := range blockData.GetData() {
		envelope, err := utils.GetEnvelopeFromBlock(txEnvelopeBytes)
		if err != nil {
			return nil, err
		}

		txnID, err := extractTxIDFromEnvelope(envelope)
		if err != nil {
			return nil, errors.WithMessage(err, "transaction ID could not be extracted")
		}

		if txnID != "" {
			txnIDs = append(txnIDs, txnID)
		}
	}

	return txnIDs, nil
}

func (c *blockCache) LookupByTxnID(id string) (*common.Block, bool) {
	c.mtx.RLock()
	blockNumber, ok := c.txnIDToNumber[id]
	c.mtx.RUnlock()
	if !ok {
		return nil, false
	}

	logger.Errorf("Cache hit (Txn ID) [%s]", id)
	return c.LookupByNumber(blockNumber)
}


func (c *blockCache) LookupByNumber(number uint64) (*common.Block, bool) {
	c.mtx.RLock()
	b, ok := c.blocks.Get(number)
	c.mtx.RUnlock()
	if !ok {
		return nil, false
	}

	return b.(*common.Block), true
}

func (c *blockCache) LookupByHash(blockHash []byte) (*common.Block, bool) {
	blockHashHex := hex.EncodeToString(blockHash)

	c.mtx.RLock()
	number, ok := c.hashToNumber[blockHashHex]
	c.mtx.RUnlock()
	if !ok {
		logger.Errorf("Cache miss (hash) [%s]", blockHashHex)
		return nil, false
	}

	logger.Errorf("Cache hit (hash) [%s]", blockHashHex)
	return c.LookupByNumber(number)
}

func (c *blockCache) onEvicted(key lru.Key, value interface{}) {
	blockNumber := key.(uint64)
	blockHashHex := c.numberToHash[blockNumber]

	delete(c.hashToNumber, blockHashHex)
	delete(c.numberToHash, blockNumber)

	txnIDs, ok := c.numberToTxnIDs[blockNumber]
	if ok {
		for _, txnID := range txnIDs {
			delete(c.txnIDToNumber, txnID)
		}
		delete(c.numberToTxnIDs, blockNumber)
	}
}