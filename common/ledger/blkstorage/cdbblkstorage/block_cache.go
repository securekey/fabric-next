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

type cachedTxn struct {
	blockNumber   uint64
	blockPosition int
}

type blockCache struct {
	blocks          *lru.Cache
	hashToNumber    map[string]uint64
	numberToHash    map[uint64]string
	txns            map[string]*cachedTxn
	numberToTxnIDs  map[uint64][]string
	mtx             sync.RWMutex
}

func newBlockCache() *blockCache {
	blockCacheSize := ledgerconfig.GetBlockCacheSize()

	blocks := lru.New(blockCacheSize)
	hashToNumber := make(map[string]uint64)
	numberToHash := make(map[uint64]string)
	txns := make(map[string]*cachedTxn)
	numberToTxnIDs := make(map[uint64][]string)
	mtx := sync.RWMutex{}

	c := blockCache{
		blocks,
		hashToNumber,
		numberToHash,
		txns,
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

	blockTxns, err := createCachedTxnsFromBlock(block)
	if err != nil {
		logger.Warningf("extracting transaction IDs from block failed [%s]", err)
	} else {
		var txnIDs []string
		for id, txn := range blockTxns {
			c.txns[id] = txn
			txnIDs = append(txnIDs)
		}
		c.numberToTxnIDs[blockNumber] = txnIDs
	}

	c.blocks.Add(block.GetHeader().Number, block)
}

func createCachedTxnsFromBlock(block *common.Block) (map[string]*cachedTxn, error) {
	txns := make(map[string]*cachedTxn)
	blockNumber := block.GetHeader().GetNumber()
	blockData := block.GetData()

	for i, txEnvelopeBytes := range blockData.GetData() {
		envelope, err := utils.GetEnvelopeFromBlock(txEnvelopeBytes)
		if err != nil {
			return nil, err
		}

		txnID, err := extractTxIDFromEnvelope(envelope)
		if err != nil {
			return nil, errors.WithMessage(err, "transaction ID could not be extracted")
		}

		if txnID != "" {
			cachedTxn := cachedTxn{blockNumber:blockNumber, blockPosition:i}
			txns[txnID] = &cachedTxn
		}
	}

	return txns, nil
}

func (c *blockCache) LookupByTxnID(id string) (*common.Block, bool) {
	c.mtx.RLock()
	cachedTxn, ok := c.txns[id]
	c.mtx.RUnlock()
	if !ok {
		return nil, false
	}

	return c.LookupByNumber(cachedTxn.blockNumber)
}

func (c *blockCache) LookupTxnBlockPosition(id string) (int, bool) {
	c.mtx.RLock()
	cachedTxn, ok := c.txns[id]
	c.mtx.RUnlock()
	if !ok {
		return 0, false
	}

	return cachedTxn.blockPosition, true
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
		return nil, false
	}

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
			delete(c.txns, txnID)
		}
		delete(c.numberToTxnIDs, blockNumber)
	}
}