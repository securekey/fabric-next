/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package memblkcache

import (
	"encoding/hex"
	"sync"

	"github.com/golang/groupcache/lru"
	"github.com/pkg/errors"

	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/utils"
)

type blockCache struct {
	blocks          *lru.Cache
	hashToNumber    map[string]uint64
	numberToHash    map[uint64]string
	txnLocs         map[string]*txnLoc
	numberToTxnIDs  map[uint64][]string
	mtx             sync.RWMutex
}

func newBlockCache() *blockCache {
	blockCacheSize := ledgerconfig.GetBlockCacheSize()

	blocks := lru.New(blockCacheSize)
	hashToNumber := make(map[string]uint64)
	numberToHash := make(map[uint64]string)
	txns := make(map[string]*txnLoc)
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

func (c *blockCache) AddBlock(block *common.Block) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	blockHashHex := hex.EncodeToString(block.GetHeader().Hash())
	blockNumber := block.GetHeader().GetNumber()

	c.hashToNumber[blockHashHex] = blockNumber
	c.numberToHash[blockNumber] = blockHashHex

	blockTxns, err := createTxnLocsFromBlock(block)
	if err != nil {
		logger.Warningf("extracting transaction IDs from block failed [%s]", err)
	} else {
		var txnIDs []string
		for id, txn := range blockTxns {
			c.txnLocs[id] = txn
			txnIDs = append(txnIDs)
		}
		c.numberToTxnIDs[blockNumber] = txnIDs
	}

	c.blocks.Add(block.GetHeader().Number, block)
	return nil
}

func createTxnLocsFromBlock(block *common.Block) (map[string]*txnLoc, error) {
	txns := make(map[string]*txnLoc)
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
			txnLoc := txnLoc{blockNumber:blockNumber, txNumber:uint64(i)}
			txns[txnID] = &txnLoc
		}
	}

	return txns, nil
}

func (c *blockCache) LookupBlockByNumber(number uint64) (*common.Block, bool) {
	c.mtx.RLock()
	b, ok := c.blocks.Get(number)
	c.mtx.RUnlock()
	if !ok {
		return nil, false
	}

	return b.(*common.Block), true
}

func (c *blockCache) LookupBlockByHash(blockHash []byte) (*common.Block, bool) {
	blockHashHex := hex.EncodeToString(blockHash)

	c.mtx.RLock()
	number, ok := c.hashToNumber[blockHashHex]
	c.mtx.RUnlock()
	if !ok {
		return nil, false
	}

	return c.LookupBlockByNumber(number)
}

func (c *blockCache) LookupTxLoc(id string) (blkstorage.TxLoc, bool) {
	c.mtx.RLock()
	txnLoc, ok := c.txnLocs[id]
	c.mtx.RUnlock()
	if !ok {
		return nil, false
	}

	return txnLoc, true
}

// Shutdown closes the storage instance
func (c *blockCache) Shutdown() {
}

func (c *blockCache) onEvicted(key lru.Key, value interface{}) {
	blockNumber := key.(uint64)
	blockHashHex := c.numberToHash[blockNumber]

	delete(c.hashToNumber, blockHashHex)
	delete(c.numberToHash, blockNumber)

	txnIDs, ok := c.numberToTxnIDs[blockNumber]
	if ok {
		for _, txnID := range txnIDs {
			delete(c.txnLocs, txnID)
		}
		delete(c.numberToTxnIDs, blockNumber)
	}
}

type txnLoc struct {
	blockNumber uint64
	txNumber    uint64
}

func (l *txnLoc) BlockNumber() uint64 {
	return l.blockNumber
}

func (l *txnLoc) TxNumber() uint64 {
	return l.txNumber
}

func extractTxIDFromEnvelope(txEnvelope *common.Envelope) (string, error) {
	payload, err := utils.GetPayload(txEnvelope)
	if err != nil {
		return "", nil
	}

	payloadHeader := payload.Header
	channelHeader, err := utils.UnmarshalChannelHeader(payloadHeader.ChannelHeader)
	if err != nil {
		return "", err
	}

	return channelHeader.TxId, nil
}