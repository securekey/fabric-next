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
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/utils"
)

type blockCache struct {
	blocks          *lru.Cache
	pinnedBlocks    map[uint64]*common.Block
	hashToNumber    map[string]uint64
	numberToHash    map[uint64]string
	txnLocs         map[string]*txnLoc
	numberToTxnIDs  map[uint64][]string
	configBlockNum  uint64
	mtx             sync.RWMutex
}

func newBlockCache(blockCacheSize int) *blockCache {
	blocks := lru.New(blockCacheSize)
	pinnedBlocks := make(map[uint64]*common.Block)
	hashToNumber := make(map[string]uint64)
	numberToHash := make(map[uint64]string)
	txns := make(map[string]*txnLoc)
	numberToTxnIDs := make(map[uint64][]string)
	configBlockNum := uint64(0)
	mtx := sync.RWMutex{}

	c := blockCache{
		blocks,
		pinnedBlocks,
		hashToNumber,
		numberToHash,
		txns,
		numberToTxnIDs,
		configBlockNum,
		mtx,
	}

	c.blocks.OnEvicted = c.onEvicted

	return &c
}

func (c *blockCache) AddBlock(block *common.Block) error {
	blockHashHex := hex.EncodeToString(block.GetHeader().Hash())
	blockNumber := block.GetHeader().GetNumber()

	blockTxns, err := createTxnLocsFromBlock(block)

	c.mtx.Lock()
	defer c.mtx.Unlock()

	if err != nil {
		logger.Warningf("extracting transaction IDs from block failed [%s]", err)
	} else {
		var txnIDs []string
		for id, txn := range blockTxns {
			c.txnLocs[id] = txn
			txnIDs = append(txnIDs, id)
		}
		c.numberToTxnIDs[blockNumber] = txnIDs
	}

	c.hashToNumber[blockHashHex] = blockNumber
	c.numberToHash[blockNumber] = blockHashHex
	c.pinnedBlocks[blockNumber] = block

	return nil
}

func (c *blockCache) OnBlockStored(blockNum uint64) bool {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	b, ok := c.pinnedBlocks[blockNum]
	if !ok {
		return false
	}

	if blockNum == 0 {
		return c.onGenesisBlockStored(b)
	}

	if utils.IsConfigBlock(b)  {
		return c.onConfigBlockStored(b)
	}

	return c.onBlockStored(b)
}

func (c *blockCache) onGenesisBlockStored(block *common.Block) bool {
	// Keep the genesis block in memory (by leaving in pinnedBlocks)
	// TODO: Determine if this is really needed.
	return true
}

func (c *blockCache) onConfigBlockStored(block *common.Block) bool {
	if c.configBlockNum == 0 {
		// Keep the latest config block in memory (by leaving in pinnedBlocks)
		c.configBlockNum = block.GetHeader().GetNumber()

		return true
	}

	// Unpin the previous config block
	oldBlock, ok := c.pinnedBlocks[c.configBlockNum]
	if !ok {
		return false
	}

	c.configBlockNum = block.GetHeader().GetNumber()

	return c.onBlockStored(oldBlock)
}

func (c *blockCache) onBlockStored(block *common.Block) bool {
	blockNum := block.GetHeader().GetNumber()

	delete(c.pinnedBlocks, blockNum)
	c.blocks.Add(blockNum, block)
	return true
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
	b, ok := c.pinnedBlocks[number]
	if !ok {
		b, ok = c.lookupBlockFromLRU(number)
	}
	c.mtx.RUnlock()
	if !ok {
		return nil, false
	}

	return b, true
}

func (c *blockCache) lookupBlockFromLRU(number uint64) (*common.Block, bool){
	b, ok := c.blocks.Get(number)
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