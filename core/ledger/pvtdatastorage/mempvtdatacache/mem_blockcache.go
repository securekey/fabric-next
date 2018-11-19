/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mempvtdatacache

import (
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/golang/groupcache/lru"

	"sync"

	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/core/ledger/pvtdatapolicy"
	"github.com/hyperledger/fabric/core/ledger/pvtdatastorage"
	"github.com/hyperledger/fabric/protos/ledger/rwset"
)

type pvtDataCache struct {
	pvtData            *lru.Cache
	pinnedPvtData      map[uint64]*map[string][]byte
	mtx                sync.RWMutex
	isEmpty            bool
	lastCommittedBlock uint64
	btlPolicy          pvtdatapolicy.BTLPolicy
	batchPending       bool
}
type dataEntry struct {
	key   *dataKey
	value *rwset.CollectionPvtReadWriteSet
}

type dataKey struct {
	blkNum   uint64
	txNum    uint64
	ns, coll string
}

type blkTranNumKey []byte

func newPvtDataCache(blockCacheSize int) *pvtDataCache {
	pvtData := lru.New(blockCacheSize)
	pinnedPvtData := make(map[uint64]*map[string][]byte)
	mtx := sync.RWMutex{}
	c := pvtDataCache{
		pvtData,
		pinnedPvtData,
		mtx,
		true,
		0,
		nil,
		false,
	}

	return &c
}
func (c *pvtDataCache) nextBlockNum() uint64 {
	if c.isEmpty {
		return 0
	}
	return c.lastCommittedBlock + 1
}

func (c *pvtDataCache) Init(btlPolicy pvtdatapolicy.BTLPolicy) {
	c.btlPolicy = btlPolicy
}

func (c *pvtDataCache) Prepare(blockNum uint64, pvtData []*ledger.TxPvtData) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if !ledgerconfig.IsCommitter() {
		panic("calling Prepare on a peer that is not a committer")
	}

	if c.batchPending {
		return pvtdatastorage.NewErrIllegalCall(`A pending batch exists as as result of last invoke to "Prepare" call.
			 Invoke "Commit" or "Rollback" on the pending batch before invoking "Prepare" function`)
	}

	if len(pvtData) > 0 {
		pvtDataEntries, err := preparePvtDataEntries(blockNum, pvtData)
		if err != nil {
			return err
		}
		c.pinnedPvtData[blockNum] = &pvtDataEntries
	}
	c.batchPending = true

	return nil
}

func (c *pvtDataCache) Commit() error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if !ledgerconfig.IsCommitter() {
		panic("calling Commit on a peer that is not a committer")
	}

	if !c.batchPending {
		return pvtdatastorage.NewErrIllegalCall("No pending batch to commit")
	}
	committingBlockNum := c.nextBlockNum()

	pvtData, ok := c.pinnedPvtData[committingBlockNum]
	if ok {
		delete(c.pinnedPvtData, committingBlockNum)
		c.pvtData.Add(committingBlockNum, pvtData)
	}
	c.batchPending = false
	c.isEmpty = false
	c.lastCommittedBlock = committingBlockNum
	logger.Debugf("Committed private data for block [%d]", committingBlockNum)
	return nil
}

func (c *pvtDataCache) InitLastCommittedBlock(blockNum uint64) error {
	if !(c.isEmpty && !c.batchPending) {
		return pvtdatastorage.NewErrIllegalCall("The private data store is not empty. InitLastCommittedBlock() function call is not allowed")
	}

	c.isEmpty = false
	c.lastCommittedBlock = blockNum
	logger.Debugf("InitLastCommittedBlock set to block [%d]", blockNum)
	return nil
}

func (c *pvtDataCache) GetPvtDataByBlockNum(blockNum uint64, filter ledger.PvtNsCollFilter) ([]*ledger.TxPvtData, error) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	logger.Debugf("Get private data for block [%d] from cache, filter=%#v", blockNum, filter)
	if c.isEmpty {
		return nil, pvtdatastorage.NewErrOutOfRange("The store is empty")
	}
	lastCommittedBlock := c.lastCommittedBlock
	if blockNum > lastCommittedBlock {
		logger.Debugf("Block %d is greater than last committed block %d in cache", blockNum, lastCommittedBlock)
		return nil, pvtdatastorage.NewErrOutOfRange(fmt.Sprintf("Last committed block=%d, block requested=%d", lastCommittedBlock, blockNum))
	}
	logger.Debugf("Querying private data storage for write sets using blockNum=%d in cache", blockNum)

	data, exist := c.pvtData.Get(blockNum)
	if !exist {
		return nil, nil
	}
	results := data.(map[string][]byte)
	logger.Debugf("Got private data results for block %d in cache: %#v", blockNum, results)

	var blockPvtdata []*ledger.TxPvtData
	var currentTxNum uint64
	var currentTxWsetAssember *txPvtdataAssembler
	firstItr := true

	var sortedKeys []string
	for key, _ := range results {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Strings(sortedKeys)

	for _, key := range sortedKeys {
		dataKeyBytes, err := hex.DecodeString(key)
		if err != nil {
			return nil, err
		}
		dataValueBytes := results[key]

		if v11Format(dataKeyBytes) {
			return v11RetrievePvtdata(results, filter)
		}
		dataKey := decodeDatakey(dataKeyBytes)
		expired, err := isExpired(dataKey, c.btlPolicy, lastCommittedBlock)
		if err != nil {
			return nil, err
		}
		if expired || !passesFilter(dataKey, filter) {
			continue
		}
		dataValue, err := decodeDataValue(dataValueBytes)
		if err != nil {
			return nil, err
		}

		if firstItr {
			currentTxNum = dataKey.txNum
			currentTxWsetAssember = newTxPvtdataAssembler(blockNum, currentTxNum)
			firstItr = false
		}

		if dataKey.txNum != currentTxNum {
			blockPvtdata = append(blockPvtdata, currentTxWsetAssember.getTxPvtdata())
			currentTxNum = dataKey.txNum
			currentTxWsetAssember = newTxPvtdataAssembler(blockNum, currentTxNum)
		}
		currentTxWsetAssember.add(dataKey.ns, dataValue)
	}
	if currentTxWsetAssember != nil {
		blockPvtdata = append(blockPvtdata, currentTxWsetAssember.getTxPvtdata())
	}

	logger.Debugf("Successfully retrieved private data for block %d in cache: %#v", blockNum, blockPvtdata)
	return blockPvtdata, nil

}

func (c *pvtDataCache) HasPendingBatch() (bool, error) {
	return c.batchPending, nil
}

func (c *pvtDataCache) LastCommittedBlockHeight() (uint64, error) {
	if c.isEmpty {
		return 0, nil
	}
	return c.lastCommittedBlock + 1, nil
}

func (c *pvtDataCache) IsEmpty() (bool, error) {
	return c.isEmpty, nil
}

// Rollback implements the function in the interface `Store`
func (c *pvtDataCache) Rollback() error {
	if !c.batchPending {
		return pvtdatastorage.NewErrIllegalCall("No pending batch to rollback")
	}
	delete(c.pinnedPvtData, c.lastCommittedBlock+1)
	c.batchPending = false
	return nil
}

// Shutdown closes the storage instance
func (c *pvtDataCache) Shutdown() {
}

func preparePvtDataEntries(blockNum uint64, pvtData []*ledger.TxPvtData) (map[string][]byte, error) {
	data := make(map[string][]byte)
	for _, txPvtdata := range pvtData {
		for _, nsPvtdata := range txPvtdata.WriteSet.NsPvtRwset {
			for _, collPvtdata := range nsPvtdata.CollectionPvtRwset {
				txnum := txPvtdata.SeqInBlock
				ns := nsPvtdata.Namespace
				coll := collPvtdata.CollectionName
				dataKey := &dataKey{blockNum, txnum, ns, coll}
				dataEntry := &dataEntry{key: dataKey, value: collPvtdata}
				keyBytes := encodeDataKey(dataEntry.key)
				valBytes, err := encodeDataValue(dataEntry.value)
				if err != nil {
					return nil, err
				}
				keyBytesHex := hex.EncodeToString(keyBytes)
				data[keyBytesHex] = valBytes
			}
		}
	}
	return data, nil
}
