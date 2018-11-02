/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cachedblkstore

import (
	"context"
	"encoding/hex"
	"github.com/hyperledger/fabric/common/ledger/blkstorage/memblkcache"
	"github.com/hyperledger/fabric/common/ledger/blkstorage/mocks"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric/protos/peer"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestAddBlock(t *testing.T) {
	cbs := newMockCachedBlockStore(t)

	const blockNumber = 0
	b := newMockBlock(blockNumber)

	err := cbs.AddBlock(b)
	assert.NoError(t, err, "block should have been added successfully")
	assert.Equal(t, b, cbs.blockStore.(*mockBlockStoreWithCheckpoint).LastBlockAdd, "block should have been added to store")
	assert.Equal(t, b, cbs.blockIndex.(*mocks.MockBlockIndex).LastBlockAdd, "block should have been added to index")

	cachedBlock, ok := cbs.blockCache.LookupBlockByNumber(blockNumber)
	assert.True(t, ok, "block should exist in cache")
	assert.Equal(t, b, cachedBlock, "block should have been added to cache")
}

func TestCheckpointBlock(t *testing.T) {
	cbs := newMockCachedBlockStore(t)

	const blockNumber = 0
	b := newMockBlock(blockNumber)

	err := cbs.CheckpointBlock(b)
	assert.NoError(t, err, "block should have been checkpointed successfully")
	assert.Equal(t, b, cbs.blockStore.(*mockBlockStoreWithCheckpoint).LastBlockCheckpoint, "block should have been checkpointed to store")
}

func TestShutdown(t *testing.T) {
	cbs := newMockCachedBlockStore(t)
	cbs.Shutdown()

	assert.True(t, cbs.blockStore.(*mockBlockStoreWithCheckpoint).IsShutdown, "store should be shutdown")
	assert.True(t, cbs.blockIndex.(*mocks.MockBlockIndex).IsShutdown, "index should be shutdown")
	// TODO: cache
}

func TestGetBlockchainInfo(t *testing.T) {
	cbs := newMockCachedBlockStore(t)

	mbi := common.BlockchainInfo{Height: 10}
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlockchainInfo = &mbi

	bi, err := cbs.GetBlockchainInfo()
	assert.NoError(t, err, "getting blockchain info should be successful")
	assert.Equal(t, &mbi, bi, "blockchain info from store should have been returned")
}

func TestRetrieveBlocks(t *testing.T) {
	cbs := newMockCachedBlockStore(t)

	eb0 := newMockBlock(0)
	eb1 := newMockBlock(1)
	eb2 := newMockBlock(2)
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByNumber[0] = eb0
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByNumber[1] = eb1
	cbs.blockCache.AddBlock(eb2)

	itr, err := cbs.RetrieveBlocks(0)
	assert.NoError(t, err, "retrieving blocks should be successful")

	ab0, err := itr.Next()
	assert.NoError(t, err, "retrieving block should be successful")
	assert.Equal(t, eb0, ab0, "expected block 0 from store")

	ab1, err := itr.Next()
	assert.NoError(t, err, "retrieving block should be successful")
	assert.Equal(t, eb1, ab1, "expected block 1 from store")

	ab2, err := itr.Next()
	assert.NoError(t, err, "retrieving block should be successful")
	assert.Equal(t, eb2, ab2, "expected block 2 from cache")

	_, err = itr.Next()
	assert.Error(t, err, "iterator should return error due to non existent block and mocked WaitForBlock")
}

func TestRetrieveBlockByNumber(t *testing.T) {
	cbs := newMockCachedBlockStore(t)

	eb0 := newMockBlock(0)
	eb1 := newMockBlock(1)
	eb2 := newMockBlock(2)
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByNumber[0] = eb0
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByNumber[1] = eb1
	cbs.blockCache.AddBlock(eb2)

	ab0, err := cbs.RetrieveBlockByNumber(0)
	assert.NoError(t, err, "retrieving block should be successful")
	assert.Equal(t, eb0, ab0, "expected block 0 from store")

	ab1, err := cbs.RetrieveBlockByNumber(1)
	assert.NoError(t, err, "retrieving block should be successful")
	assert.Equal(t, eb1, ab1, "expected block 1 from store")

	ab2, err := cbs.RetrieveBlockByNumber(2)
	assert.NoError(t, err, "retrieving block should be successful")
	assert.Equal(t, eb2, ab2, "expected block 2 from cache")

	_, err = cbs.RetrieveBlockByNumber(3)
	assert.Error(t, err, "retrieval should return error due to non existent block")
}

func TestRetrieveBlockByHash(t *testing.T) {
	cbs := newMockCachedBlockStore(t)

	eb0 := newMockBlock(0)
	eb1 := newMockBlock(1)
	eb2 := newMockBlock(2)
	hb0 := eb0.GetHeader().Hash()
	hb1 := eb1.GetHeader().Hash()
	hb2 := eb2.GetHeader().Hash()
	hb3 := []byte("3")

	hb0Hex := hex.EncodeToString(hb0)
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByHash[hb0Hex] = eb0
	hb1Hex := hex.EncodeToString(hb1)
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByHash[hb1Hex] = eb1

	cbs.blockCache.AddBlock(eb2)

	ab0, err := cbs.RetrieveBlockByHash(hb0)
	assert.NoError(t, err, "retrieving block should be successful")
	assert.Equal(t, eb0, ab0, "expected block 0 from store")

	ab1, err := cbs.RetrieveBlockByHash(hb1)
	assert.NoError(t, err, "retrieving block should be successful")
	assert.Equal(t, eb1, ab1, "expected block 1 from store")

	ab2, err := cbs.RetrieveBlockByHash(hb2)
	assert.NoError(t, err, "retrieving block should be successful")
	assert.Equal(t, eb2, ab2, "expected block 2 from cache")

	_, err = cbs.RetrieveBlockByHash(hb3)
	assert.Error(t, err, "retrieval should return error due to non existent block")
}

func newMockBlock(blockNum uint64) *common.Block {
	const namespace = "test_namespace"
	const key = "test_key"
	const txID = "12345"

	b := mocks.NewBlock(
		blockNum,
		&common.ChannelHeader{
			ChannelId: "",
			TxId:      txID,
			Type:      int32(common.HeaderType_ENDORSER_TRANSACTION),
		},
		[]peer.TxValidationCode{peer.TxValidationCode_VALID},
		mocks.NewTransaction(
			&rwsetutil.NsRwSet{
				NameSpace: namespace,
				KvRwSet: &kvrwset.KVRWSet{
					Writes: []*kvrwset.KVWrite{{Key: key, IsDelete: false, Value: []byte("some_value")}},
				},
			},
		),
	)

	return b
}

func newMockCachedBlockStore(t *testing.T) *cachedBlockStore {
	const noCacheLimit = 0
	blockCacheProvider := memblkcache.NewProvider(noCacheLimit)

	blockStore := mockBlockStoreWithCheckpoint{}
	blockStore.BlocksByNumber = make(map[uint64]*common.Block)
	blockStore.BlocksByHash = make(map[string]*common.Block)

	blockIndex := mocks.MockBlockIndex{}
	blockCache, err := blockCacheProvider.OpenBlockCache("mock")

	assert.NoError(t, err)

	return newCachedBlockStore(&blockStore, &blockIndex, blockCache)
}

type mockBlockStoreWithCheckpoint struct {
	mocks.MockBlockStore
}

func (m *mockBlockStoreWithCheckpoint) WaitForBlock(ctx context.Context, blockNum uint64) uint64 {
	return blockNum
}

func (m *mockBlockStoreWithCheckpoint) LastBlockNumber() uint64 {
	return 0
}

