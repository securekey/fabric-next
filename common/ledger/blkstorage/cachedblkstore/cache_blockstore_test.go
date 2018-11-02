/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cachedblkstore

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/hyperledger/fabric/common/ledger/blkstorage/memblkcache"
	"github.com/hyperledger/fabric/common/ledger/blkstorage/mocks"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric/protos/peer"
)

func TestAddBlock(t *testing.T) {
	cbs := newMockCachedBlockStore(t)

	const blockNumber = 0
	b := createMockBlock(blockNumber)

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
	b := createMockBlock(blockNumber)

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

	eb0 := createMockBlock(0)
	eb1 := createMockBlock(1)
	eb2 := createMockBlock(2)
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByNumber[0] = eb0
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByNumber[1] = eb1

	// Block 2 is only added to cache so that we can that the cache is hit rather than the store.
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

	eb0 := createMockBlock(0)
	eb1 := createMockBlock(1)
	eb2 := createMockBlock(2)
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByNumber[0] = eb0
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByNumber[1] = eb1

	// Block 2 is only added to cache so that we can that the cache is hit rather than the store.
	cbs.blockCache.AddBlock(eb2)

	_, ok := cbs.blockCache.LookupBlockByNumber(0)
	assert.False(t, ok, "block 0 is not in the cache")

	ab0, err := cbs.RetrieveBlockByNumber(0)
	assert.NoError(t, err, "retrieving block should be successful")
	assert.Equal(t, eb0, ab0, "expected block 0 from store")

	_, ok = cbs.blockCache.LookupBlockByNumber(0)
	assert.True(t, ok, "block 0 should be in the cache")

	_, ok = cbs.blockCache.LookupBlockByNumber(1)
	assert.False(t, ok, "block 1 is not in the cache")

	ab1, err := cbs.RetrieveBlockByNumber(1)
	assert.NoError(t, err, "retrieving block should be successful")
	assert.Equal(t, eb1, ab1, "expected block 1 from store")

	_, ok = cbs.blockCache.LookupBlockByNumber(1)
	assert.True(t, ok, "block 1 should be in the cache")

	ab2, err := cbs.RetrieveBlockByNumber(2)
	assert.NoError(t, err, "retrieving block should be successful")
	assert.Equal(t, eb2, ab2, "expected block 2 from cache")

	_, err = cbs.RetrieveBlockByNumber(3)
	assert.Error(t, err, "retrieval should return error due to non existent block")
}

func TestRetrieveBlockByHash(t *testing.T) {
	cbs := newMockCachedBlockStore(t)

	eb0 := createMockBlock(0)
	eb1 := createMockBlock(1)
	eb2 := createMockBlock(2)
	hb0 := eb0.GetHeader().Hash()
	hb1 := eb1.GetHeader().Hash()
	hb2 := eb2.GetHeader().Hash()
	hb3 := []byte("3")

	hb0Hex := hex.EncodeToString(hb0)
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByHash[hb0Hex] = eb0
	hb1Hex := hex.EncodeToString(hb1)
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByHash[hb1Hex] = eb1

	// Block 2 is only added to cache so that we can that the cache is hit rather than the store.
	cbs.blockCache.AddBlock(eb2)

	_, ok := cbs.blockCache.LookupBlockByHash(hb0)
	assert.False(t, ok, "block 0 is not in the cache")

	ab0, err := cbs.RetrieveBlockByHash(hb0)
	assert.NoError(t, err, "retrieving block should be successful")
	assert.Equal(t, eb0, ab0, "expected block 0 from store")

	_, ok = cbs.blockCache.LookupBlockByHash(hb0)
	assert.True(t, ok, "block 0 should be in the cache")

	_, ok = cbs.blockCache.LookupBlockByHash(hb1)
	assert.False(t, ok, "block 1 is not in the cache")

	ab1, err := cbs.RetrieveBlockByHash(hb1)
	assert.NoError(t, err, "retrieving block should be successful")
	assert.Equal(t, eb1, ab1, "expected block 1 from store")

	_, ok = cbs.blockCache.LookupBlockByHash(hb1)
	assert.True(t, ok, "block 1 should be in the cache")

	ab2, err := cbs.RetrieveBlockByHash(hb2)
	assert.NoError(t, err, "retrieving block should be successful")
	assert.Equal(t, eb2, ab2, "expected block 2 from cache")

	_, err = cbs.RetrieveBlockByHash(hb3)
	assert.Error(t, err, "retrieval should return error due to non existent block")
}

func TestRetrieveTxByBlockNumTranNum(t *testing.T) {
	cbs := newMockCachedBlockStore(t)

	eb0 := createMockBlockWithTxn(0, "a", "keya", peer.TxValidationCode_VALID)
	eb1 := createMockBlockWithTxn(1, "b", "keyb", peer.TxValidationCode_VALID)
	eb2 := createMockBlockWithTxn(2, "c", "keyc", peer.TxValidationCode_VALID)
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByNumber[0] = eb0
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByNumber[1] = eb1

	// Block 2 is only added to cache so that we can that the cache is hit rather than the store.
	cbs.blockCache.AddBlock(eb2)

	_, ok := cbs.blockCache.LookupBlockByNumber(0)
	assert.False(t, ok, "block 0 is not in the cache")

	atx0, err := cbs.RetrieveTxByBlockNumTranNum(0, 0)
	assert.NoError(t, err, "retrieving txn should be successful")
	etx0, err := extractEnvelopeFromBlock(eb0, 0)
	assert.NoError(t, err, "retrieving mock txn from mock block should be successful")
	assert.Equal(t, etx0, atx0, "expected txn 0 of block 0 from store")

	_, ok = cbs.blockCache.LookupBlockByNumber(0)
	assert.True(t, ok, "block 0 should be in the cache")

	_, ok = cbs.blockCache.LookupBlockByNumber(1)
	assert.False(t, ok, "block 1 is not in the cache")

	atx1, err := cbs.RetrieveTxByBlockNumTranNum(1, 0)
	assert.NoError(t, err, "retrieving txn should be successful")
	etx1, err := extractEnvelopeFromBlock(eb1, 0)
	assert.NoError(t, err, "retrieving mock txn from mock block should be successful")
	assert.Equal(t, etx1, atx1, "expected txn 0 of block 1 from store")

	_, ok = cbs.blockCache.LookupBlockByNumber(1)
	assert.True(t, ok, "block 1 should be in the cache")

	atx2, err := cbs.RetrieveTxByBlockNumTranNum(2, 0)
	assert.NoError(t, err, "retrieving txn should be successful")
	etx2, err := extractEnvelopeFromBlock(eb2, 0)
	assert.NoError(t, err, "retrieving mock txn from mock block should be successful")
	assert.Equal(t, etx2, atx2, "expected txn 0 of block 2 from cache")

	_, err = cbs.RetrieveTxByBlockNumTranNum(3, 0)
	assert.Error(t, err, "retrieval should return error due to non existent block")
}

func TestRetrieveTxByID(t *testing.T) {
	cbs := newMockCachedBlockStore(t)

	eb0 := createMockBlockWithTxn(0, "a", "keya", peer.TxValidationCode_VALID)
	eb1 := createMockBlockWithTxn(1, "b", "keyb", peer.TxValidationCode_VALID)
	eb2 := createMockBlockWithTxn(2, "c", "keyc", peer.TxValidationCode_VALID)
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByNumber[0] = eb0
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByNumber[1] = eb1

	// Block 0 is indexed but not cached.
	txaLoc := mocks.MockTXLoc{
		MockBlockNumber: 0,
		MockTxNumber: 0,
	}
	cbs.blockIndex.(*mocks.MockBlockIndex).TxLocsByTxID["a"] = &txaLoc

	// Block 1 is indexed and cached.
	txbLoc := mocks.MockTXLoc{
		MockBlockNumber: 1,
		MockTxNumber: 0,
	}
	cbs.blockIndex.(*mocks.MockBlockIndex).TxLocsByTxID["b"] = &txbLoc
	cbs.blockCache.AddBlock(eb1)

	//cbs.blockIndex.(*mocks.MockBlockIndex).TxLocsByNum[0] = make(map[uint64]blkstorage.TxLoc)
	//cbs.blockIndex.(*mocks.MockBlockIndex).TxLocsByNum[0][0] = &txaLoc

	_, ok := cbs.blockCache.LookupBlockByNumber(0)
	assert.False(t, ok, "block 0 is not in the cache")

	atx0, err := cbs.RetrieveTxByID("a")
	assert.NoError(t, err, "retrieving txn should be successful")
	etx0, err := extractEnvelopeFromBlock(eb0, 0)
	assert.NoError(t, err, "retrieving mock txn from mock block should be successful")
	assert.Equal(t, etx0, atx0, "expected txn 0 of block 0 from store")

	_, ok = cbs.blockCache.LookupBlockByNumber(0)
	assert.True(t, ok, "block 0 should be in the cache")

	atx1, err := cbs.RetrieveTxByID("b")
	assert.NoError(t, err, "retrieving txn should be successful")
	etx1, err := extractEnvelopeFromBlock(eb1, 0)
	assert.NoError(t, err, "retrieving mock txn from mock block should be successful")
	assert.Equal(t, etx1, atx1, "expected txn 0 of block 1 from store")

	// Block 2 is only added to cache so that we can that the cache is hit rather than the index.
	cbs.blockCache.AddBlock(eb2)

	atx2, err := cbs.RetrieveTxByID("c")
	assert.NoError(t, err, "retrieving txn should be successful")
	etx2, err := extractEnvelopeFromBlock(eb2, 0)
	assert.NoError(t, err, "retrieving mock txn from mock block should be successful")
	assert.Equal(t, etx2, atx2, "expected txn 0 of block 2 from cache")

	_, err = cbs.RetrieveTxByID("d")
	assert.Error(t, err, "retrieving non-existing txn should fail")
}

func TestRetrieveTxByIDNonExistenceIndex(t *testing.T) {
	cbs := newMockCachedBlockStore(t)
	// note: we do not add the transactions to the backing store so we know it does not get hit.

	eb0 := createMockBlockWithTxn(0, "a", "keya", peer.TxValidationCode_VALID)
	eb1 := createMockBlockWithTxn(1, "b", "keyb", peer.TxValidationCode_VALID)
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByNumber[0] = eb0
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByNumber[1] = eb1
	cbs.blockCache.AddBlock(eb1)


	_, err := cbs.RetrieveTxByID("a")
	assert.Error(t, err, "retrieving non-indexed & non-cached txn should fail")

	_, err = cbs.RetrieveTxByID("b")
	assert.NoError(t, err, "retrieving cached txn does not fail")
}

func TestRetrieveBlockByTxID(t *testing.T) {
	cbs := newMockCachedBlockStore(t)

	eb0 := createMockBlockWithTxn(0, "a", "keya", peer.TxValidationCode_VALID)
	eb1 := createMockBlockWithTxn(1, "b", "keyb", peer.TxValidationCode_VALID)
	eb2 := createMockBlockWithTxn(2, "c", "keyc", peer.TxValidationCode_VALID)
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByNumber[0] = eb0
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByNumber[1] = eb1

	// Block 0 is indexed but not cached.
	txaLoc := mocks.MockTXLoc{
		MockBlockNumber: 0,
		MockTxNumber: 0,
	}
	cbs.blockIndex.(*mocks.MockBlockIndex).TxLocsByTxID["a"] = &txaLoc

	// Block 1 is indexed and cached.
	txbLoc := mocks.MockTXLoc{
		MockBlockNumber: 1,
		MockTxNumber: 0,
	}
	cbs.blockIndex.(*mocks.MockBlockIndex).TxLocsByTxID["b"] = &txbLoc
	cbs.blockCache.AddBlock(eb1)

	_, ok := cbs.blockCache.LookupBlockByNumber(0)
	assert.False(t, ok, "block 0 is not in the cache")

	ab0, err := cbs.RetrieveBlockByTxID("a")
	assert.NoError(t, err, "retrieving block should be successful")
	assert.Equal(t, eb0, ab0, "expected block 0 from store")

	_, ok = cbs.blockCache.LookupBlockByNumber(0)
	assert.True(t, ok, "block 0 should be in the cache")

	ab1, err := cbs.RetrieveBlockByTxID("b")
	assert.NoError(t, err, "retrieving block should be successful")
	assert.Equal(t, eb1, ab1, "expected block 1 from store")

	// Block 2 is only added to cache so that we can that the cache is hit rather than the index.
	cbs.blockCache.AddBlock(eb2)

	ab2, err := cbs.RetrieveBlockByTxID("c")
	assert.NoError(t, err, "retrieving block should be successful")
	assert.Equal(t, eb2, ab2, "expected block 2 from cache")

	_, err = cbs.RetrieveBlockByTxID("d")
	assert.Error(t, err, "retrieving non-existing txn should fail")
}

func TestRetrieveBlockByTxIDNonExistenceIndex(t *testing.T) {
	cbs := newMockCachedBlockStore(t)
	// note: we do not add the transactions to the backing store so we know it does not get hit.

	eb0 := createMockBlockWithTxn(0, "a", "keya", peer.TxValidationCode_VALID)
	eb1 := createMockBlockWithTxn(1, "b", "keyb", peer.TxValidationCode_VALID)
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByNumber[0] = eb0
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByNumber[1] = eb1
	cbs.blockCache.AddBlock(eb1)


	_, err := cbs.RetrieveBlockByTxID("a")
	assert.Error(t, err, "retrieving non-indexed & non-cached txn should fail")

	_, err = cbs.RetrieveBlockByTxID("b")
	assert.NoError(t, err, "retrieving cached txn does not fail")
}

func TestRetrieveTxValidationCodeByTxID(t *testing.T) {
	cbs := newMockCachedBlockStore(t)

	etx0 := peer.TxValidationCode_BAD_PAYLOAD
	etx1 := peer.TxValidationCode_BAD_COMMON_HEADER
	etx2 := peer.TxValidationCode_BAD_CREATOR_SIGNATURE
	eb0 := createMockBlockWithTxn(0, "a", "keya", etx0)
	eb1 := createMockBlockWithTxn(1, "b", "keyb", etx1)
	eb2 := createMockBlockWithTxn(2, "c", "keyc", etx2)
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByNumber[0] = eb0
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByNumber[1] = eb1

	// Block 0 is indexed but not cached.
	cbs.blockIndex.(*mocks.MockBlockIndex).TxValidationCodeByTxID["a"] = etx0

	// Block 1 is indexed and cached.
	cbs.blockIndex.(*mocks.MockBlockIndex).TxValidationCodeByTxID["b"] = etx1
	cbs.blockCache.AddBlock(eb1)

	_, ok := cbs.blockCache.LookupBlockByNumber(0)
	assert.False(t, ok, "block 0 is not in the cache")

	atx0, err := cbs.RetrieveTxValidationCodeByTxID("a")
	assert.NoError(t, err, "retrieving txn should be successful")
	assert.Equal(t, etx0, atx0, "expected txn 0 of block 0 from store")

	// TODO: make an explicit cache for txn validation codes?

	atx1, err := cbs.RetrieveTxValidationCodeByTxID("b")
	assert.NoError(t, err, "retrieving txn should be successful")
	assert.Equal(t, etx1, atx1, "expected txn 0 of block 1 from cache")

	// Block 2 is only added to cache so that we can that the cache is hit rather than the index.
	cbs.blockCache.AddBlock(eb2)

	atx2, err := cbs.RetrieveTxValidationCodeByTxID("c")
	assert.NoError(t, err, "retrieving txn should be successful")
	assert.Equal(t, etx2, atx2, "expected txn 0 of block 2 from cache")

	_, err = cbs.RetrieveTxValidationCodeByTxID("d")
	assert.Error(t, err, "retrieving non-existing txn should fail")
}

func TestRetrieveTxValidationCodeByTxIDNonExistenceIndex(t *testing.T) {
	cbs := newMockCachedBlockStore(t)
	// note: we do not add the transactions to the backing store so we know it does not get hit.

	eb0 := createMockBlockWithTxn(0, "a", "keya", peer.TxValidationCode_VALID)
	eb1 := createMockBlockWithTxn(1, "b", "keyb", peer.TxValidationCode_VALID)
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByNumber[0] = eb0
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByNumber[1] = eb1
	cbs.blockCache.AddBlock(eb1)

	_, err := cbs.RetrieveTxValidationCodeByTxID("a")
	assert.Error(t, err, "retrieving non-indexed & non-cached txn should fail")

	_, err = cbs.RetrieveTxValidationCodeByTxID("b")
	assert.NoError(t, err, "retrieving cached txn does not fail")
}

func createMockBlockWithTxn(blockNum uint64, txID string, txKey string, txValidationCode peer.TxValidationCode) *common.Block {
	const namespace = "test_namespace"

	b := mocks.NewBlock(
		blockNum,
		&common.ChannelHeader{
			ChannelId: "",
			TxId:      txID,
			Type:      int32(common.HeaderType_ENDORSER_TRANSACTION),
		},
		[]peer.TxValidationCode{txValidationCode},
		mocks.NewTransaction(
			&rwsetutil.NsRwSet{
				NameSpace: namespace,
				KvRwSet: &kvrwset.KVRWSet{
					Writes: []*kvrwset.KVWrite{{Key: txKey, IsDelete: false, Value: []byte("some_value")}},
				},
			},
		),
	)

	return b
}

func createMockBlock(blockNum uint64) *common.Block {
	const key = "test_key"
	const txID = "12345"

	return createMockBlockWithTxn(blockNum, txID, key, peer.TxValidationCode_VALID)
}

func newMockCachedBlockStore(t *testing.T) *cachedBlockStore {
	const noCacheLimit = 0
	blockCacheProvider := memblkcache.NewProvider(noCacheLimit)

	blockStore := newMockBlockStoreWithCheckpoint()
	blockIndex := mocks.NewMockBlockIndex()

	blockCache, err := blockCacheProvider.OpenBlockCache("mock")
	assert.NoError(t, err)

	return newCachedBlockStore(blockStore, blockIndex, blockCache)
}

type mockBlockStoreWithCheckpoint struct {
	*mocks.MockBlockStore
}

func newMockBlockStoreWithCheckpoint() *mockBlockStoreWithCheckpoint {
	mbs := mocks.NewMockBlockStore()

	bs := mockBlockStoreWithCheckpoint{
		MockBlockStore: mbs,
	}

	return &bs
}

func (m *mockBlockStoreWithCheckpoint) WaitForBlock(ctx context.Context, blockNum uint64) uint64 {
	return blockNum
}

func (m *mockBlockStoreWithCheckpoint) LastBlockNumber() uint64 {
	return 0
}

