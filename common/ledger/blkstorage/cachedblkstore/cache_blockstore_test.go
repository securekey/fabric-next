/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cachedblkstore

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hyperledger/fabric/common/ledger/blkstorage/memblkcache"
	"github.com/hyperledger/fabric/common/ledger/blkstorage/mocks"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
)

func TestAddBlock(t *testing.T) {
	cbs := newMockCachedBlockStore(t)

	const blockNumber = 0
	b := mocks.CreateSimpleMockBlock(blockNumber)

	err := cbs.AddBlock(b)
	assert.NoError(t, err, "block should have been added successfully")

	err = cbs.CheckpointBlock(b)
	assert.NoError(t, err, "block should have been checkpointed successfully")

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	cbs.blockStore.WaitForBlock(ctx, blockNumber)

	assert.Equal(t, b, cbs.blockStore.(*mockBlockStoreWithCheckpoint).LastBlockAdd, "block should have been added to store")
	assert.Equal(t, b, cbs.blockIndex.(*mocks.MockBlockIndex).LastBlockAdd, "block should have been added to index")

	cachedBlock, ok := cbs.blockCache.LookupBlockByNumber(blockNumber)
	assert.True(t, ok, "block should exist in cache")
	assert.Equal(t, b, cachedBlock, "block should have been added to cache")
}

func TestCheckpointBlock(t *testing.T) {
	cbs := newMockCachedBlockStore(t)

	const blockNumber = 0
	b := mocks.CreateSimpleMockBlock(blockNumber)

	err := cbs.AddBlock(b)
	assert.NoError(t, err, "block should have been added successfully")

	err = cbs.CheckpointBlock(b)
	assert.NoError(t, err, "block should have been checkpointed successfully")

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	cbs.blockStore.WaitForBlock(ctx, blockNumber)

	assert.Equal(t, b, cbs.blockStore.(*mockBlockStoreWithCheckpoint).LastBlockCheckpoint, "block should have been checkpointed to store")
}

func TestBlockCommittedSignal(t *testing.T) {
	cbs := newMockCachedBlockStore(t)

	const blockNumber = 0
	b := mocks.CreateSimpleMockBlock(blockNumber)

	_, nextBlockCh := cbs.BlockCommitted()

	err := cbs.AddBlock(b)
	assert.NoError(t, err, "block should have been added successfully")

	select {
	case <-nextBlockCh:
		t.Fatal("Committed Signal should not have been called")
	case <-time.After(100 * time.Millisecond):
	}

	err = cbs.CheckpointBlock(b)
	assert.NoError(t, err, "block should have been checkpointed successfully")

	cbs.blockStore.(*mockBlockStoreWithCheckpoint).blockCommittedCh = make(chan struct{})
	close(cbs.blockStore.(*mockBlockStoreWithCheckpoint).blockCommittedCh)

	_, nextBlockCh = cbs.BlockCommitted()

	select {
	case <-nextBlockCh:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Committed Signal should have been called")
	}

	assert.Equal(t, uint64(0), cbs.LastBlockNumber(), "last block committed should be 0")
}

func TestShutdown(t *testing.T) {
	cbs := newMockCachedBlockStore(t)
	cbs.Shutdown()

	assert.True(t, cbs.blockStore.(*mockBlockStoreWithCheckpoint).IsShutdown, "store should be shutdown")
	assert.True(t, cbs.blockIndex.(*mocks.MockBlockIndex).IsShutdown, "index should be shutdown")
	// TODO: cache
}

func TestGetBlockchainInfoOnStartup(t *testing.T) {
	mbi := common.BlockchainInfo{Height: 10}
	cbs := newMockCachedBlockStoreWithBlockchainInfo(t, &mbi)

	bi, err := cbs.GetBlockchainInfo()
	assert.NoError(t, err, "getting blockchain info should be successful")
	assert.Equal(t, &mbi, bi, "blockchain info from store should have been returned")
}

func TestGetBlockchainInfo(t *testing.T) {
	mbi := common.BlockchainInfo{Height: 10}
	cbs := newMockCachedBlockStoreWithBlockchainInfo(t, &mbi)

	eb0 := mocks.CreateSimpleMockBlock(0)
	eb1 := mocks.CreateSimpleMockBlock(1)
	eb1.Header.PreviousHash = eb0.GetHeader().Hash()

	mbi0 := common.BlockchainInfo{
		Height:           1,
		CurrentBlockHash: eb0.GetHeader().Hash(),
	}

	mbi1 := common.BlockchainInfo{
		Height:            2,
		CurrentBlockHash:  eb1.GetHeader().Hash(),
		PreviousBlockHash: eb0.GetHeader().Hash(),
	}

	err := cbs.AddBlock(eb0)
	assert.NoError(t, err, "block should have been added successfully")

	bi, err := cbs.GetBlockchainInfo()
	assert.NoError(t, err, "getting blockchain info should be successful")
	assert.Equal(t, &mbi, bi, "blockchain info from store should have been returned")

	err = cbs.CheckpointBlock(eb0)
	assert.NoError(t, err, "block should have been checkpointed successfully")

	bi, err = cbs.GetBlockchainInfo()
	assert.NoError(t, err, "getting blockchain info should be successful")
	assert.Equal(t, &mbi0, bi, "blockchain info from store should have been returned")

	err = cbs.AddBlock(eb1)
	assert.NoError(t, err, "block should have been added successfully")

	err = cbs.CheckpointBlock(eb1)
	assert.NoError(t, err, "block should have been checkpointed successfully")

	bi, err = cbs.GetBlockchainInfo()
	assert.NoError(t, err, "getting blockchain info should be successful")
	assert.Equal(t, &mbi1, bi, "blockchain info from store should have been returned")
}

func TestRetrieveBlocks(t *testing.T) {
	cbs := newMockCachedBlockStore(t)

	eb0 := mocks.CreateSimpleMockBlock(0)
	eb1 := mocks.CreateSimpleMockBlock(1)
	eb2 := mocks.CreateSimpleMockBlock(2)
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

	eb0 := mocks.CreateSimpleMockBlock(0)
	eb1 := mocks.CreateSimpleMockBlock(1)
	eb2 := mocks.CreateSimpleMockBlock(2)
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

	eb0 := mocks.CreateSimpleMockBlock(0)
	eb1 := mocks.CreateSimpleMockBlock(1)
	eb2 := mocks.CreateSimpleMockBlock(2)
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

	eb0 := mocks.CreateBlock(0,
		mocks.NewTransactionWithMockKey("a", "keya", peer.TxValidationCode_VALID),
		mocks.NewTransactionWithMockKey("aa", "keyaa", peer.TxValidationCode_VALID),
	)
	eb1 := mocks.CreateBlock(1, mocks.NewTransactionWithMockKey("b", "keyb", peer.TxValidationCode_VALID))
	eb2 := mocks.CreateBlock(2, mocks.NewTransactionWithMockKey("c", "keyc", peer.TxValidationCode_VALID))
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByNumber[0] = eb0
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByNumber[1] = eb1

	// Block 2 is only added to cache so that we can that the cache is hit rather than the store.
	cbs.blockCache.AddBlock(eb2)

	_, ok := cbs.blockCache.LookupBlockByNumber(0)
	assert.False(t, ok, "block 0 is not in the cache")

	atx0a, err := cbs.RetrieveTxByBlockNumTranNum(0, 0)
	assert.NoError(t, err, "retrieving txn should be successful")
	etx0a, err := extractEnvelopeFromBlock(eb0, 0)
	assert.NoError(t, err, "retrieving mock txn from mock block should be successful")
	assert.Equal(t, etx0a, atx0a, "expected txn 0 of block 0 from store")

	atx0b, err := cbs.RetrieveTxByBlockNumTranNum(0, 1)
	assert.NoError(t, err, "retrieving txn should be successful")
	etx0b, err := extractEnvelopeFromBlock(eb0, 1)
	assert.NoError(t, err, "retrieving mock txn from mock block should be successful")
	assert.Equal(t, etx0b, atx0b, "expected txn 1 of block 1 from store")

	_, ok = cbs.blockCache.LookupBlockByNumber(0)
	assert.True(t, ok, "block 0 should be in the cache")

	_, ok = cbs.blockCache.LookupBlockByNumber(1)
	assert.False(t, ok, "block 1 is not in the cache")

	atx1a, err := cbs.RetrieveTxByBlockNumTranNum(1, 0)
	assert.NoError(t, err, "retrieving txn should be successful")
	etx1a, err := extractEnvelopeFromBlock(eb1, 0)
	assert.NoError(t, err, "retrieving mock txn from mock block should be successful")
	assert.Equal(t, etx1a, atx1a, "expected txn 0 of block 1 from store")

	_, ok = cbs.blockCache.LookupBlockByNumber(1)
	assert.True(t, ok, "block 1 should be in the cache")

	atx2a, err := cbs.RetrieveTxByBlockNumTranNum(2, 0)
	assert.NoError(t, err, "retrieving txn should be successful")
	etx2a, err := extractEnvelopeFromBlock(eb2, 0)
	assert.NoError(t, err, "retrieving mock txn from mock block should be successful")
	assert.Equal(t, etx2a, atx2a, "expected txn 0 of block 2 from cache")

	_, err = cbs.RetrieveTxByBlockNumTranNum(3, 0)
	assert.Error(t, err, "retrieval should return error due to non existent block")
}

func TestRetrieveTxByID(t *testing.T) {
	cbs := newMockCachedBlockStore(t)

	eb0 := mocks.CreateBlock(0,
		mocks.NewTransactionWithMockKey("a", "keya", peer.TxValidationCode_VALID),
		mocks.NewTransactionWithMockKey("aa", "keyaa", peer.TxValidationCode_VALID),
	)
	eb1 := mocks.CreateBlock(1,
		mocks.NewTransactionWithMockKey("b", "keyb", peer.TxValidationCode_VALID),
		mocks.NewTransactionWithMockKey("bb", "keybb", peer.TxValidationCode_VALID),
	)
	eb2 := mocks.CreateBlock(2, mocks.NewTransactionWithMockKey("c", "keyc", peer.TxValidationCode_VALID))
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByNumber[0] = eb0
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByNumber[1] = eb1

	// Block 0 is indexed but not cached.
	txaLoc := mocks.MockTXLoc{
		MockBlockNumber: 0,
		MockTxNumber:    0,
	}
	cbs.blockIndex.(*mocks.MockBlockIndex).TxLocsByTxID["a"] = &txaLoc

	txaaLoc := mocks.MockTXLoc{
		MockBlockNumber: 0,
		MockTxNumber:    1,
	}
	cbs.blockIndex.(*mocks.MockBlockIndex).TxLocsByTxID["aa"] = &txaaLoc

	// Block 1 is indexed and cached.
	txbLoc := mocks.MockTXLoc{
		MockBlockNumber: 1,
		MockTxNumber:    0,
	}
	cbs.blockIndex.(*mocks.MockBlockIndex).TxLocsByTxID["b"] = &txbLoc
	txbbLoc := mocks.MockTXLoc{
		MockBlockNumber: 1,
		MockTxNumber:    1,
	}
	cbs.blockIndex.(*mocks.MockBlockIndex).TxLocsByTxID["bb"] = &txbbLoc
	cbs.blockCache.AddBlock(eb1)

	//cbs.blockIndex.(*mocks.MockBlockIndex).TxLocsByNum[0] = make(map[uint64]blkstorage.TxLoc)
	//cbs.blockIndex.(*mocks.MockBlockIndex).TxLocsByNum[0][0] = &txaLoc

	_, ok := cbs.blockCache.LookupBlockByNumber(0)
	assert.False(t, ok, "block 0 is not in the cache")

	atx0a, err := cbs.RetrieveTxByID("a")
	assert.NoError(t, err, "retrieving txn should be successful")
	etx0a, err := extractEnvelopeFromBlock(eb0, 0)
	assert.NoError(t, err, "retrieving mock txn from mock block should be successful")
	assert.Equal(t, etx0a, atx0a, "expected txn 0 of block 0 from store")

	atx0b, err := cbs.RetrieveTxByID("aa")
	assert.NoError(t, err, "retrieving txn should be successful")
	etx0b, err := extractEnvelopeFromBlock(eb0, 1)
	assert.NoError(t, err, "retrieving mock txn from mock block should be successful")
	assert.Equal(t, etx0b, atx0b, "expected txn 1 of block 0 from store")

	_, ok = cbs.blockCache.LookupBlockByNumber(0)
	assert.True(t, ok, "block 0 should be in the cache")

	atx1a, err := cbs.RetrieveTxByID("b")
	assert.NoError(t, err, "retrieving txn should be successful")
	etx1a, err := extractEnvelopeFromBlock(eb1, 0)
	assert.NoError(t, err, "retrieving mock txn from mock block should be successful")
	assert.Equal(t, etx1a, atx1a, "expected txn 0 of block 1 from store")

	atx1b, err := cbs.RetrieveTxByID("bb")
	assert.NoError(t, err, "retrieving txn should be successful")
	etx1b, err := extractEnvelopeFromBlock(eb1, 1)
	assert.NoError(t, err, "retrieving mock txn from mock block should be successful")
	assert.Equal(t, etx1b, atx1b, "expected txn 1 of block 1 from store")

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

	eb0 := mocks.CreateBlock(0, mocks.NewTransactionWithMockKey("a", "keya", peer.TxValidationCode_VALID))
	eb1 := mocks.CreateBlock(1, mocks.NewTransactionWithMockKey("b", "keyb", peer.TxValidationCode_VALID))
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

	eb0 := mocks.CreateBlock(0,
		mocks.NewTransactionWithMockKey("a", "keya", peer.TxValidationCode_VALID),
		mocks.NewTransactionWithMockKey("aa", "keyaa", peer.TxValidationCode_VALID),
	)
	eb1 := mocks.CreateBlock(1,
		mocks.NewTransactionWithMockKey("b", "keyb", peer.TxValidationCode_VALID),
		mocks.NewTransactionWithMockKey("bb", "keybb", peer.TxValidationCode_VALID),
	)
	eb2 := mocks.CreateBlock(2, mocks.NewTransactionWithMockKey("c", "keyc", peer.TxValidationCode_VALID))
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByNumber[0] = eb0
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByNumber[1] = eb1

	// Block 0 is indexed but not cached.
	txaLoc := mocks.MockTXLoc{
		MockBlockNumber: 0,
		MockTxNumber:    0,
	}
	cbs.blockIndex.(*mocks.MockBlockIndex).TxLocsByTxID["a"] = &txaLoc
	txaaLoc := mocks.MockTXLoc{
		MockBlockNumber: 0,
		MockTxNumber:    1,
	}
	cbs.blockIndex.(*mocks.MockBlockIndex).TxLocsByTxID["aa"] = &txaaLoc

	// Block 1 is indexed and cached.
	txbLoc := mocks.MockTXLoc{
		MockBlockNumber: 1,
		MockTxNumber:    0,
	}
	cbs.blockIndex.(*mocks.MockBlockIndex).TxLocsByTxID["b"] = &txbLoc
	txbbLoc := mocks.MockTXLoc{
		MockBlockNumber: 1,
		MockTxNumber:    1,
	}
	cbs.blockIndex.(*mocks.MockBlockIndex).TxLocsByTxID["bb"] = &txbbLoc
	cbs.blockCache.AddBlock(eb1)

	_, ok := cbs.blockCache.LookupBlockByNumber(0)
	assert.False(t, ok, "block 0 is not in the cache")

	ab0a, err := cbs.RetrieveBlockByTxID("a")
	assert.NoError(t, err, "retrieving block should be successful")
	assert.Equal(t, eb0, ab0a, "expected block 0 from store")

	ab0b, err := cbs.RetrieveBlockByTxID("aa")
	assert.NoError(t, err, "retrieving block should be successful")
	assert.Equal(t, eb0, ab0b, "expected block 0 from store")

	_, ok = cbs.blockCache.LookupBlockByNumber(0)
	assert.True(t, ok, "block 0 should be in the cache")

	ab1a, err := cbs.RetrieveBlockByTxID("b")
	assert.NoError(t, err, "retrieving block should be successful")
	assert.Equal(t, eb1, ab1a, "expected block 1 from store")

	ab1b, err := cbs.RetrieveBlockByTxID("bb")
	assert.NoError(t, err, "retrieving block should be successful")
	assert.Equal(t, eb1, ab1b, "expected block 1 from store")

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

	eb0 := mocks.CreateBlock(0, mocks.NewTransactionWithMockKey("a", "keya", peer.TxValidationCode_VALID))
	eb1 := mocks.CreateBlock(1, mocks.NewTransactionWithMockKey("b", "keyb", peer.TxValidationCode_VALID))
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

	etx0a := peer.TxValidationCode_BAD_PAYLOAD
	etx0b := peer.TxValidationCode_BAD_RESPONSE_PAYLOAD
	etx1a := peer.TxValidationCode_BAD_COMMON_HEADER
	etx1b := peer.TxValidationCode_BAD_RWSET
	etx2 := peer.TxValidationCode_BAD_CREATOR_SIGNATURE
	eb0 := mocks.CreateBlock(0,
		mocks.NewTransactionWithMockKey("a", "keya", etx0a),
		mocks.NewTransactionWithMockKey("aa", "keyaa", etx0b),
	)
	eb1 := mocks.CreateBlock(1,
		mocks.NewTransactionWithMockKey("b", "keyb", etx1a),
		mocks.NewTransactionWithMockKey("bb", "keybb", etx1b),
	)
	eb2 := mocks.CreateBlock(2, mocks.NewTransactionWithMockKey("c", "keyc", etx2))
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByNumber[0] = eb0
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByNumber[1] = eb1

	// Block 0 is indexed but not cached.
	cbs.blockIndex.(*mocks.MockBlockIndex).TxValidationCodeByTxID["a"] = etx0a
	cbs.blockIndex.(*mocks.MockBlockIndex).TxValidationCodeByTxID["aa"] = etx0b

	// Block 1 is indexed and cached.
	cbs.blockIndex.(*mocks.MockBlockIndex).TxValidationCodeByTxID["b"] = etx1a
	cbs.blockIndex.(*mocks.MockBlockIndex).TxValidationCodeByTxID["bb"] = etx1b
	cbs.blockCache.AddBlock(eb1)

	_, ok := cbs.blockCache.LookupBlockByNumber(0)
	assert.False(t, ok, "block 0 is not in the cache")

	atx0a, err := cbs.RetrieveTxValidationCodeByTxID("a")
	assert.NoError(t, err, "retrieving txn should be successful")
	assert.Equal(t, etx0a, atx0a, "expected txn 0 of block 0 from store")

	atx0b, err := cbs.RetrieveTxValidationCodeByTxID("aa")
	assert.NoError(t, err, "retrieving txn should be successful")
	assert.Equal(t, etx0b, atx0b, "expected txn 1 of block 0 from store")

	// TODO: make an explicit cache for txn validation codes?

	atx1a, err := cbs.RetrieveTxValidationCodeByTxID("b")
	assert.NoError(t, err, "retrieving txn should be successful")
	assert.Equal(t, etx1a, atx1a, "expected txn 0 of block 1 from cache")

	atx1b, err := cbs.RetrieveTxValidationCodeByTxID("bb")
	assert.NoError(t, err, "retrieving txn should be successful")
	assert.Equal(t, etx1b, atx1b, "expected txn 1 of block 1 from cache")

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

	eb0 := mocks.CreateBlock(0, mocks.NewTransactionWithMockKey("a", "keya", peer.TxValidationCode_VALID))
	eb1 := mocks.CreateBlock(1, mocks.NewTransactionWithMockKey("b", "keyb", peer.TxValidationCode_VALID))
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByNumber[0] = eb0
	cbs.blockStore.(*mockBlockStoreWithCheckpoint).BlocksByNumber[1] = eb1
	cbs.blockCache.AddBlock(eb1)

	_, err := cbs.RetrieveTxValidationCodeByTxID("a")
	assert.Error(t, err, "retrieving non-indexed & non-cached txn should fail")

	_, err = cbs.RetrieveTxValidationCodeByTxID("b")
	assert.NoError(t, err, "retrieving cached txn does not fail")
}

func newMockCachedBlockStore(t *testing.T) *cachedBlockStore {
	return newMockCachedBlockStoreWithBlockchainInfo(t, nil)
}

func newMockCachedBlockStoreWithBlockchainInfo(t *testing.T, mbi *common.BlockchainInfo) *cachedBlockStore {
	const noCacheLimit = 0
	blockCacheProvider := memblkcache.NewProvider(noCacheLimit)

	blockStore := newMockBlockStoreWithCheckpoint()
	blockIndex := mocks.NewMockBlockIndex()

	if mbi != nil {
		blockStore.BlockchainInfo = mbi
	}

	blockCache, err := blockCacheProvider.OpenBlockCache("mock")
	assert.NoError(t, err)

	cbs, err := newCachedBlockStore(blockStore, blockIndex, blockCache)
	assert.NoError(t, err)

	return cbs
}

type mockBlockStoreWithCheckpoint struct {
	*mocks.MockBlockStore
	blockCommittedCh chan struct{}
}

func newMockBlockStoreWithCheckpoint() *mockBlockStoreWithCheckpoint {
	mbs := mocks.NewMockBlockStore()

	bs := mockBlockStoreWithCheckpoint{
		MockBlockStore: mbs,
	}

	return &bs
}

func (m *mockBlockStoreWithCheckpoint) BlockCommitted() (uint64, chan struct{}) {
	return m.LastBlockNumber(), m.blockCommittedCh
}

func (m *mockBlockStoreWithCheckpoint) WaitForBlock(ctx context.Context, blockNum uint64) uint64 {
	for {
		select {
		case <-ctx.Done():
			return m.LastBlockNumber()
		case <-time.After(10 * time.Millisecond):
		}

		if blockNum >= m.LastBlockNumber() {
			return m.LastBlockNumber()
		}
	}
}

func (m *mockBlockStoreWithCheckpoint) LastBlockNumber() uint64 {
	return m.LastBlockCheckpoint.GetHeader().GetNumber()
}
