/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package memblkcache

import (
	"encoding/hex"
	"github.com/hyperledger/fabric/common/ledger/blkstorage/mocks"
	"github.com/hyperledger/fabric/protos/peer"
	"github.com/stretchr/testify/assert"
	"sort"
	"testing"
)

func TestAddBlock(t *testing.T) {
	bc := newBlockCache(0)

	assert.Equal(t, bc.blocks.Len(), 0, "there should not be any blocks on creation")

	eb0 := mocks.CreateSimpleMockBlock(0)
	err := bc.AddBlock(eb0)
	assert.NoError(t, err, "adding block should have been successful")
	ok := bc.OnBlockStored(eb0.GetHeader().GetNumber())
	assert.True(t, ok, "cache should have been updated with storage event")
	assert.Equal(t, 0, bc.blocks.Len(), "adding genesis block should not increase the blocks length")

	key := eb0.GetHeader().Number
	ab0, ok := bc.pinnedBlocks[key]
	assert.True(t, ok, "block 0 should exist in pinned blocks")
	assert.Equal(t, eb0, ab0, "retrieved block should match added block")

	eb1 := mocks.CreateSimpleMockBlock(1)
	err = bc.AddBlock(eb1)
	assert.NoError(t, err, "adding block should have been successful")
	ok = bc.OnBlockStored(eb1.GetHeader().GetNumber())
	assert.True(t, ok, "cache should have been updated with storage event")
	assert.Equal(t, 1, bc.blocks.Len(), "adding a block should increase the blocks length")

	key = eb1.GetHeader().Number
	ab1, ok := bc.blocks.Get(key)
	assert.True(t, ok, "block 1 should exist in cache")
	assert.Equal(t, eb1, ab1, "retrieved block should match added block")
}

func TestLookupBlockByNumber(t *testing.T) {
	bc := newBlockCache(0)

	_, ok := bc.LookupBlockByNumber(0)
	assert.False(t, ok, "block 0 should not exist in cache")

	eb0 := mocks.CreateSimpleMockBlock(0)
	err := bc.AddBlock(eb0)
	assert.NoError(t, err, "adding block 0 should have been successful")
	ok = bc.OnBlockStored(eb0.GetHeader().GetNumber())
	assert.True(t, ok, "cache should have been updated with storage event")

	ab0, ok := bc.LookupBlockByNumber(0)
	assert.True(t, ok, "block 0 should exist in cache")
	assert.Equal(t, eb0, ab0, "retrieved block should match added block")

	eb1 := mocks.CreateSimpleMockBlock(1)
	err = bc.AddBlock(eb1)
	assert.NoError(t, err, "adding block 1 should have been successful")
	ok = bc.OnBlockStored(eb1.GetHeader().GetNumber())
	assert.True(t, ok, "cache should have been updated with storage event")

	ab1, ok := bc.LookupBlockByNumber(1)
	assert.True(t, ok, "block 1 should exist in cache")
	assert.Equal(t, eb1, ab1, "retrieved block should match added block")
}

func TestLookupBlockByHash(t *testing.T) {
	bc := newBlockCache(0)

	eb0 := mocks.CreateSimpleMockBlock(0)
	hb0 := eb0.GetHeader().Hash()
	_, ok := bc.LookupBlockByHash(hb0)
	assert.False(t, ok, "block 0 should not exist in cache")

	err := bc.AddBlock(eb0)
	assert.NoError(t, err, "adding block 0 should have been successful")
	ok = bc.OnBlockStored(eb0.GetHeader().GetNumber())
	assert.True(t, ok, "cache should have been updated with storage event")

	ab0, ok := bc.LookupBlockByHash(hb0)
	assert.True(t, ok, "block 0 should exist in cache")
	assert.Equal(t, eb0, ab0, "retrieved block should match added block")

	eb1 := mocks.CreateSimpleMockBlock(1)
	hb1 := eb1.GetHeader().Hash()
	_, ok = bc.LookupBlockByHash(hb1)
	assert.False(t, ok, "block 1 should not exist in cache")

	err = bc.AddBlock(eb1)
	assert.NoError(t, err, "adding block 1 should have been successful")
	ok = bc.OnBlockStored(eb1.GetHeader().GetNumber())
	assert.True(t, ok, "cache should have been updated with storage event")

	ab1, ok := bc.LookupBlockByHash(hb1)
	assert.True(t, ok, "block 1 should exist in cache")
	assert.Equal(t, eb1, ab1, "retrieved block should match added block")
}

func TestLookupTxLoc(t *testing.T) {
	bc := newBlockCache(0)

	const (
		txnID0="a"
		txnID1="b"
		txnID2="c"
	)

	b0 := mocks.CreateBlock(0,
		mocks.NewTransactionWithMockKey(txnID0, "some-key", peer.TxValidationCode_VALID),
		mocks.NewTransactionWithMockKey(txnID1, "some-key", peer.TxValidationCode_VALID),
	)
	b1 := mocks.CreateBlock(1, mocks.NewTransactionWithMockKey(txnID2, "some-key", peer.TxValidationCode_VALID))
	_, ok := bc.LookupTxLoc(txnID2)
	assert.False(t, ok, "txn 2 should not exist in cache")

	err := bc.AddBlock(b0)
	assert.NoError(t, err, "adding block 0 should have been successful")
	err = bc.AddBlock(b1)
	assert.NoError(t, err, "adding block 1 should have been successful")

	atx0, ok := bc.LookupTxLoc(txnID0)
	assert.True(t, ok, "block 0 should exist in cache")
	assert.Equal(t, uint64(0), atx0.BlockNumber(), "retrieved block should match added block")
	assert.Equal(t, uint64(0), atx0.TxNumber(), "retrieved block should match added block")

	atx1, ok := bc.LookupTxLoc(txnID1)
	assert.True(t, ok, "block 0 should exist in cache")
	assert.Equal(t, uint64(0), atx1.BlockNumber(), "retrieved block should match added block")
	assert.Equal(t, uint64(1), atx1.TxNumber(), "retrieved block should match added block")

	atx2, ok := bc.LookupTxLoc(txnID2)
	assert.True(t, ok, "block 1 should exist in cache")
	assert.Equal(t, uint64(1), atx2.BlockNumber(), "retrieved block should match added block")
	assert.Equal(t, uint64(0), atx2.TxNumber(), "retrieved block should match added block")
}

func TestBlockEviction(t *testing.T) {
	bc := newBlockCache(2)

	_, ok := bc.LookupBlockByNumber(0)
	assert.False(t, ok, "block 0 should not exist in cache")

	_, ok = bc.LookupBlockByNumber(1)
	assert.False(t, ok, "block 1 should not exist in cache")

	eb1 := mocks.CreateSimpleMockBlock(1)
	err := bc.AddBlock(eb1)
	assert.NoError(t, err, "adding block 1 should have been successful")
	ok = bc.OnBlockStored(eb1.GetHeader().GetNumber())
	assert.True(t, ok, "cache should have been updated with storage event")

	_, ok = bc.LookupBlockByNumber(1)
	assert.True(t, ok, "block 1 should exist in cache")

	eb2 := mocks.CreateSimpleMockBlock(2)
	err = bc.AddBlock(eb2)
	assert.NoError(t, err, "adding block 2 should have been successful")
	ok = bc.OnBlockStored(eb2.GetHeader().GetNumber())
	assert.True(t, ok, "cache should have been updated with storage event")

	eb3 := mocks.CreateSimpleMockBlock(3)
	err = bc.AddBlock(eb3)
	assert.NoError(t, err, "adding block 3 should have been successful")
	ok = bc.OnBlockStored(eb3.GetHeader().GetNumber())
	assert.True(t, ok, "cache should have been updated with storage event")

	_, ok = bc.LookupBlockByNumber(1)
	assert.False(t, ok, "block 1 should have been evicted from cache")

	_, ok = bc.LookupBlockByNumber(3)
	assert.True(t, ok, "block 3 should exist in cache")
	_, ok = bc.LookupBlockByNumber(2)
	assert.True(t, ok, "block 2 should exist in cache")

	eb4 := mocks.CreateSimpleMockBlock(4)
	err = bc.AddBlock(eb4)
	assert.NoError(t, err, "adding block 4 should have been successful")
	ok = bc.OnBlockStored(eb4.GetHeader().GetNumber())
	assert.True(t, ok, "cache should have been updated with storage event")

	_, ok = bc.LookupBlockByNumber(3)
	assert.False(t, ok, "block 3 should have been evicted from cache")
}

func TestPinnedBlockEviction(t *testing.T) {
	bc := newBlockCache(2)

	ok := bc.OnBlockStored(1)
	assert.False(t, ok, "block 1 should not handle stored event")

	eb1 := mocks.CreateSimpleMockBlock(1)
	err := bc.AddBlock(eb1)
	assert.NoError(t, err, "adding block 1 should have been successful")

	_, ok = bc.LookupBlockByNumber(1)
	assert.True(t, ok, "block 1 should exist in cache")

	eb2 := mocks.CreateSimpleMockBlock(2)
	err = bc.AddBlock(eb2)
	assert.NoError(t, err, "adding block 2 should have been successful")

	_, ok = bc.LookupBlockByNumber(2)
	assert.True(t, ok, "block 2 should exist in cache")

	eb3 := mocks.CreateSimpleMockBlock(3)
	err = bc.AddBlock(eb3)
	assert.NoError(t, err, "adding block 3 should have been successful")

	_, ok = bc.LookupBlockByNumber(1)
	assert.True(t, ok, "block 1 should exist in cache")
	_, ok = bc.LookupBlockByNumber(2)
	assert.True(t, ok, "block 2 should exist in cache")
	_, ok = bc.LookupBlockByNumber(3)
	assert.True(t, ok, "block 3 should exist in cache")

	ok = bc.OnBlockStored(1)
	assert.True(t, ok, "block 1 should handle stored event")
	_, ok = bc.LookupBlockByNumber(1)
	assert.True(t, ok, "block 1 should exist in cache")
	ok = bc.OnBlockStored(2)
	assert.True(t, ok, "block 2 should handle stored event")
	_, ok = bc.LookupBlockByNumber(2)
	assert.True(t, ok, "block 2 should exist in cache")
	ok = bc.OnBlockStored(3)
	assert.True(t, ok, "block 3 should handle stored event")
	_, ok = bc.LookupBlockByNumber(3)
	assert.True(t, ok, "block 3 should exist in cache")

	_, ok = bc.LookupBlockByNumber(1)
	assert.False(t, ok, "block 1 should have been evicted from cache")
	_, ok = bc.LookupBlockByNumber(2)
	assert.True(t, ok, "block 2 should exist in cache")
	_, ok = bc.LookupBlockByNumber(3)
	assert.True(t, ok, "block 3 should exist in cache")

	ok = bc.OnBlockStored(1)
	assert.False(t, ok, "block 1 should not handle stored event")
}

func TestEvictionCleanup(t *testing.T) {
	bc := newBlockCache(1)

	const (
		txnID0="a"
		txnID1="b"
		txnID2="c"
	)

	b1 := mocks.CreateBlock(1,
		mocks.NewTransactionWithMockKey(txnID0, "some-key", peer.TxValidationCode_VALID),
		mocks.NewTransactionWithMockKey(txnID1, "some-key", peer.TxValidationCode_VALID),
	)
	b2 := mocks.CreateBlock(2, mocks.NewTransactionWithMockKey(txnID2, "some-key", peer.TxValidationCode_VALID))

	assert.Equal(t, 0, bc.blocks.Len(), "blocks should be empty")
	assert.Equal(t, 0, len(bc.hashToNumber), "hashToNumber index should be empty")
	assert.Equal(t, 0, len(bc.numberToHash), "numberToHash index should be empty")
	assert.Equal(t, 0, len(bc.numberToTxnIDs), "numberToTxnIDs index should be empty")
	assert.Equal(t, 0, len(bc.txnLocs), "txnLocs index should be empty")

	err := bc.AddBlock(b1)
	assert.NoError(t, err, "adding block 0 should have been successful")
	ok := bc.OnBlockStored(b1.GetHeader().GetNumber())
	assert.True(t, ok, "cache should have been updated with storage event")

	assert.Equal(t, 1, bc.blocks.Len(), "blocks should have one entry")
	assert.Equal(t, 1, len(bc.hashToNumber), "hashToNumber index should have one entry")
	assert.Equal(t, 1, len(bc.numberToHash), "numberToHash index should have one entry")
	assert.Equal(t, 1, len(bc.numberToTxnIDs), "numberToTxnIDs index should have one entry")
	assert.Equal(t, 2, len(bc.txnLocs), "txnLocs index should have two entries")

	numberToTxnIDs := bc.numberToTxnIDs[1]
	sort.Strings(numberToTxnIDs)
	assert.Equal(t, []string{txnID0, txnID1}, numberToTxnIDs, "expected txn 1 & 2 in numberToTxnIDs index")

	k1 := b1.GetHeader().Number
	_, ok = bc.blocks.Get(k1)
	assert.True(t, ok, "block 1 should have been inserted into blocks cache")
	h1 := hex.EncodeToString(b1.GetHeader().Hash())
	_, ok = bc.hashToNumber[h1]
	assert.True(t, ok, "block 1 should have been inserted into hashToNumber index")
	_, ok = bc.numberToHash[1]
	assert.True(t, ok, "block 1 should have been inserted into numberToHash index")
	_, ok = bc.numberToTxnIDs[1]
	assert.True(t, ok, "block 1 should have been inserted into numberToTxnIDs index")
	_, ok = bc.txnLocs[txnID0]
	assert.True(t, ok, "txn 0 should have been inserted into txnLocs index")
	_, ok = bc.txnLocs[txnID1]
	assert.True(t, ok, "txn 1 should have been inserted into txnLocs index")

	err = bc.AddBlock(b2)
	assert.NoError(t, err, "adding block 2 should have been successful")
	ok = bc.OnBlockStored(b2.GetHeader().GetNumber())
	assert.True(t, ok, "cache should have been updated with storage event")

	assert.Equal(t, 1, bc.blocks.Len(), "blocks should have one entry")
	assert.Equal(t, 1, len(bc.hashToNumber), "hashToNumber index should have one entry")
	assert.Equal(t, 1, len(bc.numberToHash), "numberToHash index should have one entry")
	assert.Equal(t, 1, len(bc.numberToTxnIDs), "numberToTxnIDs index should have one entry")
	assert.Equal(t, 1, len(bc.txnLocs), "txnLocs index should have one entry")

	assert.Equal(t, []string{txnID2}, bc.numberToTxnIDs[2], "expected txn 2 in numberToTxnIDs index")

	_, ok = bc.blocks.Get(k1)
	assert.False(t, ok, "block 1 should have been evicted from blocks cache")
	_, ok = bc.hashToNumber[h1]
	assert.False(t, ok, "block 1 should have been evicted from hashToNumber index")
	_, ok = bc.numberToHash[1]
	assert.False(t, ok, "block 1 should have been evicted from numberToHash index")
	_, ok = bc.numberToTxnIDs[1]
	assert.False(t, ok, "block 1 should have been evicted from numberToTxnIDs index")
	_, ok = bc.txnLocs[txnID0]
	assert.False(t, ok, "txn 0 should have been evicted from txnLocs index")
	_, ok = bc.txnLocs[txnID1]
	assert.False(t, ok, "txn 1 should have been evicted from txnLocs index")

	k2 := b2.GetHeader().Number
	_, ok = bc.blocks.Get(k2)
	assert.True(t, ok, "block 2 should have been inserted into blocks cache")
	h2 := hex.EncodeToString(b2.GetHeader().Hash())
	_, ok = bc.hashToNumber[h2]
	assert.True(t, ok, "block 2 should have been inserted into hashToNumber index")
	_, ok = bc.numberToHash[2]
	assert.True(t, ok, "block 2 should have been inserted into numberToHash index")
	_, ok = bc.numberToTxnIDs[2]
	assert.True(t, ok, "block 2 should have been inserted into numberToTxnIDs index")
	_, ok = bc.txnLocs[txnID2]
	assert.True(t, ok, "txn 2 should have been inserted into txnLocs index")
}

func TestGenesisBlockPinned(t *testing.T) {
	bc := newBlockCache(1)

	eb0 := mocks.CreateSimpleMockBlock(0)
	err := bc.AddBlock(eb0)
	assert.NoError(t, err, "adding block 0 should have been successful")
	ok := bc.OnBlockStored(eb0.GetHeader().GetNumber())
	assert.True(t, ok, "cache should have been updated with storage event")

	eb1 := mocks.CreateSimpleMockBlock(1)
	err = bc.AddBlock(eb1)
	assert.NoError(t, err, "adding block 1 should have been successful")
	ok = bc.OnBlockStored(eb1.GetHeader().GetNumber())
	assert.True(t, ok, "cache should have been updated with storage event")

	eb2 := mocks.CreateSimpleMockBlock(2)
	err = bc.AddBlock(eb2)
	assert.NoError(t, err, "adding block 2 should have been successful")
	ok = bc.OnBlockStored(eb2.GetHeader().GetNumber())
	assert.True(t, ok, "cache should have been updated with storage event")

	_, ok = bc.LookupBlockByNumber(0)
	assert.True(t, ok, "block 0 should exist in cache")
	_, ok = bc.LookupBlockByNumber(1)
	assert.False(t, ok, "block 1 should not exist in cache")
	_, ok = bc.LookupBlockByNumber(2)
	assert.True(t, ok, "block 2 should exist in cache")
}

func TestConfigBlockPinned(t *testing.T) {
	bc := newBlockCache(1)

	eb0 := mocks.CreateSimpleMockBlock(0)
	err := bc.AddBlock(eb0)
	assert.NoError(t, err, "adding block 0 should have been successful")
	ok := bc.OnBlockStored(eb0.GetHeader().GetNumber())
	assert.True(t, ok, "cache should have been updated with storage event")

	eb1 := mocks.CreateSimpleConfigBlock(1)
	err = bc.AddBlock(eb1)
	assert.NoError(t, err, "adding block 1 should have been successful")
	ok = bc.OnBlockStored(eb1.GetHeader().GetNumber())
	assert.True(t, ok, "cache should have been updated with storage event")

	eb2 := mocks.CreateSimpleMockBlock(2)
	err = bc.AddBlock(eb2)
	assert.NoError(t, err, "adding block 2 should have been successful")
	ok = bc.OnBlockStored(eb2.GetHeader().GetNumber())
	assert.True(t, ok, "cache should have been updated with storage event")

	eb3 := mocks.CreateSimpleMockBlock(3)
	err = bc.AddBlock(eb3)
	assert.NoError(t, err, "adding block 3 should have been successful")
	ok = bc.OnBlockStored(eb3.GetHeader().GetNumber())
	assert.True(t, ok, "cache should have been updated with storage event")

	_, ok = bc.LookupBlockByNumber(0)
	assert.True(t, ok, "block 0 should exist in cache")
	_, ok = bc.LookupBlockByNumber(1)
	assert.True(t, ok, "block 1 should exist in cache")
	_, ok = bc.LookupBlockByNumber(2)
	assert.False(t, ok, "block 2 should not exist in cache")
	_, ok = bc.LookupBlockByNumber(3)
	assert.True(t, ok, "block 3 should exist in cache")

	eb4 := mocks.CreateSimpleConfigBlock(4)
	err = bc.AddBlock(eb4)
	assert.NoError(t, err, "adding block 4 should have been successful")
	ok = bc.OnBlockStored(eb4.GetHeader().GetNumber())
	assert.True(t, ok, "cache should have been updated with storage event")

	eb5 := mocks.CreateSimpleMockBlock(5)
	err = bc.AddBlock(eb5)
	assert.NoError(t, err, "adding block 5 should have been successful")
	ok = bc.OnBlockStored(eb5.GetHeader().GetNumber())
	assert.True(t, ok, "cache should have been updated with storage event")

	_, ok = bc.LookupBlockByNumber(0)
	assert.True(t, ok, "block 0 should exist in cache")
	_, ok = bc.LookupBlockByNumber(1)
	assert.False(t, ok, "block 1 should not exist in cache")
	_, ok = bc.LookupBlockByNumber(2)
	assert.False(t, ok, "block 2 should not exist in cache")
	_, ok = bc.LookupBlockByNumber(3)
	assert.False(t, ok, "block 3 should not exist in cache")
	_, ok = bc.LookupBlockByNumber(4)
	assert.True(t, ok, "block 4 should exist in cache")
	_, ok = bc.LookupBlockByNumber(5)
	assert.True(t, ok, "block 5 should exist in cache")
}