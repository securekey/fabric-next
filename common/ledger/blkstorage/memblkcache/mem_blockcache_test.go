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
	bc.AddBlock(eb0)
	assert.Equal(t, bc.blocks.Len(), 1, "adding a block should increase the blocks length")

	key := eb0.GetHeader().Number
	ab0, ok := bc.blocks.Get(key)
	assert.True(t, ok, "block 0 should exist in cache")
	assert.Equal(t, eb0, ab0, "retrieved block should match added block")
}

func TestLookupBlockByNumber(t *testing.T) {
	bc := newBlockCache(0)

	_, ok := bc.LookupBlockByNumber(0)
	assert.False(t, ok, "block 0 should not exist in cache")

	eb0 := mocks.CreateSimpleMockBlock(0)
	bc.AddBlock(eb0)

	ab0, ok := bc.LookupBlockByNumber(0)
	assert.True(t, ok, "block 0 should exist in cache")
	assert.Equal(t, eb0, ab0, "retrieved block should match added block")
}

func TestLookupBlockByHash(t *testing.T) {
	bc := newBlockCache(0)

	eb0 := mocks.CreateSimpleMockBlock(0)
	hb0 := eb0.GetHeader().Hash()
	_, ok := bc.LookupBlockByHash(hb0)
	assert.False(t, ok, "block 0 should not exist in cache")

	bc.AddBlock(eb0)

	ab0, ok := bc.LookupBlockByHash(hb0)
	assert.True(t, ok, "block 0 should exist in cache")
	assert.Equal(t, eb0, ab0, "retrieved block should match added block")
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
	assert.False(t, ok, "block 0 should not exist in cache")

	bc.AddBlock(b0)
	bc.AddBlock(b1)
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

	eb0 := mocks.CreateSimpleMockBlock(0)
	bc.AddBlock(eb0)

	_, ok = bc.LookupBlockByNumber(0)
	assert.True(t, ok, "block 0 should exist in cache")

	eb1 := mocks.CreateSimpleMockBlock(1)
	bc.AddBlock(eb1)

	eb2 := mocks.CreateSimpleMockBlock(2)
	bc.AddBlock(eb2)

	_, ok = bc.LookupBlockByNumber(0)
	assert.False(t, ok, "block 0 should have been evicted from cache")

	_, ok = bc.LookupBlockByNumber(2)
	assert.True(t, ok, "block 2 should exist in cache")
	_, ok = bc.LookupBlockByNumber(1)
	assert.True(t, ok, "block 1 should exist in cache")

	eb3 := mocks.CreateSimpleMockBlock(3)
	bc.AddBlock(eb3)

	_, ok = bc.LookupBlockByNumber(2)
	assert.False(t, ok, "block 2 should have been evicted from cache")
}

func TestEvictionCleanup(t *testing.T) {
	bc := newBlockCache(1)

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

	assert.Equal(t, 0, bc.blocks.Len(), "blocks should be empty")
	assert.Equal(t, 0, len(bc.hashToNumber), "hashToNumber index should be empty")
	assert.Equal(t, 0, len(bc.numberToHash), "numberToHash index should be empty")
	assert.Equal(t, 0, len(bc.numberToTxnIDs), "numberToTxnIDs index should be empty")
	assert.Equal(t, 0, len(bc.txnLocs), "txnLocs index should be empty")

	bc.AddBlock(b0)
	assert.Equal(t, 1, bc.blocks.Len(), "blocks should have one entry")
	assert.Equal(t, 1, len(bc.hashToNumber), "hashToNumber index should have one entry")
	assert.Equal(t, 1, len(bc.numberToHash), "numberToHash index should have one entry")
	assert.Equal(t, 1, len(bc.numberToTxnIDs), "numberToTxnIDs index should have one entry")
	assert.Equal(t, 2, len(bc.txnLocs), "txnLocs index should have two entries")

	numberToTxnIDs := bc.numberToTxnIDs[0]
	sort.Strings(numberToTxnIDs)
	assert.Equal(t, []string{txnID0, txnID1}, numberToTxnIDs, "expected txn 1 & 2 in numberToTxnIDs index")

	k0 := b0.GetHeader().Number
	_, ok := bc.blocks.Get(k0)
	assert.True(t, ok, "block 0 should have been inserted into blocks cache")
	h0 := hex.EncodeToString(b0.GetHeader().Hash())
	_, ok = bc.hashToNumber[h0]
	assert.True(t, ok, "block 0 should have been inserted into hashToNumber index")
	_, ok = bc.numberToHash[0]
	assert.True(t, ok, "block 0 should have been inserted into numberToHash index")
	_, ok = bc.numberToTxnIDs[0]
	assert.True(t, ok, "block 0 should have been inserted into numberToTxnIDs index")
	_, ok = bc.txnLocs[txnID0]
	assert.True(t, ok, "txn 0 should have been inserted into txnLocs index")
	_, ok = bc.txnLocs[txnID1]
	assert.True(t, ok, "txn 1 should have been inserted into txnLocs index")

	bc.AddBlock(b1)
	assert.Equal(t, 1, bc.blocks.Len(), "blocks should have one entry")
	assert.Equal(t, 1, len(bc.hashToNumber), "hashToNumber index should have one entry")
	assert.Equal(t, 1, len(bc.numberToHash), "numberToHash index should have one entry")
	assert.Equal(t, 1, len(bc.numberToTxnIDs), "numberToTxnIDs index should have one entry")
	assert.Equal(t, 1, len(bc.txnLocs), "txnLocs index should have one entry")

	assert.Equal(t, []string{txnID2}, bc.numberToTxnIDs[1], "expected txn 2 in numberToTxnIDs index")

	_, ok = bc.blocks.Get(k0)
	assert.False(t, ok, "block 0 should have been evicted from blocks cache")
	_, ok = bc.hashToNumber[h0]
	assert.False(t, ok, "block 0 should have been evicted from hashToNumber index")
	_, ok = bc.numberToHash[0]
	assert.False(t, ok, "block 0 should have been evicted from numberToHash index")
	_, ok = bc.numberToTxnIDs[0]
	assert.False(t, ok, "block 0 should have been evicted from numberToTxnIDs index")
	_, ok = bc.txnLocs[txnID0]
	assert.False(t, ok, "txn 0 should have been evicted from txnLocs index")
	_, ok = bc.txnLocs[txnID1]
	assert.False(t, ok, "txn 1 should have been evicted from txnLocs index")

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
	_, ok = bc.txnLocs[txnID2]
	assert.True(t, ok, "txn 2 should have been inserted into txnLocs index")

}