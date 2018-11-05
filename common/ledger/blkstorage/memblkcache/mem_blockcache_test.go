/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package memblkcache

import (
	"github.com/hyperledger/fabric/common/ledger/blkstorage/mocks"
	"github.com/hyperledger/fabric/protos/peer"
	"github.com/stretchr/testify/assert"
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

	const txnID0="a"
	const txnID1="b"

	b0 := mocks.CreateBlock(0, mocks.NewTransactionWithMockKey(txnID0, "some-key", peer.TxValidationCode_VALID))
	b1 := mocks.CreateBlock(1, mocks.NewTransactionWithMockKey(txnID1, "some-key", peer.TxValidationCode_VALID))
	_, ok := bc.LookupTxLoc(txnID0)
	assert.False(t, ok, "block 0 should not exist in cache")

	bc.AddBlock(b0)
	bc.AddBlock(b1)
	atx0, ok := bc.LookupTxLoc(txnID0)
	assert.True(t, ok, "block 0 should exist in cache")
	assert.Equal(t, uint64(0), atx0.BlockNumber(), "retrieved block should match added block")
	assert.Equal(t, uint64(0), atx0.TxNumber(), "retrieved block should match added block")

	atx1, ok := bc.LookupTxLoc(txnID1)
	assert.True(t, ok, "block 1 should exist in cache")
	assert.Equal(t, uint64(1), atx1.BlockNumber(), "retrieved block should match added block")
	assert.Equal(t, uint64(0), atx1.TxNumber(), "retrieved block should match added block")
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
}