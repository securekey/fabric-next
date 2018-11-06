/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ldbblkindex

import (
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/common/ledger/blkstorage/mocks"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/protos/peer"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
)

func TestAddBlock(t *testing.T) {
	bi, cleanup, err := newMockMockBlockIndex()
	defer cleanup()
	assert.NoError(t, err, "block index should have been created")

	eb0 := mocks.CreateSimpleMockBlock(0)
	err = bi.AddBlock(eb0)
	assert.NoError(t, err, "adding block 0 should have been successful")

	num, err := bi.RetrieveLastBlockIndexed()
	assert.NoError(t, err, "last block indexed should have been successful")
	assert.Equal(t, uint64(0), num, "block 0 should have been indexed")

	eb1 := mocks.CreateSimpleMockBlock(1)
	err = bi.AddBlock(eb1)
	assert.NoError(t, err, "adding block 1 should have been successful")

	num, err = bi.RetrieveLastBlockIndexed()
	assert.NoError(t, err, "last block indexed should have been successful")
	assert.Equal(t, uint64(1), num, "block 1 should have been indexed")
}

func TestRetrieveTxLoc(t *testing.T) {
	bi, cleanup, err := newMockMockBlockIndex()
	defer cleanup()
	assert.NoError(t, err, "block index should have been created")

	const (
		txnID0 = "a"
		txnID1 = "b"
		txnID2 = "c"
		txnID3 = "d"
		txnID4 = "e"
	)

	b0 := mocks.CreateBlock(0,
		mocks.NewTransactionWithMockKey(txnID0, "some-keya", peer.TxValidationCode_VALID),
		mocks.NewTransactionWithMockKey(txnID1, "another-keyb", peer.TxValidationCode_VALID),
		mocks.NewTransactionWithMockKey(txnID2, "yet-another-keyc", peer.TxValidationCode_MVCC_READ_CONFLICT),
	)
	b1 := mocks.CreateBlock(1,
		mocks.NewTransactionWithMockKey(txnID3, "my-keyd", peer.TxValidationCode_VALID),
		mocks.NewTransactionWithMockKey(txnID4, "a-keyd", peer.TxValidationCode_BAD_PROPOSAL_TXID),
	)
	_, err = bi.RetrieveTxLoc(txnID0)
	assert.Error(t, err, "txn 0 should not exist in index")

	err = bi.AddBlock(b0)
	assert.NoError(t, err, "adding block 0 should have been successful")

	_, err = bi.RetrieveTxLoc(txnID3)
	assert.Error(t, err, "txn 3 should not exist in index")

	err = bi.AddBlock(b1)
	assert.NoError(t, err, "adding block 1 should have been successful")

	// txn 0
	em0 := txMetadata{
		blockNumber: 0,
		txValidationCode: peer.TxValidationCode_VALID,
		locPointer: &locPointer{
			txnNumber: 0,
			offset: 4,
			bytesLength: 85,
		},
	}

	am0, err := bi.retrieveTxMetadata(txnID0)
	assert.NoError(t, err, "txn 0 should exist in index")
	assertTxMetadata(t, &em0, am0)

	al0, err := bi.RetrieveTxLoc(txnID0)
	assert.NoError(t, err, "txn 0 should exist in index")
	assertTxLoc(t, &em0, al0)

	// txn 1
	em1 := txMetadata{
		blockNumber: 0,
		txValidationCode: peer.TxValidationCode_VALID,
		locPointer: &locPointer{
			txnNumber: 1,
			offset: 89,
			bytesLength: 88,
		},
	}

	am1, err := bi.retrieveTxMetadata(txnID1)
	assert.NoError(t, err, "txn 1 should exist in index")
	assertTxMetadata(t, &em1, am1)

	al1, err := bi.RetrieveTxLoc(txnID1)
	assert.NoError(t, err, "txn 1 should exist in index")
	assertTxLoc(t, &em1, al1)

	// txn 2
	em2 := txMetadata{
		blockNumber: 0,
		txValidationCode: peer.TxValidationCode_MVCC_READ_CONFLICT,
		locPointer: &locPointer{
			txnNumber: 2,
			offset: 177,
			bytesLength: 92,
		},
	}

	am2, err := bi.retrieveTxMetadata(txnID2)
	assert.NoError(t, err, "txn 2 should exist in index")
	assertTxMetadata(t, &em2, am2)

	al2, err := bi.RetrieveTxLoc(txnID2)
	assert.NoError(t, err, "txn 2 should exist in index")
	assertTxLoc(t, &em2, al2)

	// txn 3
	em3 := txMetadata{
		blockNumber: 1,
		txValidationCode: peer.TxValidationCode_VALID,
		locPointer: &locPointer{
			txnNumber: 0,
			offset: 4,
			bytesLength: 83,
		},
	}

	am3, err := bi.retrieveTxMetadata(txnID3)
	assert.NoError(t, err, "txn 3 should exist in index")
	assertTxMetadata(t, &em3, am3)

	al3, err := bi.RetrieveTxLoc(txnID3)
	assert.NoError(t, err, "txn 3 should exist in index")
	assertTxLoc(t, &em3, al3)

	// txn 4
	em4 := txMetadata{
		blockNumber: 1,
		txValidationCode: peer.TxValidationCode_BAD_PROPOSAL_TXID,
		locPointer: &locPointer{
			txnNumber: 1,
			offset: 87,
			bytesLength: 82,
		},
	}

	am4, err := bi.retrieveTxMetadata(txnID4)
	assert.NoError(t, err, "txn 4 should exist in index")
	assertTxMetadata(t, &em4, am4)

	al4, err := bi.RetrieveTxLoc(txnID4)
	assert.NoError(t, err, "txn 4 should exist in index")
	assertTxLoc(t, &em4, al4)
}

func assertTxMetadata(t *testing.T, e *txMetadata, a *txMetadata) {
	assert.Equal(t, e.blockNumber, a.blockNumber, "unexpected block number")
	assert.Equal(t, e.txValidationCode, a.txValidationCode, "unexpected transaction validation code")
	assert.Equal(t, e.locPointer, a.locPointer, "unexpected transaction location pointer")
}

func assertTxLoc(t *testing.T, e blkstorage.TxLoc, a blkstorage.TxLoc) {
	assert.Equal(t, e.BlockNumber(), a.BlockNumber(), "unexpected transaction location (block number)")
	assert.Equal(t, e.TxNumber(), a.TxNumber(), "unexpected transaction location (tx number)")
}


func TestDuplicateTx(t *testing.T) {
	bi, cleanup, err := newMockMockBlockIndex()
	defer cleanup()
	assert.NoError(t, err, "block index should have been created")

	const (
		txnID0 = "a"
		txnID1 = "b"
		txnID2 = "c"
	)

	b0 := mocks.CreateBlock(0,
		mocks.NewTransactionWithMockKey(txnID0, "some-keya", peer.TxValidationCode_VALID),
		mocks.NewTransactionWithMockKey(txnID1, "another-keyb", peer.TxValidationCode_VALID),
		mocks.NewTransactionWithMockKey(txnID0, "yet-another-keyc", peer.TxValidationCode_MVCC_READ_CONFLICT),
	)
	b1 := mocks.CreateBlock(1,
		mocks.NewTransactionWithMockKey(txnID2, "my-keyd", peer.TxValidationCode_VALID),
		mocks.NewTransactionWithMockKey(txnID1, "a-keyd", peer.TxValidationCode_BAD_PROPOSAL_TXID),
	)

	err = bi.AddBlock(b0)
	assert.NoError(t, err, "adding block 0 should have been successful")

	// txn 0
	em0 := txMetadata{
		blockNumber: 0,
		txValidationCode: peer.TxValidationCode_VALID,
		locPointer: &locPointer{
			txnNumber: 0,
			offset: 4,
			bytesLength: 85,
		},
	}

	am0, err := bi.retrieveTxMetadata(txnID0)
	assert.NoError(t, err, "txn 0 should exist in index")
	assertTxMetadata(t, &em0, am0)

	// txn 1
	em1 := txMetadata{
		blockNumber: 0,
		txValidationCode: peer.TxValidationCode_VALID,
		locPointer: &locPointer{
			txnNumber: 1,
			offset: 89,
			bytesLength: 88,
		},
	}

	am1, err := bi.retrieveTxMetadata(txnID1)
	assert.NoError(t, err, "txn 1 should exist in index")
	assertTxMetadata(t, &em1, am1)

	err = bi.AddBlock(b1)
	assert.NoError(t, err, "adding block 1 should have been successful")

	am1, err = bi.retrieveTxMetadata(txnID1)
	assert.NoError(t, err, "txn 1 should exist in index")
	assertTxMetadata(t, &em1, am1)
}

func TestRetrieveTxValidationCodeByTxID(t *testing.T) {
	bi, cleanup, err := newMockMockBlockIndex()
	defer cleanup()
	assert.NoError(t, err, "block index should have been created")

	const (
		txnID0 = "a"
		txnID1 = "b"
		txnID2 = "c"
		txnID3 = "d"
		txnID4 = "e"
	)

	b0 := mocks.CreateBlock(0,
		mocks.NewTransactionWithMockKey(txnID0, "some-keya", peer.TxValidationCode_VALID),
		mocks.NewTransactionWithMockKey(txnID1, "another-keyb", peer.TxValidationCode_VALID),
		mocks.NewTransactionWithMockKey(txnID2, "yet-another-keyc", peer.TxValidationCode_MVCC_READ_CONFLICT),
	)
	b1 := mocks.CreateBlock(1,
		mocks.NewTransactionWithMockKey(txnID0, "some-keya", peer.TxValidationCode_MVCC_READ_CONFLICT),
		mocks.NewTransactionWithMockKey(txnID3, "my-keyd", peer.TxValidationCode_VALID),
		mocks.NewTransactionWithMockKey(txnID4, "a-keyd", peer.TxValidationCode_BAD_PROPOSAL_TXID),
	)

	_, err = bi.RetrieveTxValidationCodeByTxID(txnID0)
	assert.Error(t, err, "txn 0 should not exist in index")

	err = bi.AddBlock(b0)
	assert.NoError(t, err, "adding block 0 should have been successful")

	tvc0, err := bi.RetrieveTxValidationCodeByTxID(txnID0)
	assert.NoError(t, err, "txn 0 should exist in index")
	assert.Equal(t, peer.TxValidationCode_VALID, tvc0)

	_, err = bi.RetrieveTxValidationCodeByTxID(txnID3)
	assert.Error(t, err, "txn 3 should not exist in index")

	err = bi.AddBlock(b1)
	assert.NoError(t, err, "adding block 1 should have been successful")

	tvc0, err = bi.RetrieveTxValidationCodeByTxID(txnID0)
	assert.NoError(t, err, "txn 0 should exist in index")
	assert.Equal(t, peer.TxValidationCode_VALID, tvc0)

	tvc1, err := bi.RetrieveTxValidationCodeByTxID(txnID1)
	assert.NoError(t, err, "txn 1 should exist in index")
	assert.Equal(t, peer.TxValidationCode_VALID, tvc1)

	tvc2, err := bi.RetrieveTxValidationCodeByTxID(txnID2)
	assert.NoError(t, err, "txn 2 should exist in index")
	assert.Equal(t, peer.TxValidationCode_MVCC_READ_CONFLICT, tvc2)

	tvc3, err := bi.RetrieveTxValidationCodeByTxID(txnID3)
	assert.NoError(t, err, "txn 3 should exist in index")
	assert.Equal(t, peer.TxValidationCode_VALID, tvc3)

	tvc4, err := bi.RetrieveTxValidationCodeByTxID(txnID4)
	assert.NoError(t, err, "txn 4 should exist in index")
	assert.Equal(t, peer.TxValidationCode_BAD_PROPOSAL_TXID, tvc4)
}

func newMockMockBlockIndex() (*blockIndex, func(), error) {
	tempDir, err := ioutil.TempDir("", "fabric")
	if err != nil {
		return nil, nil, err
	}

	ldbp := leveldbhelper.NewProvider(&leveldbhelper.Conf{DBPath: tempDir})
	dbh := ldbp.GetDBHandle("some-ledger")

	bic := blkstorage.IndexConfig{
		AttrsToIndex: []blkstorage.IndexableAttr{blkstorage.IndexableAttrTxID},
	}
	bi, err := newBlockIndex(&bic, dbh)
	if err != nil {
		return nil, nil, err
	}

	cleanup := func() {
		os.RemoveAll(tempDir)
	}

	return bi, cleanup, nil
}