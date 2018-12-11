/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mocks

import (
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
)

type MockBlockIndex struct{
	TxLocsByTxID           map[string]blkstorage.TxLoc
	TxLocsByNum            map[uint64]map[uint64]blkstorage.TxLoc
	TxValidationCodeByTxID map[string]peer.TxValidationCode
	LastBlockAdd           *common.Block
	IsShutdown             bool
}

type MockTXLoc struct {
	MockBlockNumber uint64
	MockTxNumber    uint64
}

func (m *MockTXLoc) BlockNumber() uint64 {
	return m.MockBlockNumber
}

func (m *MockTXLoc) TxNumber() uint64 {
	return m.MockTxNumber
}

func NewMockBlockIndex() *MockBlockIndex {
	txLocsByTxID := make(map[string]blkstorage.TxLoc)
	txLocsByNum := make(map[uint64]map[uint64]blkstorage.TxLoc)
	txValidationCodeByTxID := make(map[string]peer.TxValidationCode)

	mbi := MockBlockIndex{
		TxLocsByTxID: txLocsByTxID,
		TxLocsByNum: txLocsByNum,
		TxValidationCodeByTxID: txValidationCodeByTxID,
	}

	return &mbi
}

func (m *MockBlockIndex) AddBlock(block *common.Block) error {
	m.LastBlockAdd = block
	return nil
}

func (m *MockBlockIndex) Shutdown() {
	m.IsShutdown = true
}

func (m *MockBlockIndex) RetrieveTxLoc(txID string) (blkstorage.TxLoc, error) {
	l, ok := m.TxLocsByTxID[txID]
	if !ok {
		return nil, blkstorage.ErrAttrNotIndexed
	}

	return l, nil
}

func (m *MockBlockIndex) RetrieveTxLocByBlockNumTranNum(blockNum uint64, tranNum uint64) (blkstorage.TxLoc, error) {
	b, ok := m.TxLocsByNum[blockNum]
	if !ok {
		return nil, blkstorage.ErrAttrNotIndexed
	}

	l, ok := b[tranNum]
	if !ok {
		return nil, blkstorage.ErrAttrNotIndexed
	}

	return l, nil
}

func (m *MockBlockIndex) RetrieveTxValidationCodeByTxID(txID string) (peer.TxValidationCode, error) {
	c, ok := m.TxValidationCodeByTxID[txID]
	if !ok {
		return peer.TxValidationCode_INVALID_OTHER_REASON, blkstorage.ErrAttrNotIndexed
	}

	return c, nil
}
