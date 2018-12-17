/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mocks

import (
	"encoding/hex"
	"github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	cledger "github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
)

// Mock implementation of blkstorage.BlockStore.
type MockBlockStore struct {
	// For RetrieveTxByBlockNumTranNum().
	EnvelopeByBlknumTxNum map[uint64]map[uint64]*common.Envelope
	BlocksByNumber        map[uint64]*common.Block
	BlocksByHash          map[string]*common.Block
	ItrBlocks             []*common.Block
	LastBlockAdd          *common.Block
	LastBlockCheckpoint   *common.Block
	BlockchainInfo        *common.BlockchainInfo
	IsShutdown            bool
}

func NewMockBlockStore() *MockBlockStore {
	envelopeByBlknumTxNum := make(map[uint64]map[uint64]*common.Envelope)
	blocksByNumber := make(map[uint64]*common.Block)
	blocksByHash := make(map[string]*common.Block)

	mbs := MockBlockStore{
		EnvelopeByBlknumTxNum: envelopeByBlknumTxNum,
		BlocksByNumber:        blocksByNumber,
		BlocksByHash:          blocksByHash,
	}

	return &mbs
}

// BlockStore.AddBlock()
func (mock *MockBlockStore) AddBlock(block *common.Block) error {
	mock.LastBlockAdd = block
	return nil
}

// BlockStore.CheckpointBlock()
func (mock *MockBlockStore) CheckpointBlock(block *common.Block) error {
	mock.LastBlockCheckpoint = block
	return nil
}

// BlockStore.GetBlockchainInfo()
func (mock *MockBlockStore) GetBlockchainInfo() (*common.BlockchainInfo, error) {
	return mock.BlockchainInfo, nil
}

// BlockStore.RetrieveBLockByHash()
func (mock *MockBlockStore) RetrieveBlockByHash(blockHash []byte) (*common.Block, error) {
	blockHashHex := hex.EncodeToString(blockHash)
	b, ok := mock.BlocksByHash[blockHashHex]
	if !ok {
		return nil, blkstorage.ErrAttrNotIndexed
	}

	return b, nil
}

// BlockStore.RetrieveBlockByNumber()
func (mock *MockBlockStore) RetrieveBlockByNumber(blockNum uint64) (*common.Block, error) {
	b, ok := mock.BlocksByNumber[blockNum]
	if !ok {
		return nil, blkstorage.ErrAttrNotIndexed
	}

	return b, nil
}

// BlockStore.RetrieveBlockByTxID()
func (mock *MockBlockStore) RetrieveBlockByTxID(txID string) (*common.Block, error) {
	panic("implement me")
}

// BlockStore.RetrieveBlocks()
func (mock *MockBlockStore) RetrieveBlocks(startNum uint64) (ledger.ResultsIterator, error) {
	return &MockBlockStoreItr{Blocks: mock.ItrBlocks}, nil
}

type MockBlockStoreItr struct {
	Blocks []*common.Block
	Pos    int
}

func (m *MockBlockStoreItr) Next() (ledger.QueryResult, error) {
	if m.Pos >= len(m.Blocks) {
		return nil, nil
	}

	b := m.Blocks[m.Pos]
	m.Pos++
	return b, nil
}

func (m *MockBlockStoreItr) Close() {
}

// BlockStore.RetrieveTxByBlockNumTranNum()
func (mock *MockBlockStore) RetrieveTxByBlockNumTranNum(blockNum uint64, tranNum uint64) (*common.Envelope, error) {
	if trxs, found := mock.EnvelopeByBlknumTxNum[blockNum]; found {
		if envelope, found := trxs[tranNum]; found {
			return envelope, nil
		}
	}
	return nil, nil
}

// BlockStore.RetrieveTxByID()
func (mock *MockBlockStore) RetrieveTxByID(txID string, hints ...cledger.SearchHint) (*common.Envelope, error) {
	panic("implement me")
}

// BlockStore.RetrieveTxValidationCodeByTxID()
func (mock *MockBlockStore) RetrieveTxValidationCodeByTxID(txID string) (peer.TxValidationCode, error) {
	panic("implement me")
}

// BlockStore.Shutdown()
func (mock *MockBlockStore) Shutdown() {
	mock.IsShutdown = true
}
