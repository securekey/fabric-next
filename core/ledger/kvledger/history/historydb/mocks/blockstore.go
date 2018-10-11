package mocks

import (
	"github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
)

// Mock implementation of blkstorage.BlockStore.
type MockBlockStore struct {
	// For RetrieveTxByBlockNumTranNum().
	EnvelopeByBlknumTxNum map[uint64]map[uint64]*common.Envelope
}

// BlockStore.AddBlock()
func (mock MockBlockStore) AddBlock(block *common.Block) error {
	panic("implement me")
}

// BlockStore.CheckpointBlock()
func (mock MockBlockStore) CheckpointBlock(block *common.Block) error {
	panic("implement me")
}

// BlockStore.GetBlockchainInfo()
func (mock MockBlockStore) GetBlockchainInfo() (*common.BlockchainInfo, error) {
	panic("implement me")
}

// BlockStore.RetrieveBLockByHash()
func (mock MockBlockStore) RetrieveBlockByHash(blockHash []byte) (*common.Block, error) {
	panic("implement me")
}

// BlockStore.RetrieveBlockByNumber()
func (mock MockBlockStore) RetrieveBlockByNumber(blockNum uint64) (*common.Block, error) {
	panic("implement me")
}

// BlockStore.RetrieveBlockByTxID()
func (mock MockBlockStore) RetrieveBlockByTxID(txID string) (*common.Block, error) {
	panic("implement me")
}

// BlockStore.RetrieveBlocks()
func (mock MockBlockStore) RetrieveBlocks(startNum uint64) (ledger.ResultsIterator, error) {
	panic("implement me")
}

// BlockStore.RetrieveTxByBlockNumTranNum()
func (mock MockBlockStore) RetrieveTxByBlockNumTranNum(blockNum uint64, tranNum uint64) (*common.Envelope, error) {
	if trxs, found := mock.EnvelopeByBlknumTxNum[blockNum]; found {
		if envelope, found := trxs[tranNum]; found {
			return envelope, nil
		}
	}
	return nil, nil
}

// BlockStore.RetrieveTxByID()
func (mock MockBlockStore) RetrieveTxByID(txID string) (*common.Envelope, error) {
	panic("implement me")
}

// BlockStore.RetrieveTxValidationCodeByTxID()
func (mock MockBlockStore) RetrieveTxValidationCodeByTxID(txID string) (peer.TxValidationCode, error) {
	panic("implement me")
}

// BlockStore.Shutdown()
func (mock MockBlockStore) Shutdown() {
	panic("implement me")
}

