package ldbblkindex

import (
	"github.com/hyperledger/fabric/protos/common"
)

type indexedTxn struct {
	blockNumber   uint64
	blockPosition int
}

type txnIndex struct {
}

func newTxnIndex() *txnIndex {
	index := txnIndex{}

	return &index
}

func (c *txnIndex) Add(block *common.Block) {
}

func (c *txnIndex) LookupTxnBlockNumber(id string) (uint64, bool) {
	return 0, false
}