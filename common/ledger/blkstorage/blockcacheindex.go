/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blkstorage

import (
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
)

// BlockCacheProvider provides an handle to a BlockCache
type BlockCacheProvider interface {
	OpenBlockCache(ledgerid string) (BlockCache, error)
	Exists(ledgerid string) (bool, error)
	List() ([]string, error)
	Close()
}

// BlockCache - an interface for persisting and retrieving blocks from a cache
type BlockCache interface {
	AddBlock(block *common.Block) error
	OnBlockStored(blockNum uint64) bool
	LookupBlockByNumber(number uint64) (*common.Block, bool)
	LookupBlockByHash(blockHash []byte) (*common.Block, bool)
	LookupTxLoc(id string) (TxLoc, bool)
	Shutdown()
}

// BlockIndexProvider provides an handle to a BlockIndex
type BlockIndexProvider interface {
	OpenBlockIndex(ledgerid string) (BlockIndex, error)
	Exists(ledgerid string) (bool, error)
	List() ([]string, error)
	Close()
}

type TxLoc interface {
	BlockNumber() uint64
	TxNumber() uint64
	// TODO: make use of offset & length or remove.
	//Offset() int
	//Length() int
}

// BlockIndex - an interface for persisting and retrieving block & transaction metadata
type BlockIndex interface {
	AddBlock(block *common.Block) error
	Shutdown()
	//RetrieveLastBlockIndexed() (uint64, error)
	RetrieveTxLoc(txID string) (TxLoc, error)
	// TODO: make us of RetrieveTxLocByBlockNumTranNum or remove.
	//RetrieveTxLocByBlockNumTranNum(blockNum uint64, tranNum uint64) (TxLoc, error)
	RetrieveTxValidationCodeByTxID(txID string) (peer.TxValidationCode, error)
}

