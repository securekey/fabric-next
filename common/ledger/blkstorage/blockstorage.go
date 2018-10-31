/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package blkstorage

import (
	"errors"

	"github.com/hyperledger/fabric/common/ledger"
	l "github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
)

// IndexableAttr represents an indexable attribute
type IndexableAttr string

// constants for indexable attributes
const (
	IndexableAttrBlockNum         = IndexableAttr("BlockNum")
	IndexableAttrBlockHash        = IndexableAttr("BlockHash")
	IndexableAttrTxID             = IndexableAttr("TxID")
	IndexableAttrBlockNumTranNum  = IndexableAttr("BlockNumTranNum")
	IndexableAttrBlockTxID        = IndexableAttr("BlockTxID")
	IndexableAttrTxValidationCode = IndexableAttr("TxValidationCode")
)

// IndexConfig - a configuration that includes a list of attributes that should be indexed
type IndexConfig struct {
	AttrsToIndex []IndexableAttr
}

var (
	// ErrNotFoundInIndex is used to indicate missing entry in the index
	ErrNotFoundInIndex = l.NotFoundInIndexErr("")

	// ErrAttrNotIndexed is used to indicate that an attribute is not indexed
	ErrAttrNotIndexed = errors.New("Attribute not indexed")
)

// BlockStoreProvider provides an handle to a BlockStore
type BlockStoreProvider interface {
	CreateBlockStore(ledgerid string) (BlockStore, error)
	OpenBlockStore(ledgerid string) (BlockStore, error)
	Exists(ledgerid string) (bool, error)
	List() ([]string, error)
	Close()
}

// BlockStore - an interface for persisting and retrieving blocks
// An implementation of this interface is expected to take an argument
// of type `IndexConfig` which configures the block store on what items should be indexed
type BlockStore interface {
	AddBlock(block *common.Block) error
	CheckpointBlock(block *common.Block) error
	GetBlockchainInfo() (*common.BlockchainInfo, error)
	RetrieveBlocks(startNum uint64) (ledger.ResultsIterator, error)
	RetrieveBlockByHash(blockHash []byte) (*common.Block, error)
	RetrieveBlockByNumber(blockNum uint64) (*common.Block, error) // blockNum of  math.MaxUint64 will return last block
	RetrieveTxByID(txID string) (*common.Envelope, error)
	RetrieveTxByBlockNumTranNum(blockNum uint64, tranNum uint64) (*common.Envelope, error)
	RetrieveBlockByTxID(txID string) (*common.Block, error)
	RetrieveTxValidationCodeByTxID(txID string) (peer.TxValidationCode, error)
	Shutdown()
}

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
	LookupBlockByNumber(number uint64) (*common.Block, bool)
	LookupBlockByHash(blockHash []byte) (*common.Block, bool)
	Shutdown()
}

// BlockIndexProvider provides an handle to a BlockIndex
type BlockIndexProvider interface {
	OpenBlockIndex(ledgerid string) (BlockIndex, error)
	Exists(ledgerid string) (bool, error)
	List() ([]string, error)
	Close()
}

type TxBlockLoc interface {
	BlockNumber() uint64
	TxNumber() uint64
	// TODO: make use of offset & length or remove.
	Offset() int
	Length() int
}

// BlockIndex - an interface for persisting and retrieving block & transaction metadata
type BlockIndex interface {
	AddBlock(block *common.Block) error
	Shutdown()
	//GetLastBlockIndexed() (uint64, error)
	GetTxLoc(txID string) (TxBlockLoc, error)
	GetTxValidationCodeByTxID(txID string) (peer.TxValidationCode, error)
	GetTxLocByBlockNumTranNum(blockNum uint64, tranNum uint64) (TxBlockLoc, error)
}
