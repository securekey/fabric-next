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
/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ldbblkindex

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/common/ledger/util"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	ledgerUtil "github.com/hyperledger/fabric/core/ledger/util"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
)

const (
	txIDIdxKeyPrefix               = 't'
	blockNumTranNumIdxKeyPrefix    = 'a'
	//blockTxIDIdxKeyPrefix          = 'b'
	txValidationResultIdxKeyPrefix = 'v'
	indexCheckpointKeyStr          = "indexCheckpointKey"
)

var indexCheckpointKey = []byte(indexCheckpointKeyStr)
var errIndexEmpty = errors.New("NoBlockIndexed")

type blockIdxInfo struct {
	blockNum  uint64
	blockHash []byte
	txOffsets []*txindexInfo
	metadata  *common.BlockMetadata
}

type blockIndex struct {
	indexItemsMap map[blkstorage.IndexableAttr]bool
	db            *leveldbhelper.DBHandle
}

func newBlockIndex(indexConfig *blkstorage.IndexConfig, db *leveldbhelper.DBHandle) (*blockIndex, error) {
	indexItems := indexConfig.AttrsToIndex
	logger.Debugf("newBlockIndex() - indexItems:[%s]", indexItems)
	indexItemsMap := make(map[blkstorage.IndexableAttr]bool)
	for _, indexItem := range indexItems {
		indexItemsMap[indexItem] = true
	}
	// This dependency is needed because the index 'IndexableAttrTxID' is used for detecting the duplicate txid
	// and the results are reused in the other two indexes. Ideally, all three index should be merged into one
	// for efficiency purpose - [FAB-10587]
	if (indexItemsMap[blkstorage.IndexableAttrTxValidationCode] || indexItemsMap[blkstorage.IndexableAttrBlockTxID]) &&
		!indexItemsMap[blkstorage.IndexableAttrTxID] {
		return nil, fmt.Errorf("dependent index [%s] is not enabled for [%s] or [%s]",
			blkstorage.IndexableAttrTxID, blkstorage.IndexableAttrTxValidationCode, blkstorage.IndexableAttrBlockTxID)
	}
	return &blockIndex{indexItemsMap, db}, nil
}

func (index *blockIndex) AddBlock(block *common.Block) error {
	_, info, err := serializeBlock(block)
	if err != nil {
		return fmt.Errorf("error while serializing block [%s]", err)
	}
	blockHash := block.Header.Hash()
	//Get the location / offset where each transaction starts in the block and where the block ends
	txOffsets := info.txOffsets
	if err != nil {
		return fmt.Errorf("error while serializing block [%s]", err)
	}
	//save the index in the database
	idxInfo := blockIdxInfo{
		blockNum: block.Header.Number,
		blockHash: blockHash,
		txOffsets: txOffsets,
		metadata: block.Metadata,
	}

	return index.indexBlock(&idxInfo)
}

func (index *blockIndex) Shutdown() {
}

func (index *blockIndex) RetrieveLastBlockIndexed() (uint64, error) {
	var blockNumBytes []byte
	var err error
	if blockNumBytes, err = index.db.Get(indexCheckpointKey); err != nil {
		return 0, err
	}
	if blockNumBytes == nil {
		return 0, errIndexEmpty
	}
	return decodeBlockNum(blockNumBytes), nil
}

func (index *blockIndex) indexBlock(blockIdxInfo *blockIdxInfo) error {
	// do not index anything
	if len(index.indexItemsMap) == 0 {
		logger.Debug("Not indexing block... as nothing to index")
		return nil
	}
	logger.Debugf("Indexing block [%s]", blockIdxInfo)
	txOffsets := blockIdxInfo.txOffsets
	txsfltr := ledgerUtil.TxValidationFlags(blockIdxInfo.metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER])
	batch := leveldbhelper.NewUpdateBatch()

	//Index3 Used to find a transaction by it's transaction id
	if _, ok := index.indexItemsMap[blkstorage.IndexableAttrTxID]; ok {
		if err := index.markDuplicateTxids(blockIdxInfo); err != nil {
			logger.Errorf("error while detecting duplicate txids:%s", err)
			return err
		}
		for _, txoffset := range txOffsets {
			if txoffset.isDuplicate { // do not overwrite txid entry in the index - FAB-8557
				logger.Debugf("txid [%s] is a duplicate of a previous tx. Not indexing in txid-index", txoffset.txID)
				continue
			}
			txLoc := txoffset.loc
			logger.Debugf("Adding txLoc [%s] for tx ID: [%s] to txid-index", txLoc, txoffset.txID)
			blockTxLoc := blockLocPointer{blockNumber:blockIdxInfo.blockNum,locPointer:txLoc}
			txFlpBytes, marshalErr := blockTxLoc.marshal()
			if marshalErr != nil {
				return marshalErr
			}
			batch.Put(constructTxIDKey(txoffset.txID), txFlpBytes)
		}
	}

	//Index4 - Store BlockNumTranNum will be used to query history data
	if _, ok := index.indexItemsMap[blkstorage.IndexableAttrBlockNumTranNum]; ok {
		for txIterator, txoffset := range txOffsets {
			txLoc := txoffset.loc
			logger.Debugf("Adding txLoc [%s] for tx number:[%d] ID: [%s] to blockNumTranNum index", txLoc, txIterator, txoffset.txID)
			blockTxLoc := blockLocPointer{blockNumber:blockIdxInfo.blockNum,locPointer:txLoc}
			txFlpBytes, marshalErr := blockTxLoc.marshal()
			if marshalErr != nil {
				return marshalErr
			}
			batch.Put(constructBlockNumTranNumKey(blockIdxInfo.blockNum, uint64(txIterator)), txFlpBytes)
		}
	}

	/*
	// The current implementation of TxLoc indices contains the block number.
	// Index5 - Store BlockNumber will be used to find block by transaction id
	if _, ok := index.indexItemsMap[blkstorage.IndexableAttrBlockTxID]; ok {
		for _, txoffset := range txOffsets {
			if txoffset.isDuplicate { // do not overwrite txid entry in the index - FAB-8557
				continue
			}
			batch.Put(constructBlockTxIDKey(txoffset.txID), encodeBlockNum(blockIdxInfo.blockNum))
		}
	}
	*/

	// Index6 - Store transaction validation result by transaction id
	if _, ok := index.indexItemsMap[blkstorage.IndexableAttrTxValidationCode]; ok {
		for idx, txoffset := range txOffsets {
			if txoffset.isDuplicate { // do not overwrite txid entry in the index - FAB-8557
				continue
			}
			batch.Put(constructTxValidationCodeIDKey(txoffset.txID), []byte{byte(txsfltr.Flag(idx))})
		}
	}

	batch.Put(indexCheckpointKey, encodeBlockNum(blockIdxInfo.blockNum))
	// Setting snyc to true as a precaution, false may be an ok optimization after further testing.
	if err := index.db.WriteBatch(batch, true); err != nil {
		return err
	}
	return nil
}

func (index *blockIndex) markDuplicateTxids(blockIdxInfo *blockIdxInfo) error {
	uniqueTxids := make(map[string]bool)
	for _, txIdxInfo := range blockIdxInfo.txOffsets {
		txid := txIdxInfo.txID
		if uniqueTxids[txid] { // txid is duplicate of a previous tx in the block
			txIdxInfo.isDuplicate = true
			continue
		}

		loc, err := index.RetrieveTxLoc(txid)
		if loc != nil { // txid is duplicate of a previous tx in the index
			txIdxInfo.isDuplicate = true
			continue
		}
		if err != blkstorage.ErrNotFoundInIndex {
			return err
		}
		uniqueTxids[txid] = true
	}
	return nil
}

func (index *blockIndex) RetrieveTxLoc(txID string) (blkstorage.TxLoc, error) {
	if _, ok := index.indexItemsMap[blkstorage.IndexableAttrTxID]; !ok {
		return nil, blkstorage.ErrAttrNotIndexed
	}
	b, err := index.db.Get(constructTxIDKey(txID))
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, blkstorage.ErrNotFoundInIndex
	}

	txFLP := newBlockLockPointer(b)
	return txFLP, nil
}

/*
// The current implementation of RetrieveTxLoc contains the block number.
func (index *blockIndex) RetrieveBlockNumberByTxID(txID string) (uint64, error) {
	if _, ok := index.indexItemsMap[blkstorage.IndexableAttrBlockTxID]; !ok {
		return 0, blkstorage.ErrAttrNotIndexed
	}
	b, err := index.db.Get(constructBlockTxIDKey(txID))
	if err != nil {
		return 0, err
	}
	if b == nil {
		return 0, blkstorage.ErrNotFoundInIndex
	}

	return decodeBlockNum(b), nil
}
*/

func (index *blockIndex) RetrieveTxLocByBlockNumTranNum(blockNum uint64, tranNum uint64) (blkstorage.TxLoc, error) {
	if _, ok := index.indexItemsMap[blkstorage.IndexableAttrBlockNumTranNum]; !ok {
		return nil, blkstorage.ErrAttrNotIndexed
	}
	b, err := index.db.Get(constructBlockNumTranNumKey(blockNum, tranNum))
	if err != nil {
		return nil, err
	}
	if b == nil {
		return nil, blkstorage.ErrNotFoundInIndex
	}
	txFLP := newBlockLockPointer(b)
	return txFLP, nil
}

func (index *blockIndex) RetrieveTxValidationCodeByTxID(txID string) (peer.TxValidationCode, error) {
	if _, ok := index.indexItemsMap[blkstorage.IndexableAttrTxValidationCode]; !ok {
		return peer.TxValidationCode(-1), blkstorage.ErrAttrNotIndexed
	}

	raw, err := index.db.Get(constructTxValidationCodeIDKey(txID))

	if err != nil {
		return peer.TxValidationCode(-1), err
	} else if raw == nil {
		return peer.TxValidationCode(-1), blkstorage.ErrNotFoundInIndex
	} else if len(raw) != 1 {
		return peer.TxValidationCode(-1), errors.New("Invalid value in indexItems")
	}

	result := peer.TxValidationCode(int32(raw[0]))

	return result, nil
}

func constructTxIDKey(txID string) []byte {
	return append([]byte{txIDIdxKeyPrefix}, []byte(txID)...)
}

/*
func constructBlockTxIDKey(txID string) []byte {
	return append([]byte{blockTxIDIdxKeyPrefix}, []byte(txID)...)
}
*/

func constructTxValidationCodeIDKey(txID string) []byte {
	return append([]byte{txValidationResultIdxKeyPrefix}, []byte(txID)...)
}

func constructBlockNumTranNumKey(blockNum uint64, txNum uint64) []byte {
	blkNumBytes := util.EncodeOrderPreservingVarUint64(blockNum)
	tranNumBytes := util.EncodeOrderPreservingVarUint64(txNum)
	key := append(blkNumBytes, tranNumBytes...)
	return append([]byte{blockNumTranNumIdxKeyPrefix}, key...)
}

func encodeBlockNum(blockNum uint64) []byte {
	return proto.EncodeVarint(blockNum)
}

func decodeBlockNum(blockNumBytes []byte) uint64 {
	blockNum, _ := proto.DecodeVarint(blockNumBytes)
	return blockNum
}

type blockLocPointer struct {
	blockNumber uint64
	*locPointer
}

type locPointer struct {
	txnNumber   uint64
	offset      int
	bytesLength int
}

func (lp *locPointer) TxNumber() uint64 {
	return lp.txnNumber
}

func (lp *locPointer) Offset() int {
	return lp.offset
}

func (lp *locPointer) Length() int {
	return lp.bytesLength
}

func (lp *locPointer) String() string {
	return fmt.Sprintf("offset=%d, bytesLength=%d",
		lp.offset, lp.bytesLength)
}

func (lp *blockLocPointer) BlockNumber() uint64 {
	return lp.blockNumber
}

func (lp *blockLocPointer) marshal() ([]byte, error) {
	buffer := proto.NewBuffer([]byte{})
	err := buffer.EncodeVarint(lp.blockNumber)
	if err != nil {
		return nil, err
	}
	err = buffer.EncodeVarint(lp.txnNumber)
	if err != nil {
		return nil, err
	}
	err = buffer.EncodeVarint(uint64(lp.offset))
	if err != nil {
		return nil, err
	}
	err = buffer.EncodeVarint(uint64(lp.bytesLength))
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func newBlockLockPointer(b []byte) *blockLocPointer {
	blp := blockLocPointer{
		blockNumber: 0,
		locPointer: &locPointer{},
	}

	blp.unmarshal(b)
	return &blp
}

func (lp *blockLocPointer) unmarshal(b []byte) error {
	lp.locPointer = &locPointer{}

	buffer := proto.NewBuffer(b)
	i, err := buffer.DecodeVarint()
	if err != nil {
		return err
	}
	lp.blockNumber = i
	i, err = buffer.DecodeVarint()
	if err != nil {
		return err
	}
	lp.txnNumber = i
	i, err = buffer.DecodeVarint()
	if err != nil {
		return err
	}
	lp.offset = int(i)
	i, err = buffer.DecodeVarint()
	if err != nil {
		return err
	}
	lp.bytesLength = int(i)
	return nil
}

func (lp *blockLocPointer) String() string {
	return fmt.Sprintf("blockNumber=%d,%s", lp.blockNumber, lp.locPointer.String())
}

func (blockIdxInfo *blockIdxInfo) String() string {

	var buffer bytes.Buffer
	for _, txOffset := range blockIdxInfo.txOffsets {
		buffer.WriteString("txId=")
		buffer.WriteString(txOffset.txID)
		buffer.WriteString(" locPointer=")
		buffer.WriteString(txOffset.loc.String())
		buffer.WriteString("\n")
	}
	txOffsetsString := buffer.String()

	return fmt.Sprintf("blockNum=%d, blockHash=%#v txOffsets=\n%s", blockIdxInfo.blockNum, blockIdxInfo.blockHash, txOffsetsString)
}
