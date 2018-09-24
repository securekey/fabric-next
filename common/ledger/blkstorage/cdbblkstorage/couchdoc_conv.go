/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbblkstorage

import (
	"encoding/hex"
	"encoding/json"
	"github.com/gogo/protobuf/proto"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/pkg/errors"
	"strconv"
)

const (
	idField             = "_id"
	blockHashField      = "hash"
	blockTxnsField      = "transactions"
	blockTxnIDField     = "id"
	blockHashIndexName  = "by_hash"
	blockHashIndexDoc   = "indexHash"
	blockTxnIDIndexName  = "by_txn_id"
	blockTxnIndexDoc   = "indexTxn"
	blockAttachmentName = "block"
	blockKeyPrefix      = ""
)

const blockHashIndexDef = `
	{
		"index": {
			"fields": ["` + blockHashField + `"]
		},
		"name": "` + blockHashIndexName + `",
		"ddoc": "` + blockHashIndexDoc + `",
		"type": "json"
	}`

const blockTxnIndexDef = `
	{
		"index": {
			"fields": [
				"`+ idField + `",
				"` + blockTxnsField + `.[].` + blockTxnIDField + `"
			]
		},
		"name": "` + blockTxnIDIndexName + `",
		"ddoc": "` + blockTxnIndexDoc + `",
		"type": "json"
	}`

type jsonValue map[string]interface{}

func (v jsonValue) toBytes() ([]byte, error) {
	return json.Marshal(v)
}

func blockToCouchDoc(block *common.Block) (*couchdb.CouchDoc, error) {
	jsonMap := make(jsonValue)

	blockHeader := block.GetHeader()
	key := blockNumberToKey(blockHeader.Number)
	blockHashHex := hex.EncodeToString(blockHeader.Hash())
	blockTxns, err := blockToTransactionsField(block)
	if err != nil {
		return nil, err
	}

	jsonMap[idField] = key
	jsonMap[blockHashField] = blockHashHex
	jsonMap[blockTxnsField] = blockTxns

	jsonBytes, err := jsonMap.toBytes()
	if err != nil {
		return nil, err
	}
	couchDoc := &couchdb.CouchDoc{JSONValue: jsonBytes}

	attachment, err := blockToAttachment(block)
	if err != nil {
		return nil, err
	}

	attachments := append([]*couchdb.AttachmentInfo{}, attachment)
	couchDoc.Attachments = attachments
	return couchDoc, nil
}

func blockToTransactionsField(block *common.Block) ([]jsonValue, error) {
	blockData := block.GetData()

	var txns []jsonValue

	for _, txEnvelopeBytes := range blockData.Data {
		txID, err := extractTxID(txEnvelopeBytes)
		if err != nil {
			return nil, errors.WithMessage(err, "transaction ID could not be extracted")
		}

		txField := make(jsonValue)
		txField[blockTxnIDField] = txID

		txns = append(txns, txField)
	}

	return txns, nil
}

func blockToAttachment(block *common.Block) (*couchdb.AttachmentInfo, error) {
	blockBytes, err := proto.Marshal(block)
	if err != nil {
		return nil, errors.Wrapf(err, "marshaling block failed")
	}

	attachment := &couchdb.AttachmentInfo{}
	attachment.AttachmentBytes = blockBytes
	attachment.ContentType = "application/octet-stream"
	attachment.Name = blockAttachmentName

	return attachment, nil
}

func couchDocToBlock(doc *couchdb.CouchDoc) (*common.Block, error) {
	return couchAttachmentsToBlock(doc.Attachments)
}

func couchAttachmentsToBlock(attachments []*couchdb.AttachmentInfo) (*common.Block, error) {
	var blockBytes []byte
	block := common.Block{}

	// get binary data from attachment
	for _, a := range attachments {
		if a.Name == blockAttachmentName {
			blockBytes = a.AttachmentBytes
		}
	}

	if len(blockBytes) == 0 {
		return nil, errors.New("block is not within couchDB document")
	}

	err := proto.Unmarshal(blockBytes, &block)
	if err != nil {
		return nil, errors.Wrapf(err, "block from couchDB document could not be unmarshaled")
	}

	return &block, nil
}

func blockNumberToKey(blockNum uint64) string {
	return blockKeyPrefix + strconv.FormatUint(blockNum, 10)
}

func retrieveBlockQuery(db *couchdb.CouchDatabase, query string) (*common.Block, error) {
	resultsP, err := db.QueryDocuments(query)
	if err != nil {
		return nil, err
	}
	results := *resultsP // remove unnecessary pointer (todo: should fix in source package)

	if len(results) != 1 {
		return nil, blkstorage.ErrNotFoundInIndex
	}

	if len(results[0].Attachments) == 0 {
		return nil, errors.New("block bytes not found")
	}

	return couchAttachmentsToBlock(results[0].Attachments)
}