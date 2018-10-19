/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbblkstorage

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"strconv"

	"github.com/hyperledger/fabric/protos/peer"

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"

	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	ledgerUtil "github.com/hyperledger/fabric/core/ledger/util"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/utils"
)

// block document
const (
	idField              = "_id"
	lastBlockNumberField = "lastBlockNumber"
	isChainEmptyField    = "chainEmpty"
	blockHashField       = "hash"
	blockTxnsField       = "transactions"
	blockTxnIDField      = "id"
	blockHashIndexName   = "by_hash"
	blockHashIndexDoc    = "indexHash"
	blockAttachmentName  = "block"
	blockKeyPrefix       = ""
	blockHeaderField     = "header"
)

// txn document
const (
	txnBlockNumberField   = "block_number"
	txnBlockHashField     = "block_hash"
	txnAttachmentName     = "transaction"
	txnValidationCode     = "validation_code"
	txnValidationCodeBase = 16
)

// checkpoint document
const (
	cpiAttachmentName = "checkpointinfo"
)

const blockHashIndexDef = `
	{
		"index": {
			"fields": ["` + blockHeaderField + `.` + blockHashField + `"]
		},
		"name": "` + blockHashIndexName + `",
		"ddoc": "` + blockHashIndexDoc + `",
		"type": "json"
	}`

type jsonValue map[string]interface{}

func (v jsonValue) toBytes() ([]byte, error) {
	return json.Marshal(v)
}

func (v jsonValue) unmarshal(bytes []byte) error {
	err := json.Unmarshal(bytes, &v)
	return err
}

func blockToCouchDoc(block *common.Block) (*couchdb.CouchDoc, error) {
	jsonMap := make(jsonValue)

	blockHeader := block.GetHeader()

	key := blockNumberToKey(blockHeader.GetNumber())
	blockHashHex := hex.EncodeToString(blockHeader.Hash())
	blockTxns, err := blockToTransactionsField(block)
	if err != nil {
		return nil, err
	}

	jsonMap[idField] = key
	header := make(jsonValue)
	header[blockHashField] = blockHashHex
	jsonMap[blockHeaderField] = header
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

func blockToTxnCouchDocs(block *common.Block, attachTxn bool) ([]*couchdb.CouchDoc, error) {
	blockHeader := block.GetHeader()
	blockNumber := blockNumberToKey(blockHeader.GetNumber())
	blockHash := hex.EncodeToString(blockHeader.Hash())

	blockData := block.GetData()

	blockMetadata := block.GetMetadata()
	txValidationFlags := ledgerUtil.TxValidationFlags(blockMetadata.GetMetadata()[common.BlockMetadataIndex_TRANSACTIONS_FILTER])

	txnDocs := make([]*couchdb.CouchDoc, 0)

	for i, txEnvelopeBytes := range blockData.GetData() {
		envelope, err := utils.GetEnvelopeFromBlock(txEnvelopeBytes)
		if err != nil {
			return nil, err
		}

		txnDoc, err := blockTxnToCouchDoc(blockNumber, blockHash, envelope, txValidationFlags.Flag(i), attachTxn)
		if err == errorNoTxID {
			continue
		} else if err != nil {
			return nil, err
		}

		txnDocs = append(txnDocs, txnDoc)
	}

	return txnDocs, nil
}

var errorNoTxID = errors.New("missing transaction ID")

func blockTxnToCouchDoc(blockNumber string, blockHash string, txEnvelope *common.Envelope, validationCode peer.TxValidationCode, attachTxn bool) (*couchdb.CouchDoc, error) {
	txID, err := extractTxIDFromEnvelope(txEnvelope)
	if err != nil {
		return nil, errors.WithMessage(err, "transaction ID could not be extracted")
	}

	// TODO: is the empty transaction queryable? If so, need to change this to a default transaction ID.
	if txID == "" {
		return nil, errorNoTxID
	}

	jsonMap := make(jsonValue)
	jsonMap[idField] = txID
	jsonMap[txnBlockHashField] = blockHash
	jsonMap[txnBlockNumberField] = blockNumber
	jsonMap[txnValidationCode] = strconv.FormatInt(int64(validationCode), txnValidationCodeBase)

	jsonBytes, err := jsonMap.toBytes()
	if err != nil {
		return nil, err
	}
	couchDoc := &couchdb.CouchDoc{JSONValue: jsonBytes}

	if attachTxn {
		attachment, err := txnEnvelopeToAttachment(txEnvelope)
		if err != nil {
			return nil, err
		}

		attachments := append([]*couchdb.AttachmentInfo{}, attachment)
		couchDoc.Attachments = attachments
	}
	return couchDoc, nil
}

func checkpointInfoToCouchDoc(i *checkpointInfo) (*couchdb.CouchDoc, error) {
	jsonMap := make(jsonValue)

	jsonMap[idField] = blkMgrInfoKey
	jsonMap[lastBlockNumberField] = strconv.FormatUint(i.lastBlockNumber, 10)
	jsonMap[isChainEmptyField] = strconv.FormatBool(i.isChainEmpty)
	jsonBytes, err := jsonMap.toBytes()
	if err != nil {
		return nil, err
	}
	couchDoc := &couchdb.CouchDoc{JSONValue: jsonBytes}
	return couchDoc, nil
}

func blockToTransactionsField(block *common.Block) ([]jsonValue, error) {
	blockData := block.GetData()

	var txns []jsonValue

	for _, txEnvelopeBytes := range blockData.GetData() {
		envelope, err := utils.GetEnvelopeFromBlock(txEnvelopeBytes)
		if err != nil {
			return nil, err
		}

		txID, err := extractTxIDFromEnvelope(envelope)
		if err != nil {
			return nil, errors.WithMessage(err, "transaction ID could not be extracted")
		}

		txField := make(jsonValue)
		txField[blockTxnIDField] = txID

		txns = append(txns, txField)
	}

	return txns, nil
}

func txnEnvelopeToAttachment(txEnvelope *common.Envelope) (*couchdb.AttachmentInfo, error) {
	txEnvelopeBytes, err := proto.Marshal(txEnvelope)
	if err != nil {
		return nil, errors.Wrapf(err, "marshaling block failed")
	}

	attachment := &couchdb.AttachmentInfo{}
	attachment.AttachmentBytes = txEnvelopeBytes
	attachment.ContentType = "application/octet-stream"
	attachment.Name = txnAttachmentName

	return attachment, nil
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

func couchAttachmentsToTxnEnvelope(attachments []*couchdb.AttachmentInfo) (*common.Envelope, error) {
	var envelope common.Envelope
	var txnBytes []byte

	// get binary data from attachment
	for _, a := range attachments {
		if a.Name == txnAttachmentName {
			txnBytes = a.AttachmentBytes
		}
	}

	if len(txnBytes) == 0 {
		return nil, errors.New("transaction envelope is not within couchDB document")
	}

	err := proto.Unmarshal(txnBytes, &envelope)
	if err != nil {
		return nil, errors.Wrapf(err, "transaction from couchDB document could not be unmarshaled")
	}

	return &envelope, nil
}

func couchDocToCheckpointInfo(doc *couchdb.CouchDoc) (*checkpointInfo, error) {
	v, err := couchDocToJSON(doc)
	if err != nil {
		return nil, errors.Wrapf(err, "couchDocToJSON return error")
	}
	isChainEmptyValue, _ := v[isChainEmptyField].(string)
	lastBlockNumberValue, _ := v[lastBlockNumberField].(string)
	isChainEmpty, _ := strconv.ParseBool(isChainEmptyValue)
	lastBlockNumber, _ := strconv.ParseUint(lastBlockNumberValue, 10, 64)
	return &checkpointInfo{isChainEmpty, lastBlockNumber}, nil

}

func blockNumberToKey(blockNum uint64) string {
	return blockKeyPrefix + strconv.FormatUint(blockNum, 10)
}

func retrieveBlockQuery(db *couchdb.CouchDatabase, query string) (*common.Block, error) {
	results, err := db.QueryDocuments(query)
	if err != nil {
		return nil, err
	}

	if len(results) == 0 {
		return nil, blkstorage.ErrNotFoundInIndex
	}

	if len(results[0].Attachments) == 0 {
		return nil, errors.New("block bytes not found")
	}

	return couchAttachmentsToBlock(results[0].Attachments)
}

func retrieveJSONQuery(db *couchdb.CouchDatabase, id string) (jsonValue, error) {
	doc, _, err := db.ReadDoc(id)
	if err != nil {
		return nil, err
	}
	if doc == nil {
		return nil, blkstorage.ErrNotFoundInIndex
	}

	return couchDocToJSON(doc)
}

func couchDocToJSON(doc *couchdb.CouchDoc) (jsonValue, error) {
	// create a generic map unmarshal the json
	jsonResult := make(map[string]interface{})
	decoder := json.NewDecoder(bytes.NewBuffer(doc.JSONValue))
	decoder.UseNumber()

	err := decoder.Decode(&jsonResult)
	if err != nil {
		return nil, errors.Wrapf(err, "result from DB is not JSON encoded")
	}

	return jsonResult, nil
}
