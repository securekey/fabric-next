/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbblkstorage

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"

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
	blockTxnIDsField     = "transaction_ids"
	blockHashIndexName   = "by_hash"
	blockHashIndexDoc    = "indexHash"
	blockTxnIndexName    = "by_id"
	blockTxnIndexDoc     = "indexTxn"
	blockAttachmentName  = "block"
	blockKeyPrefix       = ""
	blockHeaderField     = "header"
)

// txn document
const (
	txnValidationCode     = "validation_code"
	txnValidationCodeBase = 16
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

const blockTxnIndexDef = `
	{
		"index": {
			"fields": [
				"` + blockTxnIDsField + `"
			]
		},
		"name": "` + blockTxnIndexName + `",
		"ddoc": "` + blockTxnIndexDoc + `",
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

	blockTxnIDs, err := blockToTransactionIDsField(block)
	if err != nil {
		return nil, err
	}

	jsonMap[idField] = key
	header := make(jsonValue)
	header[blockHashField] = blockHashHex
	jsonMap[blockHeaderField] = header
	jsonMap[blockTxnsField] = blockTxns
	jsonMap[blockTxnIDsField] = blockTxnIDs

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

var errorNoTxID = errors.New("missing transaction ID")

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

func blockToTransactionsField(block *common.Block) (jsonValue, error) {
	blockData := block.GetData()

	txns := make(jsonValue)

	blockMetadata := block.GetMetadata()
	txValidationFlags := ledgerUtil.TxValidationFlags(blockMetadata.GetMetadata()[common.BlockMetadataIndex_TRANSACTIONS_FILTER])

	for i, txEnvelopeBytes := range blockData.GetData() {
		envelope, err := utils.GetEnvelopeFromBlock(txEnvelopeBytes)
		if err != nil {
			return nil, err
		}

		txID, err := extractTxIDFromEnvelope(envelope)
		if err != nil {
			return nil, errors.WithMessage(err, "transaction ID could not be extracted")
		}

		validationCode := txValidationFlags.Flag(i)

		txField := make(jsonValue)
		txField[txnValidationCode] = strconv.FormatInt(int64(validationCode), txnValidationCodeBase)

		txns[txID] = txField
	}

	return txns, nil
}

func blockToTransactionIDsField(block *common.Block) ([]string, error) {
	blockData := block.GetData()

	var txns []string

	for _, txEnvelopeBytes := range blockData.GetData() {
		envelope, err := utils.GetEnvelopeFromBlock(txEnvelopeBytes)
		if err != nil {
			return nil, err
		}

		txID, err := extractTxIDFromEnvelope(envelope)
		if err != nil {
			return nil, errors.WithMessage(err, "transaction ID could not be extracted")
		}

		txns = append(txns, txID)
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

type blockTransactionResponse struct {
	Transactions map[string]struct {
		ValidationCode string `json:"validation_code"`
	} `json:"transactions"`
}

func retrieveTxValidationCodeQuery(db *couchdb.CouchDatabase, query string, txID string) (int32, error) {
	results, err := db.QueryDocuments(fmt.Sprintf(query, txID))
	if err != nil {
		return 0, err
	}

	if len(results) == 0 {
		return 0, blkstorage.ErrNotFoundInIndex
	}

	var txResponse = &blockTransactionResponse{}
	err = json.Unmarshal(results[0].Value, &txResponse)
	if err != nil {
		return 0, err
	}

	txn, ok := txResponse.Transactions[txID]
	if !ok {
		return 0, errors.New("transaction not found in couch document")
	}

	const sizeOfTxValidationCode = 32
	txnValidationCode, err := strconv.ParseInt(txn.ValidationCode, txnValidationCodeBase, sizeOfTxValidationCode)
	if err != nil {
		return 0, errors.Wrapf(err, "validation code was invalid for transaction ID [%s]", txn.ValidationCode)
	}

	return int32(txnValidationCode), nil
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

func couchDocToJSON(doc *couchdb.CouchDoc) (jsonValue, error) {
	return jsonValueToJSON(doc.JSONValue)
}

func jsonValueToJSON(jsonValue []byte) (jsonValue, error) {
	// create a generic map unmarshal the json
	jsonResult := make(map[string]interface{})
	decoder := json.NewDecoder(bytes.NewBuffer(jsonValue))
	decoder.UseNumber()

	err := decoder.Decode(&jsonResult)
	if err != nil {
		return nil, errors.Wrapf(err, "result from DB is not JSON encoded")
	}

	return jsonResult, nil
}
