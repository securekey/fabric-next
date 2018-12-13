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

	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"

	"encoding/base64"

	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/utils"
)

// block document
const (
	idField            = "_id"
	blockHashField     = "hash"
	blockTxnIDsField   = "transaction_ids"
	blockHashIndexName = "by_hash"
	blockHashIndexDoc  = "indexHash"
	blockTxnIndexName  = "by_id"
	blockTxnIndexDoc   = "indexTxn"
	blockKeyPrefix     = ""
	blockHeaderField   = "header"
	blockDataField     = "data"
	numMetaDocs        = 2
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
	blockTxnIDs, err := blockToTransactionIDsField(block)
	if err != nil {
		return nil, err
	}

	jsonMap[idField] = key
	header := make(jsonValue)
	header[blockHashField] = blockHashHex
	jsonMap[blockHeaderField] = header
	jsonMap[blockTxnIDsField] = blockTxnIDs
	blockData, err := blockToBase64(block)
	if err != nil {
		return nil, err
	}
	jsonMap[blockDataField] = blockData

	jsonBytes, err := jsonMap.toBytes()
	if err != nil {
		return nil, err
	}
	couchDoc := &couchdb.CouchDoc{JSONValue: jsonBytes}
	return couchDoc, nil
}

var errorNoTxID = errors.New("missing transaction ID")

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

func blockToBase64(block *common.Block) (string, error) {
	blockBytes, err := proto.Marshal(block)
	if err != nil {
		return "", errors.Wrapf(err, "marshaling block failed")
	}

	return base64.StdEncoding.EncodeToString(blockBytes), nil

}

func couchDocToBlock(docValue []byte) (*common.Block, error) {
	jsonResult := make(map[string]interface{})
	decoder := json.NewDecoder(bytes.NewBuffer(docValue))
	decoder.UseNumber()
	err := decoder.Decode(&jsonResult)
	if err != nil {
		return nil, errors.Wrapf(err, "result from DB is not JSON encoded")
	}
	blockBytes, err := base64.StdEncoding.DecodeString(jsonResult[blockDataField].(string))
	if err != nil {
		return nil, errors.Wrapf(err, "error from DecodeString for blockDataField")
	}
	if len(blockBytes) == 0 {
		return nil, errors.New("block is not within couchDB document")
	}
	block := common.Block{}
	err = proto.Unmarshal(blockBytes, &block)
	if err != nil {
		return nil, errors.Wrapf(err, "block from couchDB document could not be unmarshaled")
	}
	return &block, nil
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

	return couchDocToBlock(results[0].Value)
}
