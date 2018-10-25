/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbtransientdata

import (
	"encoding/hex"
	"encoding/json"

	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
)

const (
	idField              = "_id"
	transientDataField   = "transientData"
	blockNumberField     = "block_number"
	blockNumberIndexName = "by_block_number"
	blockNumberIndexDoc  = "indexBlockNumber"
	txIDField            = "tx_id"
	txIDIndexName        = "by_tx_id"
	txIDIndexDoc         = "indexTxID"
	blockNumberBase      = 10
)

const blockNumberIndexDef = `
	{
		"index": {
			"fields": ["` + blockNumberField + `"]
		},
		"name": "` + blockNumberIndexName + `",
		"ddoc": "` + blockNumberIndexDoc + `",
		"type": "json"
	}`

const txIDIndexDef = `
	{
		"index": {
			"fields": ["` + txIDField + `"]
		},
		"name": "` + txIDIndexName + `",
		"ddoc": "` + txIDIndexDoc + `",
		"type": "json"
	}`

type jsonValue map[string]interface{}

func (v jsonValue) toBytes() ([]byte, error) {
	return json.Marshal(v)
}

func keyValueToCouchDoc(key []byte, value []byte, indices map[string]string) (*couchdb.CouchDoc, error) {
	jsonMap := make(jsonValue)
	k := hex.EncodeToString(key)
	jsonMap[idField] = k

	for key, val := range indices {
		jsonMap[key] = val
	}
	jsonMap[transientDataField] = value

	jsonBytes, err := jsonMap.toBytes()
	if err != nil {
		return nil, err
	}

	couchDoc := couchdb.CouchDoc{JSONValue: jsonBytes}

	return &couchDoc, nil
}
