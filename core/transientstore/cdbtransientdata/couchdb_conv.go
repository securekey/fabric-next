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
	revField             = "_rev"
	transientDataField   = "transientData"
	blockNumberField     = "block_number"
	blockNumberIndexName = "by_block_number"
	blockNumberIndexDoc  = "indexBlockNumber"
	txIDField            = "tx_id"
	txIDIndexName        = "by_tx_id"
	txIDIndexDoc         = "indexTxID"
	blockNumberBase      = 10
	deletedField         = "_deleted"
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

func asBatchDeleteCouchDocs(metaData []json.RawMessage) ([]*couchdb.CouchDoc, error) {
	var batchDeleteDocs []*couchdb.CouchDoc
	for _, row := range metaData {
		var docMetadata = &couchdb.DocMetadata{}
		err := json.Unmarshal(row, &docMetadata)
		if err != nil {
			return nil, err
		}

		couchDoc, err := newDeleteCouchDoc(docMetadata)
		if err != nil {
			return nil, err
		}
		batchDeleteDocs = append(batchDeleteDocs, couchDoc)
	}
	return batchDeleteDocs, nil
}

func newDeleteCouchDoc(docMetaData *couchdb.DocMetadata) (*couchdb.CouchDoc, error) {
	jsonMap := make(jsonValue)
	jsonMap[idField] = docMetaData.ID
	jsonMap[revField] = docMetaData.Rev
	jsonMap[deletedField] = true

	jsonBytes, err := jsonMap.toBytes()
	if err != nil {
		return nil, err
	}

	return &couchdb.CouchDoc{JSONValue: jsonBytes}, nil
}
