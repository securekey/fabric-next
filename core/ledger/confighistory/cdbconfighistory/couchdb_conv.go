/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbconfighistory

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"

	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/pkg/errors"
)

const (
	idField                    = "_id"
	binaryWrapper              = "valueBytes"
	blockNumberField           = "block_number"
	blockNumberCCNameIndexName = "by_block_number_cc_name"
	blockNumberCCNameIndexDoc  = "indexBlockNumberCCName"
	ccNameField                = "cc_name"
	blockNumberBase            = 10
	configHistoryField         = "configHistoryData"
)

const blockNumberCCNameIndexDef = `
	{
		"index": {
			"fields": ["` + blockNumberField + `","` + ccNameField + `"]
		},
		"name": "` + blockNumberCCNameIndexName + `",
		"ddoc": "` + blockNumberCCNameIndexDoc + `",
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
	jsonMap[configHistoryField] = value

	jsonBytes, err := jsonMap.toBytes()
	if err != nil {
		return nil, err
	}

	couchDoc := couchdb.CouchDoc{JSONValue: jsonBytes}

	return &couchDoc, nil
}

func docValueToConfigHistoryValue(docValue []byte) ([]byte, error) {
	jsonResult := make(map[string]interface{})
	decoder := json.NewDecoder(bytes.NewBuffer(docValue))
	decoder.UseNumber()
	err := decoder.Decode(&jsonResult)
	if err != nil {
		return nil, errors.Wrapf(err, "result from DB is not JSON encoded")
	}
	valueBytes, err := base64.StdEncoding.DecodeString(jsonResult[configHistoryField].(string))
	if err != nil {
		return nil, errors.Wrapf(err, "error from DecodeString for configHistoryField")
	}
	return valueBytes, nil
}
