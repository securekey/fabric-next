/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbpvtdata

import (
	"encoding/hex"
	"encoding/json"
	"strconv"

	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
)

const (
	idField              = "_id"
	binaryWrapper        = "valueBytes"
	blockNumberField     = "number"
	blockNumberIndexName = "by_number"
	blockNumberIndexDoc  = "indexNumber"
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

type jsonValue map[string]interface{}

func (v jsonValue) toBytes() ([]byte, error) {
	return json.Marshal(v)
}

func dataEntriesToCouchDocs(dataEntries []*dataEntry, blockNumber uint64) ([]*couchdb.CouchDoc, error) {
	var docs []*couchdb.CouchDoc

	for _, dataEntry := range dataEntries {
		keyBytes := encodeDataKey(dataEntry.key)
		valBytes, err := encodeDataValue(dataEntry.value)
		if err != nil {
			return nil, err
		}
		indices := map[string]string{blockNumberField: strconv.FormatUint(blockNumber, blockNumberBase)}
		doc, err := keyValueToCouchDoc(keyBytes, valBytes, indices)
		if err != nil {
			return nil, err
		}
		docs = append(docs, doc)
	}
	return docs, nil
}

func expiryEntriesToCouchDocs(expiryEntries []*expiryEntry) ([]*couchdb.CouchDoc, error) {
	var docs []*couchdb.CouchDoc

	for _, expiryEntry := range expiryEntries {
		keyBytes := encodeExpiryKey(expiryEntry.key)
		valBytes, err := encodeExpiryValue(expiryEntry.value)
		if err != nil {
			return nil, err
		}

		doc, err := keyValueToCouchDoc(keyBytes, valBytes, nil)
		if err != nil {
			return nil, err
		}
		docs = append(docs, doc)
	}
	return docs, nil
}

func keyValueToCouchDoc(key []byte, value []byte, indices map[string]string) (*couchdb.CouchDoc, error) {
	jsonMap := make(jsonValue)

	jsonMap[idField] = hex.EncodeToString(key)

	for key, val := range indices {
		jsonMap[key] = val
	}

	jsonBytes, err := jsonMap.toBytes()
	if err != nil {
		return nil, err
	}

	couchDoc := couchdb.CouchDoc{JSONValue: jsonBytes}

	attachment, err := valueToAttachment(value)
	if err != nil {
		return nil, err
	}

	attachments := append([]*couchdb.AttachmentInfo{}, attachment)
	couchDoc.Attachments = attachments

	return &couchDoc, nil
}

func valueToAttachment(v []byte) (*couchdb.AttachmentInfo, error) {
	attachment := &couchdb.AttachmentInfo{}
	attachment.AttachmentBytes = v
	attachment.ContentType = "application/octet-stream"
	attachment.Name = binaryWrapper

	return attachment, nil
}
