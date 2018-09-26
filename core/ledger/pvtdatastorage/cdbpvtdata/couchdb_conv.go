/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbpvtdata

import (
	"encoding/hex"
	"encoding/json"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
)

const (
	idField       = "_id"
	binaryWrapper = "valueBytes"
)

type jsonValue map[string]interface{}

func (v jsonValue) toBytes() ([]byte, error) {
	return json.Marshal(v)
}

func dataEntriesToCouchDocs(dataEntries []*dataEntry) ([]*couchdb.CouchDoc, error) {
	var docs []*couchdb.CouchDoc

	for _, dataEntry := range dataEntries {
		keyBytes := encodeDataKey(dataEntry.key)
		valBytes, err := encodeDataValue(dataEntry.value)
		if err != nil {
			return nil, err
		}

		doc, err := keyValueToCouchDoc(keyBytes, valBytes)
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

		doc, err := keyValueToCouchDoc(keyBytes, valBytes)
		if err != nil {
			return nil, err
		}
		docs = append(docs, doc)
	}
	return docs, nil
}

func keyValueToCouchDoc(key []byte, value []byte) (*couchdb.CouchDoc, error) {
	jsonMap := make(jsonValue)

	jsonMap[idField] = hex.EncodeToString(key)

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