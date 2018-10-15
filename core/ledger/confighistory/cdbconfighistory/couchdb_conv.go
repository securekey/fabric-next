/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbconfighistory

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
