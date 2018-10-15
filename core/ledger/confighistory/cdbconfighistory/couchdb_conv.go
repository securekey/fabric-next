/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbconfighistory

import (
	"encoding/hex"
	"encoding/json"

	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/pkg/errors"
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
	k := hex.EncodeToString(key)
	jsonMap[idField] = k

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

func couchAttachmentsValue(attachments []*couchdb.AttachmentInfo) ([]byte, error) {
	var valueBytes []byte
	// get binary data from attachment
	for _, a := range attachments {
		if a.Name == binaryWrapper {
			valueBytes = a.AttachmentBytes
		}
	}

	if len(valueBytes) == 0 {
		return nil, errors.New("config history data is not within couchDB document")
	}
	return valueBytes, nil
}
