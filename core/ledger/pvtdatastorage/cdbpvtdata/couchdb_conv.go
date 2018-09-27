/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbpvtdata

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"strconv"

	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/pkg/errors"
)

const (
	idField              = "_id"
	binaryWrapper        = "valueBytes"
	blockNumberField     = "block_number"
	blockNumberIndexName = "by_block_number"
	blockNumberIndexDoc  = "indexBlockNumber"
	blockNumberBase      = 10
	metadataKey          = "metadata"
	commitField          = "commit"
	pendingCommitField   = "pending"
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

func createMetadataDoc(pendingCommit bool, lastBlockNumber uint64) (*couchdb.CouchDoc, error)  {
	jsonMap := make(jsonValue)
	jsonMap[idField] = metadataKey

	commitMap := make(jsonValue)
	commitMap[pendingCommitField] = pendingCommit
	commitMap[blockNumberField] = lastBlockNumber
	jsonMap[commitField] = commitMap

	jsonBytes, err := jsonMap.toBytes()
	if err != nil {
		return nil, err
	}

	couchDoc := couchdb.CouchDoc{JSONValue: jsonBytes}

	return &couchDoc, nil
}

func couchDocToJSON(doc *couchdb.CouchDoc) (jsonValue, error) {
	return couchValueToJSON(doc.JSONValue)
}

func couchValueToJSON(value []byte) (jsonValue, error) {
	// create a generic map unmarshal the json
	jsonResult := make(map[string]interface{})
	decoder := json.NewDecoder(bytes.NewBuffer(value))
	decoder.UseNumber()

	err := decoder.Decode(&jsonResult)
	if err != nil {
		return nil, errors.Wrapf(err, "result from DB is not JSON encoded")
	}

	return jsonResult, nil
}

func extractCommitMap(jsonMap jsonValue) (jsonValue, error) {
	commitMapUT, ok := jsonMap[commitField]
	if !ok {
		return nil, errors.New("private data metadata is invalid")
	}

	commitMap, ok := commitMapUT.(jsonValue)
	if !ok {
		return nil, errors.New("private data metadata is invalid")
	}

	return commitMap, nil
}

type metadata struct {
	pending bool
	lastCommitedBlock uint64
}

func updateCommitMetadataDoc(db *couchdb.CouchDatabase, m *metadata) error {
	doc, err := createMetadataDoc(m.pending, m.lastCommitedBlock)
	if err != nil {
		return err
	}

	_, err = db.SaveDoc(metadataKey, "", doc)
	if err != nil {
		return err
	}

	return nil
}

func lookupMetadata(db *couchdb.CouchDatabase) (*metadata, bool, error) {
	doc, _, err := db.ReadDoc(metadataKey)
	if err != nil {
		return nil, false, errors.WithMessage(err, "private data metadata retrieval failed")
	}
	if doc == nil {
		return nil, false, nil
	}

	jsonMap, err := couchDocToJSON(doc)
	if err != nil {
		return nil, false, errors.WithMessage(err, "private data metadata is invalid")
	}

	commitMap, err := extractCommitMap(jsonMap)
	if err != nil {
		return nil, false, err
	}

	pendingUT, ok := commitMap[pendingCommitField]
	if !ok {
		return nil, false, errors.New("private data metadata is invalid")
	}

	pending, ok := pendingUT.(bool)
	if !ok {
		return nil, false, errors.New("private data metadata is invalid")
	}

	lastCommitedBlockUT, ok := commitMap[blockNumberField]
	if !ok {
		return nil, false, errors.New("private data metadata is invalid")
	}

	lastCommitedBlock, ok := lastCommitedBlockUT.(uint64)
	if !ok {
		return nil, false, errors.New("private data metadata is invalid")
	}

	m := metadata {pending, lastCommitedBlock}
	return &m, true, nil
}
