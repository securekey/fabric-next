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

	"fmt"

	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/pkg/errors"
)

const (
	idField                    = "_id"
	binaryWrapper              = "valueBytes"
	blockNumberField           = "block_number"
	blockNumberIndexName       = "by_block_number"
	blockNumberIndexDoc        = "indexBlockNumber"
	blockNumberExpiryField     = "block_number_expiry"
	blockNumberExpiryIndexName = "by_block_number_expiry"
	blockNumberExpiryIndexDoc  = "indexBlockNumberExpiry"
	metadataKey                = "metadata"
	commitField                = "commit"
	pendingCommitField         = "pending"
	blockNumberBase            = 10
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

const blockNumberExpiryIndexDef = `
	{
		"index": {
			"fields": ["` + blockNumberExpiryField + `"]
		},
		"name": "` + blockNumberExpiryIndexName + `",
		"ddoc": "` + blockNumberExpiryIndexDoc + `",
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
		indices := map[string]string{blockNumberField: fmt.Sprintf("%064s", strconv.FormatUint(blockNumber, blockNumberBase))}
		doc, err := keyValueToCouchDoc(keyBytes, valBytes, indices)
		if err != nil {
			return nil, err
		}
		docs = append(docs, doc)
	}
	return docs, nil
}

func expiryEntriesToCouchDocs(expiryEntries []*expiryEntry, blockNumber uint64) ([]*couchdb.CouchDoc, error) {
	var docs []*couchdb.CouchDoc

	for _, expiryEntry := range expiryEntries {
		keyBytes := encodeExpiryKey(expiryEntry.key)
		valBytes, err := encodeExpiryValue(expiryEntry.value)
		if err != nil {
			return nil, err
		}
		indices := map[string]string{blockNumberExpiryField: fmt.Sprintf("%064s", strconv.FormatUint(blockNumber, blockNumberBase))}
		doc, err := keyValueToCouchDoc(keyBytes, valBytes, indices)
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

func createMetadataDoc(pendingCommit bool, lastBlockNumber uint64) (*couchdb.CouchDoc, error) {
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
		return nil, errors.New("commit field not found in metadata")
	}

	commitMap, ok := commitMapUT.(map[string]interface{})
	// FIXME: This still returns !ok but it actually gives a valid value.
	// It must be some type of JSON structure??
	// if !ok {
	// 	return nil, errors.New("private data metadata is invalid")
	// }
	if commitMap == nil {
		return nil, errors.New("private data metadata is invalid - nil")
	}
	return commitMap, nil
}

type metadata struct {
	pending           bool
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
		return nil, false, errors.New("pending commit field not found in metadata")
	}

	pending, ok := pendingUT.(bool)
	if !ok {
		return nil, false, errors.New("pending JSON field is not a bool")
	}

	lastCommitedBlockUT, ok := commitMap[blockNumberField]
	if !ok {
		return nil, false, errors.New("block number field not found in metadata")
	}

	lastCommitedBlockJSON, ok := lastCommitedBlockUT.(json.Number)
	if !ok {
		return nil, false, errors.New("block number field is not a JSON number")
	}

	lastCommitedBlock, err := strconv.ParseUint(lastCommitedBlockJSON.String(), 10, 64)
	if err != nil {
		return nil, false, errors.Wrap(err, "error parsing block number")
	}

	m := metadata{pending, lastCommitedBlock}
	return &m, true, nil
}

func retrievePvtDataQuery(db *couchdb.CouchDatabase, query string) (map[string][]byte, error) {
	results, err := db.QueryDocuments(query)
	if err != nil {
		return nil, err
	}

	if len(results) == 0 {
		return nil, NewErrNotFoundInIndex()
	}
	m := make(map[string][]byte)

	for _, val := range results {
		key, err := hex.DecodeString(val.ID)
		if err != nil {
			return nil, err
		}
		value, err := couchAttachmentsToPvtDataBytes(val.Attachments)
		if err != nil {
			return nil, err
		}
		m[string(key)] = value
	}
	return m, nil
}

func couchAttachmentsToPvtDataBytes(attachments []*couchdb.AttachmentInfo) ([]byte, error) {
	var pvtDataBytes []byte

	// get binary data from attachment
	for _, a := range attachments {
		if a.Name == binaryWrapper {
			pvtDataBytes = a.AttachmentBytes
		}
	}

	if len(pvtDataBytes) == 0 {
		return nil, errors.New("pvt data is not within couchDB document")
	}

	return pvtDataBytes, nil
}

// NotFoundInIndexErr is used to indicate missing entry in the index
type NotFoundInIndexErr struct {
}

// NewErrNotFoundInIndex creates an missing entry in the index error
func NewErrNotFoundInIndex() *NotFoundInIndexErr {
	return &NotFoundInIndexErr{}
}

func (err *NotFoundInIndexErr) Error() string {
	return "Entry not found in index"
}
