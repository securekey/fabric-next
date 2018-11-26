/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbpvtdata

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/pkg/errors"
)

const (
	idField                    = "_id"
	revField                   = "_rev"
	blockNumberField           = "block_number"
	expiryBlockNumbersField    = "expiry_block_numbers"
	purgeBlockNumbersField     = "purge_block_numbers"
	purgeIntervalField         = "purge_interval"
	purgeBlockNumbersIndexName = "by_purge_block_number"
	purgeBlockNumbersIndexDoc  = "indexPurgeBlockNumber"
	dataField                  = "data"
	expiryField                = "expiry"
	metadataKey                = "metadata"
	commitField                = "commit"
	pendingCommitField         = "pending"
	blockKeyPrefix             = ""
	blockNumberBase            = 10
)

const purgeBlockNumbersIndexDef = `
	{
		"index": {
			"fields": ["` + purgeBlockNumbersField + `"]
		},
		"name": "` + purgeBlockNumbersIndexName + `",
		"ddoc": "` + purgeBlockNumbersIndexDoc + `",
		"type": "json"
	}`

type jsonValue map[string]interface{}

func (v jsonValue) toBytes() ([]byte, error) {
	return json.Marshal(v)
}

func createBlockCouchDoc(dataEntries []*dataEntry, expiryEntries []*expiryEntry, blockNumber uint64, purgeInterval uint64) (*couchdb.CouchDoc, error) {
	jsonMap := make(jsonValue)
	jsonMap[idField] = blockNumberToKey(blockNumber)
	jsonMap[purgeIntervalField] = strconv.FormatUint(purgeInterval, 10)

	dataJSON, err := dataEntriesToJSONValue(dataEntries)
	if err != nil {
		return nil, err
	}
	jsonMap[dataField] = dataJSON

	ei, err := expiryEntriesToJSONValue(expiryEntries, purgeInterval)
	if err != nil {
		return nil, err
	}
	jsonMap[expiryField] = ei.json
	jsonMap[purgeBlockNumbersField] = ei.purgeKeys
	jsonMap[expiryBlockNumbersField] = ei.expiryKeys

	jsonBytes, err := jsonMap.toBytes()
	if err != nil {
		return nil, err
	}

	couchDoc := couchdb.CouchDoc{JSONValue: jsonBytes}

	return &couchDoc, nil
}

func blockNumberToKey(blockNum uint64) string {
	return blockKeyPrefix + strconv.FormatUint(blockNum, 10)
}

func dataEntriesToJSONValue(dataEntries []*dataEntry) (jsonValue, error) {
	data := make(jsonValue)

	for _, dataEntry := range dataEntries {
		keyBytes := encodeDataKey(dataEntry.key)
		valBytes, err := encodeDataValue(dataEntry.value)
		if err != nil {
			return nil, err
		}

		keyBytesHex := hex.EncodeToString(keyBytes)
		data[keyBytesHex] = valBytes
	}

	return data, nil
}

type expiryInfo struct {
	json jsonValue
	purgeKeys []string
	expiryKeys []string
}

func expiryEntriesToJSONValue(expiryEntries []*expiryEntry, purgeInterval uint64) (*expiryInfo, error) {
	var ei expiryInfo

	ei.json = make(jsonValue)
	expiryBlockCounted := make(map[uint64]bool)

	for _, expiryEntry := range expiryEntries {
		keyBytes := encodeExpiryKey(expiryEntry.key)
		valBytes, err := encodeExpiryValue(expiryEntry.value)
		if err != nil {
			return nil, err
		}

		keyBytesHex := hex.EncodeToString(keyBytes)
		ei.json[keyBytesHex] = valBytes

		if !expiryBlockCounted[expiryEntry.key.expiringBlk] {
			ei.expiryKeys = append(ei.expiryKeys, blockNumberToKey(expiryEntry.key.expiringBlk))
			purgedAt := blockNumberToKey(expiryEntry.key.expiringBlk + expiryEntry.key.expiringBlk % purgeInterval)
			ei.purgeKeys = append(ei.purgeKeys, purgedAt)
			expiryBlockCounted[expiryEntry.key.expiringBlk] = true
		}
	}

	// TODO: sort string slices numerically.

	return &ei, nil
}

func createMetadataDoc(rev string, pendingCommit bool, lastBlockNumber uint64) (*couchdb.CouchDoc, error) {
	jsonMap := make(jsonValue)
	jsonMap[idField] = metadataKey
	if rev != "" {
		jsonMap[revField] = rev
	}

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

type metadata struct {
	pending           bool
	lastCommitedBlock uint64
}

func updateCommitMetadataDoc(db *couchdb.CouchDatabase, m *metadata, rev string) (string, error) {
	doc, err := createMetadataDoc(rev, m.pending, m.lastCommitedBlock)
	if err != nil {
		return "", err
	}

	rev, err = db.SaveDoc(metadataKey, rev, doc)
	if err != nil {
		return "", err
	}

	// TODO: is full sync needed after saving the metadata doc (will recovery work)?
	err = db.EnsureFullCommit()
	if err != nil {
		return "", errors.WithMessage(err, "full commit failed")
	}

	return rev, nil
}

type metadataResponse struct {
	Commit struct {
		Pending     bool   `json:"pending"`
		BlockNumber uint64 `json:"block_number"`
	} `json:"commit"`
}

func lookupMetadata(db *couchdb.CouchDatabase) (*metadata, bool, error) {
	doc, _, err := db.ReadDoc(metadataKey)
	if err != nil {
		return nil, false, errors.WithMessage(err, "private data metadata retrieval failed")
	}
	if doc == nil {
		return nil, false, nil
	}

	var mr metadataResponse
	err = json.Unmarshal(doc.JSONValue, &mr)
	if err != nil {
		return nil, false, errors.WithMessage(err, "private data metadata is invalid")
	}

	m := metadata{mr.Commit.Pending, mr.Commit.BlockNumber}
	return &m, true, nil
}

type blockPvtDataResponse struct {
	ID            string            `json:"_id"`
	Rev           string            `json:"_rev"`
	PurgeInterval string            `json:"purge_interval"`
	PurgeBlocks   []string          `json:"purge_block_numbers"`
	ExpiryBlocks  []string          `json:"expiry_block_numbers"`
	Data          map[string][]byte `json:"data"`
	Expiry        map[string][]byte `json:"expiry"`
}

func retrieveBlockPvtData(db *couchdb.CouchDatabase, id string) (*blockPvtDataResponse, error) {
	doc, _, err := db.ReadDoc(id)
	if err != nil {
		return nil, err
	}

	if doc == nil {
		return nil, NewErrNotFoundInIndex()
	}

	var blockPvtData blockPvtDataResponse
	err = json.Unmarshal(doc.JSONValue, &blockPvtData)
	if err != nil {
		return nil, errors.Wrapf(err, "result from DB is not JSON encoded")
	}

	return &blockPvtData, nil
}

func retrieveBlockExpiryData(db *couchdb.CouchDatabase, id string) ([]*blockPvtDataResponse, error) {
	const queryFmt = `
	{
		"selector": {
			"` + purgeBlockNumbersField + `": {
				"$elemMatch": {
					"$eq": "%s"
				}
			}
		},
		"use_index": ["_design/` + purgeBlockNumbersIndexDoc + `", "` + purgeBlockNumbersIndexName + `"]
	}`

	results, err := db.QueryDocuments(fmt.Sprintf(queryFmt, id))
	if err != nil {
		return nil, err
	}

	if len(results) == 0 {
		return nil, NewErrNotFoundInIndex()
	}

	var responses []*blockPvtDataResponse
	for _, result := range results {
		var blockPvtData blockPvtDataResponse
		err = json.Unmarshal(result.Value, &blockPvtData)
		if err != nil {
			return nil, errors.Wrapf(err, "result from DB is not JSON encoded")
		}
		responses = append(responses, &blockPvtData)
	}

	return responses, nil
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
