/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbpvtdata

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/pkg/errors"
)

const (
	idField                    = "_id"
	expiryBlockNumbersField    = "expiry_block_numbers"
	purgeBlockNumbersField     = "purge_block_numbers"
	purgeIntervalField         = "purge_interval"
	purgeBlockNumbersIndexName = "by_purge_block_number"
	purgeBlockNumbersIndexDoc  = "indexPurgeBlockNumber"
	dataField                  = "data"
	expiryField                = "expiry"
	blockKeyPrefix             = ""
	blockNumberBase            = 10
	numMetaDocs                = 1
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
	json       jsonValue
	purgeKeys  []string
	expiryKeys []string
}

func expiryEntriesToJSONValue(expiryEntries []*expiryEntry, purgeInterval uint64) (*expiryInfo, error) {
	ei := expiryInfo{
		json:       make(jsonValue),
		purgeKeys:  make([]string, 0),
		expiryKeys: make([]string, 0),
	}

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
			expiringBlk := blockNumberToKey(expiryEntry.key.expiringBlk)
			if !stringInSlice(expiringBlk, ei.expiryKeys) {
				ei.expiryKeys = append(ei.expiryKeys, expiringBlk)
			}
			purgedAt := blockNumberToKey(expiryEntry.key.expiringBlk + (purgeInterval - expiryEntry.key.expiringBlk%purgeInterval))
			if !stringInSlice(purgedAt, ei.purgeKeys) {
				ei.purgeKeys = append(ei.purgeKeys, purgedAt)
			}
			expiryBlockCounted[expiryEntry.key.expiringBlk] = true
		}
	}

	// TODO: sort string slices numerically.

	return &ei, nil
}

type metadata struct {
	pending           bool
	lastCommitedBlock uint64
}

func lookupMetadata(db *couchdb.CouchDatabase) (metadata, bool, error) {
	info, err := db.GetDatabaseInfo()
	if err != nil {
		return metadata{}, false, err
	}

	var lastBlockNum uint64
	var found bool

	mc := min(info.DocCount, numMetaDocs+1)
	for i := 1; i <= mc; i++ {
		doc, _, e := db.ReadDoc(blockNumberToKey(uint64(info.DocCount - i)))
		if e != nil {
			return metadata{}, false, err
		}

		if doc != nil {
			var lastPvtDataResp *blockPvtDataResponse
			err = json.Unmarshal(doc.JSONValue, lastPvtDataResp)
			if err != nil {
				return metadata{}, false, errors.Wrapf(err, "block from couchDB document could not be unmarshaled")
			}
			lastBlockNum = uint64(info.DocCount - i)
			found = true
			break
		}
	}

	if !found {
		return metadata{}, false, nil
	}

	m := metadata{false, lastBlockNum}
	return m, true, nil
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

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}
