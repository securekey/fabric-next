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

	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"

	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/pkg/errors"
	"github.com/willf/bitset"
)

const (
	idField                   = "_id"
	expiryBlockNumbersField   = "expiry_block_numbers"
	purgeBlockNumberField     = "purge_block_number"
	purgeIntervalField        = "purge_interval"
	purgeBlockNumberIndexName = "by_purge_block_number"
	purgeBlockNumberIndexDoc  = "indexPurgeBlockNumber"
	dataField                 = "data"
	missingDataField          = "missingData"
	expiryField               = "expiry"
	blockKeyPrefix            = ""
	blockNumberBase           = 10
)

const purgeBlockNumberIndexDef = `
	{
		"index": {
			"fields": ["` + purgeBlockNumberField + `"]
		},
		"name": "` + purgeBlockNumberIndexName + `",
		"ddoc": "` + purgeBlockNumberIndexDoc + `",
		"type": "json"
	}`

type jsonValue map[string]interface{}

func (v jsonValue) toBytes() ([]byte, error) {
	return json.Marshal(v)
}

func createBlockCouchDoc(storeEntries *storeEntries, blockNumber uint64, purgeInterval uint64) (*couchdb.CouchDoc, error) {
	jsonMap := make(jsonValue)
	jsonMap[idField] = blockNumberToKey(blockNumber)
	jsonMap[purgeIntervalField] = strconv.FormatUint(purgeInterval, 10)

	dataJSON, err := dataEntriesToJSONValue(storeEntries.dataEntries)
	if err != nil {
		return nil, err
	}
	jsonMap[dataField] = dataJSON

	missingDataEntriesJSON, err := missingDataEntriesToJSONValue(storeEntries.missingDataEntries)
	if err != nil {
		return nil, err
	}
	jsonMap[missingDataField] = missingDataEntriesJSON

	ei, err := expiryEntriesToJSONValue(storeEntries.expiryEntries)
	if err != nil {
		return nil, err
	}

	purgeBlockNumber, err := getPurgeBlockNumber(ei.expiryKeys, purgeInterval)
	if err != nil {
		return nil, err
	}
	logger.Debugf("Setting next purge block[%s] for block[%d]", purgeBlockNumber, blockNumber)

	jsonMap[expiryField] = ei.json
	jsonMap[purgeBlockNumberField] = purgeBlockNumber
	jsonMap[expiryBlockNumbersField] = ei.expiryKeys

	jsonBytes, err := jsonMap.toBytes()
	if err != nil {
		return nil, err
	}

	couchDoc := couchdb.CouchDoc{JSONValue: jsonBytes}

	return &couchDoc, nil
}

func getPurgeBlockNumber(expiryBlocks []string, purgeInterval uint64) (string, error) {

	var expiryBlockNumbers []uint64
	if len(expiryBlocks) >= 1 {

		for _, pvtBlockNum := range expiryBlocks {
			n, err := strconv.ParseUint(pvtBlockNum, blockNumberBase, 64)
			if err != nil {
				return "", err
			}
			expiryBlockNumbers = append(expiryBlockNumbers, uint64(n))
		}

		purgeAt := minInSlice(expiryBlockNumbers)
		if purgeAt%purgeInterval != 0 {
			purgeAt = purgeAt + (purgeInterval - purgeAt%purgeInterval)
		}

		return blockNumberToPurgeBlockKey(purgeAt), nil
	}

	return "", nil

}

func blockNumberToKey(blockNum uint64) string {
	return blockKeyPrefix + strconv.FormatUint(blockNum, 10)
}

func blockNumberToPurgeBlockKey(blockNum uint64) string {
	return blockKeyPrefix + fmt.Sprintf("%064s", strconv.FormatUint(blockNum, 10))
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

func missingDataEntriesToJSONValue(missingDataEntries map[missingDataKey]*bitset.BitSet) (jsonValue, error) {
	data := make(jsonValue)

	for k, v := range missingDataEntries {
		keyBytes := encodeMissingDataKey(&k)
		valBytes, err := encodeMissingDataValue(v)
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
	expiryKeys []string
}

func expiryEntriesToJSONValue(expiryEntries []*expiryEntry) (*expiryInfo, error) {
	ei := expiryInfo{
		json:       make(jsonValue),
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
			expiryBlockCounted[expiryEntry.key.expiringBlk] = true
		}
	}

	return &ei, nil
}

type blockPvtDataResponse struct {
	ID            string            `json:"_id"`
	Rev           string            `json:"_rev"`
	PurgeInterval string            `json:"purge_interval"`
	PurgeBlock    string            `json:"purge_block_number"`
	ExpiryBlocks  []string          `json:"expiry_block_numbers"`
	Data          map[string][]byte `json:"data"`
	Expiry        map[string][]byte `json:"expiry"`
	Deleted       bool              `json:"_deleted"`
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

	purgeInterval := ledgerconfig.GetPvtdataStorePurgeInterval()
	limit := ledgerconfig.GetInternalQueryLimit()
	if purgeInterval > uint64(limit) {
		return nil, errors.Errorf("Purge cannot be performed successfully since purge interval[%d] is greater than query limit[%d]", purgeInterval, limit)
	}

	skip := 0
	const queryFmt = `
	{
		"selector": {
			"` + purgeBlockNumberField + `":  { "$lte": "%s" },
			"$nor": [{ "` + purgeBlockNumberField + `": "" }]
		},
		"use_index": ["_design/` + purgeBlockNumberIndexDoc + `", "` + purgeBlockNumberIndexName + `"],
    	"limit": %d,
    	"skip": %d
	}`

	results, _, err := db.QueryDocuments(fmt.Sprintf(queryFmt, id, limit, skip))
	if err != nil {
		return nil, err
	}

	logger.Debugf("Number of blocks that meet purge criteria: %d", len(results))

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

func minInSlice(a []uint64) uint64 {
	min := a[0]
	for _, v := range a {
		if min > v {
			min = v
		}
	}
	return min
}
