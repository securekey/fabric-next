/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbpvtdata

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDataKeyEncoding(t *testing.T) {
	dataKey1 := &dataKey{blkNum: 2, txNum: 5, ns: "ns1", coll: "coll1"}
	datakey2 := decodeDatakey(encodeDataKey(dataKey1))
	assert.Equal(t, dataKey1, datakey2)
}

func TestExtractCommitMap(t *testing.T) {
	expectedBlock := uint64(1212)
	expectedPending := true

	doc, err := createMetadataDoc(expectedPending, expectedBlock)
	assert.NoError(t, err)

	jsonMap, err := couchValueToJSON(doc.JSONValue)
	assert.NoError(t, err)

	commitMap, err := extractCommitMap(jsonMap)
	assert.NoError(t, err)

	pendingUT, ok := commitMap[pendingCommitField]
	assert.Truef(t, ok, "pending commit field not found in metadata")

	pending, ok := pendingUT.(bool)
	assert.Truef(t, ok, "pending JSON field is not a bool")
	assert.Equal(t, expectedPending, pending)

	lastCommitedBlockUT, ok := commitMap[blockNumberField]
	assert.Truef(t, ok, "block number field not found in metadata")

	lastCommitedBlockJSON, ok := lastCommitedBlockUT.(json.Number)
	assert.Truef(t, ok, "block number field not a JSON number")

	lastCommitedBlock, err := strconv.ParseUint(lastCommitedBlockJSON.String(), 10, 64)
	assert.NoError(t, err)
	assert.Equal(t, expectedBlock, lastCommitedBlock)
}
