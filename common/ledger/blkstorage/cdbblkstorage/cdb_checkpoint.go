/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/
package cdbblkstorage

import (
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"strings"
)

// checkpointInfo
type checkpointInfo struct {
	isChainEmpty    bool
	lastBlockNumber uint64
}

//Get the current checkpoint information that is stored in the database
func retrieveCheckpointInfo(db *couchdb.CouchDatabase) (checkpointInfo, error) {
	results, err := db.ReadDocRange("", "", numIndexDocs + 1, 0, true)
	if err != nil {
		return checkpointInfo{}, err
	}

	var result *couchdb.QueryResult

	for _, r := range results {
		if !strings.HasPrefix(r.ID, "_") {
			result = r
		}
	}

	if result == nil {
		return checkpointInfo{isChainEmpty: true}, nil
	}

	block, err := couchAttachmentsToBlock(result.Attachments)
	if err != nil {
		return checkpointInfo{}, err
	}

	return checkpointInfo{
		isChainEmpty: false,
		lastBlockNumber: block.GetHeader().GetNumber(),
	}, nil
}
