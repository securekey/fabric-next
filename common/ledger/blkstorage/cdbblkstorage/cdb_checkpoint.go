/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/
package cdbblkstorage

import (
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/hyperledger/fabric/protos/common"
)

// checkpointInfo
type checkpointInfo struct {
	isChainEmpty    bool
	lastBlockNumber uint64
}

//Get the current checkpoint information that is stored in the database
func retrieveCheckpointInfo(db *couchdb.CouchDatabase) (checkpointInfo, error) {
	info,_, err := db.GetDatabaseInfo()
	if err != nil {
		return checkpointInfo{}, err
	}

	var lastBlock *common.Block
	for i := 1; i <= min(info.DocCount, numMetaDocs + 1); i++ {
		doc, _, err := db.ReadDoc(blockNumberToKey(uint64(info.DocCount - i)))
		if err != nil {
			return checkpointInfo{}, err
		}

		if doc != nil {
			lastBlock, err = couchDocToBlock(doc)
			if err != nil {
				return checkpointInfo{}, err
			}
			break
		}
	}

	if lastBlock == nil {
		return checkpointInfo{isChainEmpty: true}, nil
	}

	return checkpointInfo{
		isChainEmpty: false,
		lastBlockNumber: lastBlock.GetHeader().GetNumber(),
	}, nil
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}