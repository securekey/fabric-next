/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/
package cdbblkstorage

import (
	"fmt"

	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/pkg/errors"
)

var (
	blkMgrInfoKey = "blkMgrInfo"
)

type checkpoint struct {
	db       *couchdb.CouchDatabase
	couchRev string
}

// checkpointInfo
type checkpointInfo struct {
	isChainEmpty    bool
	lastBlockNumber uint64
}

func newCheckpoint(db *couchdb.CouchDatabase) *checkpoint {
	return &checkpoint{db: db}
}

func (cp *checkpoint) getCheckpointInfo() (*checkpointInfo, error) {
	cpInfo, err := cp.loadCurrentInfo()
	if err != nil {
		return nil, err
	}
	if cpInfo == nil {
		cpInfo = &checkpointInfo{
			isChainEmpty:    true,
			lastBlockNumber: 0,
		}
	}
	return cpInfo, nil
}

//Get the current checkpoint information that is stored in the database
func (cp *checkpoint) loadCurrentInfo() (*checkpointInfo, error) {
	doc, _, err := cp.db.ReadDoc(blkMgrInfoKey)
	if err != nil {
		return nil, errors.WithMessage(err, fmt.Sprintf("retrieval of checkpointInfo from couchDB failed [%s]", blkMgrInfoKey))
	}
	if doc == nil {
		return nil, nil
	}
	checkpointInfo, err := couchDocToCheckpointInfo(doc)
	if err != nil {
		return nil, errors.WithMessage(err, fmt.Sprintf("unmarshal of checkpointInfo from couchDB failed [%s]", blkMgrInfoKey))
	}
	logger.Debugf("loaded checkpointInfo:%s", checkpointInfo)
	return checkpointInfo, nil
}

func (cp *checkpoint) saveCurrentInfo(i *checkpointInfo) error {
	doc, err := checkpointInfoToCouchDoc(i)
	if err != nil {
		return errors.WithMessage(err, "converting checkpointInfo to couchDB document failed")
	}
	rev, err := cp.db.SaveDoc(blkMgrInfoKey, cp.couchRev, doc)
	if err != nil {
		return errors.WithMessage(err, "adding checkpointInfo to couchDB failed")
	}

	dbResponse, err := cp.db.EnsureFullCommit()
	if err != nil || dbResponse.Ok != true {
		logger.Errorf("full commit failed [%s]", err)
		return errors.WithMessage(err, "full commit failed")
	}

	cp.couchRev = rev

	return nil
}
