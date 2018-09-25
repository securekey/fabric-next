/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/
package cdbblkstorage

import (
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/pkg/errors"
)

var (
	blkMgrInfoKey = "blkMgrInfo"
)

type checkpoint struct {
	db *couchdb.CouchDatabase
}

// checkpointInfo
type checkpointInfo struct {
	isChainEmpty    bool
	lastBlockHash   []byte
	lastBlockNumber uint64
}

func newCheckpoint(db *couchdb.CouchDatabase) *checkpoint {
	return &checkpoint{db: db}
}

func (cp *checkpoint) getCheckpointInfo() *checkpointInfo {
	cpInfo, err := cp.loadCurrentInfo()
	if err != nil {
		panic(fmt.Sprintf("Could not get block file info for current block file from db: %s", err))
	}
	if cpInfo == nil {
		cpInfo = &checkpointInfo{
			isChainEmpty:  true,
			lastBlockHash: nil}
	}
	return cpInfo
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
	_, err = cp.db.SaveDoc(blkMgrInfoKey, "", doc)
	if err != nil {
		return errors.WithMessage(err, "adding checkpointInfo to couchDB failed")
	}
	return nil
}

func (i *checkpointInfo) marshal() ([]byte, error) {
	buffer := proto.NewBuffer([]byte{})
	var err error
	if err = buffer.EncodeRawBytes(i.lastBlockHash); err != nil {
		return nil, err
	}
	var chainEmptyMarker uint64
	if i.isChainEmpty {
		chainEmptyMarker = 1
	}
	if err = buffer.EncodeVarint(chainEmptyMarker); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (i *checkpointInfo) unmarshal(b []byte) error {
	buffer := proto.NewBuffer(b)
	var val []byte
	var chainEmptyMarker uint64
	var err error
	if val, err = buffer.DecodeRawBytes(false); err != nil {
		return err
	}
	i.lastBlockHash = val
	if chainEmptyMarker, err = buffer.DecodeVarint(); err != nil {
		return err
	}
	i.isChainEmpty = chainEmptyMarker == 1
	return nil
}
