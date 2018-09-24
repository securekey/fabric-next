/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbblkstorage

import (
	"encoding/hex"
	"encoding/json"
	"strconv"

	"github.com/gogo/protobuf/proto"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/pkg/errors"
)

const (
	idField             = "_id"
	blockHashField      = "hash"
	blockHashIndexName  = "by_hash"
	blockHashIndexDoc   = "indexHash"
	blockAttachmentName = "block"
	cpiAttachmentName   = "checkpointinfo"
	blockKeyPrefix      = ""
)

const blockHashIndexDef = `
	{
		"index": {
			"fields": ["` + blockHashField + `"]
		},
		"name": "` + blockHashIndexName + `",
		"ddoc": "` + blockHashIndexDoc + `",
		"type": "json"
	}`

type jsonValue map[string]interface{}

func (v jsonValue) toBytes() ([]byte, error) {
	return json.Marshal(v)
}

func blockToCouchDoc(block *common.Block) (*couchdb.CouchDoc, error) {
	jsonMap := make(jsonValue)

	blockHeader := block.GetHeader()
	key := blockNumberToKey(blockHeader.Number)
	blockHashHex := hex.EncodeToString(blockHeader.Hash())

	jsonMap[idField] = key
	jsonMap[blockHashField] = blockHashHex

	jsonBytes, err := jsonMap.toBytes()
	if err != nil {
		return nil, err
	}
	couchDoc := &couchdb.CouchDoc{JSONValue: jsonBytes}

	attachment, err := blockToAttachment(block)
	if err != nil {
		return nil, err
	}

	attachments := append([]*couchdb.AttachmentInfo{}, attachment)
	couchDoc.Attachments = attachments
	return couchDoc, nil
}

func checkpointInfoToCouchDoc(i *checkpointInfo) (*couchdb.CouchDoc, error) {
	jsonMap := make(jsonValue)

	jsonMap[idField] = blkMgrInfoKey

	jsonBytes, err := jsonMap.toBytes()
	if err != nil {
		return nil, err
	}
	couchDoc := &couchdb.CouchDoc{JSONValue: jsonBytes}

	attachment, err := checkpointInfoToAttachment(i)
	if err != nil {
		return nil, err
	}

	attachments := append([]*couchdb.AttachmentInfo{}, attachment)
	couchDoc.Attachments = attachments
	return couchDoc, nil
}

func checkpointInfoToAttachment(i *checkpointInfo) (*couchdb.AttachmentInfo, error) {
	checkpointInfoBytes, err := i.marshal()
	if err != nil {
		return nil, errors.Wrapf(err, "marshaling checkpointInfo failed")
	}

	attachment := &couchdb.AttachmentInfo{}
	attachment.AttachmentBytes = checkpointInfoBytes
	attachment.ContentType = "application/octet-stream"
	attachment.Name = cpiAttachmentName

	return attachment, nil
}

func blockToAttachment(block *common.Block) (*couchdb.AttachmentInfo, error) {
	blockBytes, err := proto.Marshal(block)
	if err != nil {
		return nil, errors.Wrapf(err, "marshaling block failed")
	}

	attachment := &couchdb.AttachmentInfo{}
	attachment.AttachmentBytes = blockBytes
	attachment.ContentType = "application/octet-stream"
	attachment.Name = blockAttachmentName

	return attachment, nil
}

func couchDocToBlock(doc *couchdb.CouchDoc) (*common.Block, error) {
	return couchAttachmentsToBlock(doc.Attachments)
}

func couchAttachmentsToBlock(attachments []*couchdb.AttachmentInfo) (*common.Block, error) {
	var blockBytes []byte
	block := common.Block{}

	// get binary data from attachment
	for _, a := range attachments {
		if a.Name == blockAttachmentName {
			blockBytes = a.AttachmentBytes
		}
	}

	if len(blockBytes) == 0 {
		return nil, errors.New("block is not within couchDB document")
	}

	err := proto.Unmarshal(blockBytes, &block)
	if err != nil {
		return nil, errors.Wrapf(err, "block from couchDB document could not be unmarshaled")
	}

	return &block, nil
}

func couchDocToCheckPointInfo(doc *couchdb.CouchDoc) (*checkpointInfo, error) {
	return couchAttachmentsToCheckPointInfo(doc.Attachments)
}

func couchAttachmentsToCheckPointInfo(attachments []*couchdb.AttachmentInfo) (*checkpointInfo, error) {
	var checkpointInfoBytes []byte
	checkpointInfo := checkpointInfo{}
	// get binary data from attachment
	for _, a := range attachments {
		if a.Name == cpiAttachmentName {
			checkpointInfoBytes = a.AttachmentBytes
		}
	}
	if len(checkpointInfoBytes) == 0 {
		return nil, errors.New("checkpointInfo is not within couchDB document")
	}
	err := checkpointInfo.unmarshal(checkpointInfoBytes)
	if err != nil {
		return nil, errors.Wrapf(err, "checkpointInfo from couchDB document could not be unmarshaled")
	}
	return &checkpointInfo, nil
}

func blockNumberToKey(blockNum uint64) string {
	return blockKeyPrefix + strconv.FormatUint(blockNum, 10)
}
