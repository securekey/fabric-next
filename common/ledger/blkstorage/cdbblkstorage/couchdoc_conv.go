/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbblkstorage

import (
	"encoding/json"
	"github.com/gogo/protobuf/proto"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/pkg/errors"
	"strconv"
)

const (
	idField             = "_id"
	blockAttachmentName = "block"
	blockKeyPrefix      = ""
)

type jsonValue map[string]interface{}

func (v jsonValue) toBytes() ([]byte, error) {
	return json.Marshal(v)
}

func blockToCouchDoc(block *common.Block) (*couchdb.CouchDoc, error) {
	jsonMap := make(jsonValue)

	key := blockNumberToKey(block.GetHeader().Number)

	// add the version, id, revision, and delete marker (if needed)
	jsonMap[idField] = key
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
	var blockBytes []byte
	block := common.Block{}

	// get binary data from attachment
	for _, a := range doc.Attachments {
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

func blockNumberToKey(blockNum uint64) string {
	return blockKeyPrefix + strconv.FormatUint(blockNum, 10)
}