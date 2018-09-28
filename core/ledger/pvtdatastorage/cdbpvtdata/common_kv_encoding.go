/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbpvtdata

import (
	"bytes"

	"github.com/hyperledger/fabric/core/ledger/pvtdatastorage/pvtmetadata"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/protos/ledger/rwset"
)

var (
	pendingCommitKey    = []byte{0}
	lastCommittedBlkkey = []byte{1}
	pvtDataKeyPrefix    = []byte{2}
	expiryKeyPrefix     = []byte{3}

	nilByte    = byte(0)
	emptyValue = []byte{}
)

func encodeDataKey(key *dataKey) []byte {
	dataKeyBytes := append(pvtDataKeyPrefix, version.NewHeight(key.blkNum, key.txNum).ToBytes()...)
	dataKeyBytes = append(dataKeyBytes, []byte(key.ns)...)
	dataKeyBytes = append(dataKeyBytes, nilByte)
	return append(dataKeyBytes, []byte(key.coll)...)
}

func encodeDataValue(collData *rwset.CollectionPvtReadWriteSet) ([]byte, error) {
	return proto.Marshal(collData)
}

func encodeExpiryKey(expiryKey *expiryKey) []byte {
	// reusing version encoding scheme here
	return append(expiryKeyPrefix, version.NewHeight(expiryKey.expiringBlk, expiryKey.committingBlk).ToBytes()...)
}

func encodeExpiryValue(expiryData *pvtmetadata.ExpiryData) ([]byte, error) {
	return proto.Marshal(expiryData)
}

func decodeDatakey(datakeyBytes []byte) *dataKey {
	v, n := version.NewHeightFromBytes(datakeyBytes[1:])
	blkNum := v.BlockNum
	tranNum := v.TxNum
	remainingBytes := datakeyBytes[n+1:]
	nilByteIndex := bytes.IndexByte(remainingBytes, nilByte)
	ns := string(remainingBytes[:nilByteIndex])
	coll := string(remainingBytes[nilByteIndex+1:])
	return &dataKey{blkNum: blkNum, txNum: tranNum, ns: ns, coll: coll}
}

func decodeDataValue(datavalueBytes []byte) (*rwset.CollectionPvtReadWriteSet, error) {
	collPvtdata := &rwset.CollectionPvtReadWriteSet{}
	err := proto.Unmarshal(datavalueBytes, collPvtdata)
	return collPvtdata, err
}
