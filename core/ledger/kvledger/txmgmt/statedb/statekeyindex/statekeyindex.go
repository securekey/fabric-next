/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package statekeyindex

import (
	"bytes"

	"github.com/golang/protobuf/proto"

	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
)

var compositeKeySep = []byte{0x00}
var lastKeyIndicator = byte(0x01)

type stateKeyIndex struct {
	db     *leveldbhelper.DBHandle
	dbName string
}

type Metadata struct {
	BlockNumber uint64
	TxNumber    uint64
	DBTag       string
}

type IndexUpdate struct {
	Key   CompositeKey
	Value Metadata
}

// MarshalMetadata marshals a Metadata into a byte slice.
func MarshalMetadata(m *Metadata) ([]byte, error) {
	buffer := proto.NewBuffer([]byte{})
	err := buffer.EncodeVarint(m.BlockNumber)
	if err != nil {
		return nil, err
	}
	err = buffer.EncodeVarint(m.TxNumber)
	if err != nil {
		return nil, err
	}

	err = buffer.EncodeStringBytes(m.DBTag)
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// UnmarshalMetadata unmarshals the byte slice into a Metadata.
func UnmarshalMetadata(b []byte) (Metadata, error) {
	buffer := proto.NewBuffer(b)

	blockNumber, err := buffer.DecodeVarint()
	if err != nil {
		return Metadata{}, err
	}

	txNumber, err := buffer.DecodeVarint()
	if err != nil {
		return Metadata{}, err
	}

	cdbRev, err := buffer.DecodeStringBytes()
	if err != nil {
		return Metadata{}, err
	}

	return Metadata{
		BlockNumber: blockNumber,
		TxNumber:    txNumber,
		DBTag:       cdbRev,
	}, nil
}

// newStateKeyIndex constructs an instance of StateKeyIndex
func newStateKeyIndex(db *leveldbhelper.DBHandle, dbName string) *stateKeyIndex {
	return &stateKeyIndex{db, dbName}
}

func (s *stateKeyIndex) AddIndex(indexUpdates []*IndexUpdate) error {
	dbBatch := leveldbhelper.NewUpdateBatch()
	for _, u := range indexUpdates {
		compositeKey := ConstructCompositeKey(u.Key.Namespace, u.Key.Key)
		logger.Debugf("[%s] adding index for state key [%s, %#v]", s.dbName, string(compositeKey), u.Value)

		metadata, err := MarshalMetadata(&u.Value)
		if err != nil {
			return err
		}
		dbBatch.Put(compositeKey, metadata)
	}
	// Setting snyc to true as a precaution, false may be an ok optimization after further testing.
	if err := s.db.WriteBatch(dbBatch, true); err != nil {
		return err
	}
	return nil
}

func (s *stateKeyIndex) DeleteIndex(keys []CompositeKey) error {
	dbBatch := leveldbhelper.NewUpdateBatch()
	for _, v := range keys {
		compositeKey := ConstructCompositeKey(v.Namespace, v.Key)
		//TODO change to DEBUG
		logger.Debugf("Channel [%s]: Delete key(string)=[%s]", s.dbName, string(compositeKey))
		dbBatch.Delete(compositeKey)
	}
	// Setting snyc to true as a precaution, false may be an ok optimization after further testing.
	if err := s.db.WriteBatch(dbBatch, true); err != nil {
		return err
	}
	return nil
}

// GetIterator implements method in StateKeyIndex interface
// startKey is inclusive
// endKey is exclusive
func (s *stateKeyIndex) GetIterator(namespace string, startKey string, endKey string) *leveldbhelper.Iterator {
	compositeStartKey := ConstructCompositeKey(namespace, startKey)
	compositeEndKey := ConstructCompositeKey(namespace, endKey)
	if endKey == "" {
		compositeEndKey[len(compositeEndKey)-1] = lastKeyIndicator
	}
	//TODO change to DEBUG
	logger.Debugf("Channel [%s]: GetIterator compositeStartKey(string)=[%s] compositeEndKey(string)=[%s]", s.dbName, compositeStartKey, compositeEndKey)
	return s.db.GetIterator(compositeStartKey, compositeEndKey)
}

func (s *stateKeyIndex) Close() {
}

func (s *stateKeyIndex) GetMetadata(key *CompositeKey) (Metadata, bool, error) {
	data, err := s.db.Get(ConstructCompositeKey(key.Namespace, key.Key))
	if err != nil {
		return Metadata{}, false, err
	}
	if data != nil {
		md, err := UnmarshalMetadata(data)
		if err != nil {
			return Metadata{}, false, err
		} else {
			return md, true, err
		}
	}
	return Metadata{}, false, nil
}

func ConstructCompositeKey(ns string, key string) []byte {
	return append(append([]byte(ns), compositeKeySep...), []byte(key)...)
}

func SplitCompositeKey(compositeKey []byte) (string, string) {
	split := bytes.SplitN(compositeKey, compositeKeySep, 2)
	return string(split[0]), string(split[1])
}
