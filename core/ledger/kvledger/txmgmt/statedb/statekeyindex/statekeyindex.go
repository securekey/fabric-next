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
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
)

var compositeKeySep = []byte{0x00}

type stateKeyIndex struct {
	db     *leveldbhelper.DBHandle
	dbName string
}

// newStateKeyIndex constructs an instance of StateKeyIndex
func newStateKeyIndex(db *leveldbhelper.DBHandle, dbName string) *stateKeyIndex {
	return &stateKeyIndex{db, dbName}
}

func (s *stateKeyIndex) AddIndex(keys []statedb.CompositeKey) error {
	dbBatch := leveldbhelper.NewUpdateBatch()
	for _, v := range keys {
		compositeKey := constructCompositeKey(v.Namespace, v.Key)
		logger.Infof("Channel [%s]: Applying key(string)=[%s] key(bytes)=[%#v]", s.dbName, string(compositeKey), compositeKey)
		dbBatch.Put(compositeKey, []byte(""))
	}
	// Setting snyc to true as a precaution, false may be an ok optimization after further testing.
	if err := s.db.WriteBatch(dbBatch, true); err != nil {
		return err
	}
	return nil
}
func (s *stateKeyIndex) Close() {

}

func constructCompositeKey(ns string, key string) []byte {
	return append(append([]byte(ns), compositeKeySep...), []byte(key)...)
}
