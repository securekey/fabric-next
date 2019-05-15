/*
Copyright SecureKey Technologies Inc. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package dbstore

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"strings"
	"time"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/extensions/collections/transientdata/storeprovider/store/api"
	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("transientdb")

var compositeKeySep = "!"

// DBStore holds the db handle and the db name
type DBStore struct {
	db     *leveldbhelper.DBHandle
	dbName string
}

// newDBStore constructs an instance of db store
func newDBStore(db *leveldbhelper.DBHandle, dbName string) *DBStore {
	return &DBStore{db, dbName}
}

// AddKey add cache key to db
func (s *DBStore) AddKey(key api.Key, value *api.Value) error {
	encodeVal, err := encodeCacheVal(value)
	if err != nil {
		return errors.Wrapf(err, "failed to encode transientdata value %s", value)
	}
	// put key in db
	err = s.db.Put(encodeCacheKey(key, time.Time{}), encodeVal, true)
	if err != nil {
		return errors.Wrapf(err, "failed to save transientdata key %s in db", key)
	}

	if !value.ExpiryTime.IsZero() {
		// put same previous key with prefix expiryTime so the clean up can remove all expired keys
		err = s.db.Put(encodeCacheKey(key, value.ExpiryTime), []byte(""), true)
		if err != nil {
			return errors.Wrapf(err, "failed to save transientdata key %s in db", key)
		}
	}

	return nil
}

// GetKey get cache key from db
func (s *DBStore) GetKey(key api.Key) (*api.Value, error) {
	logger.Debugf("load transientdata key %s from db", key)
	value, err := s.db.Get(encodeCacheKey(key, time.Time{}))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to load transientdata key %s from db", key)
	}
	if value != nil {
		val, err := decodeCacheVal(value)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to decode transientdata value %s", value)
		}
		return val, nil
	}
	return nil, nil
}

// DeleteExpiredKeys delete expired keys from db
func (s *DBStore) DeleteExpiredKeys() error {
	dbBatch := leveldbhelper.NewUpdateBatch()
	itr := s.db.GetIterator(nil, []byte(fmt.Sprintf("%d%s", time.Now().UTC().UnixNano(), compositeKeySep)))
	for itr.Next() {
		key := string(itr.Key())
		dbBatch.Delete([]byte(key))
		dbBatch.Delete([]byte(key[strings.Index(key, compositeKeySep)+1:]))
	}
	if dbBatch.Len() > 0 {
		err := s.db.WriteBatch(dbBatch, true)
		if err != nil {
			return errors.Errorf("failed to delete transient data keys %s in db %s", dbBatch.KVs, err.Error())
		}
		logger.Debugf("delete expired keys %s from db", dbBatch.KVs)
	}

	return nil
}

// Close db
func (s *DBStore) Close() {
}

func encodeCacheKey(key api.Key, expiryTime time.Time) []byte {
	var compositeKey []byte
	if !expiryTime.IsZero() {
		compositeKey = append(compositeKey, []byte(fmt.Sprintf("%d", expiryTime.UnixNano()))...)
		compositeKey = append(compositeKey, compositeKeySep...)
	}
	compositeKey = append(compositeKey, []byte(key.Namespace)...)
	compositeKey = append(compositeKey, compositeKeySep...)
	compositeKey = append(compositeKey, []byte(key.Collection)...)
	compositeKey = append(compositeKey, compositeKeySep...)
	compositeKey = append(compositeKey, []byte(key.Key)...)
	return compositeKey
}

func decodeCacheVal(b []byte) (*api.Value, error) {
	decoder := gob.NewDecoder(bytes.NewBuffer(b))
	var v *api.Value
	if err := decoder.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

func encodeCacheVal(v *api.Value) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	encoder := gob.NewEncoder(buf)
	if err := encoder.Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
