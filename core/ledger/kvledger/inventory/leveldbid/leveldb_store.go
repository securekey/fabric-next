/*
Copyright IBM Corp., SecureKey Technologies. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package leveldbid

import (
	"bytes"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
)

var (
	underConstructionLedgerKey = []byte("underConstructionLedgerKey")
	ledgerKeyPrefix            = []byte("l")
)


type Store struct {
	db *leveldbhelper.DB
}

func OpenStore(path string) *Store {
	db := leveldbhelper.CreateDB(&leveldbhelper.Conf{DBPath: path})
	db.Open()
	return &Store{db}
}

func (s *Store) SetUnderConstructionFlag(ledgerID string) error {
	return s.db.Put(underConstructionLedgerKey, []byte(ledgerID), true)
}

func (s *Store) UnsetUnderConstructionFlag() error {
	return s.db.Delete(underConstructionLedgerKey, true)
}

func (s *Store) GetUnderConstructionFlag() (string, error) {
	val, err := s.db.Get(underConstructionLedgerKey)
	if err != nil {
		return "", err
	}
	return string(val), nil
}

func (s *Store) CreateLedgerID(ledgerID string, gb *common.Block) error {
	key := s.encodeLedgerKey(ledgerID)
	var val []byte
	var err error
	if val, err = proto.Marshal(gb); err != nil {
		return err
	}
	if val, err = s.db.Get(key); err != nil {
		return err
	}
	if val != nil {
		return errors.Errorf("ledger already exists [%s]", ledgerID)
	}
	batch := &leveldb.Batch{}
	batch.Put(key, val)
	batch.Delete(underConstructionLedgerKey)
	return s.db.WriteBatch(batch, true)
}

func (s *Store) LedgerIDExists(ledgerID string) (bool, error) {
	key := s.encodeLedgerKey(ledgerID)
	val := []byte{}
	err := error(nil)
	if val, err = s.db.Get(key); err != nil {
		return false, err
	}
	return val != nil, nil
}

func (s *Store) GetAllLedgerIds() ([]string, error) {
	var ids []string
	itr := s.db.GetIterator(nil, nil)
	defer itr.Release()
	itr.First()
	for itr.Valid() {
		if bytes.Equal(itr.Key(), underConstructionLedgerKey) {
			continue
		}
		id := string(s.decodeLedgerID(itr.Key()))
		ids = append(ids, id)
		itr.Next()
	}
	return ids, nil
}

func (s *Store) Close() {
	s.db.Close()
}

func (s *Store) encodeLedgerKey(ledgerID string) []byte {
	return append(ledgerKeyPrefix, []byte(ledgerID)...)
}

func (s *Store) decodeLedgerID(key []byte) string {
	return string(key[len(ledgerKeyPrefix):])
}