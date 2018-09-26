/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbpvtdata

import (
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/pvtdatapolicy"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/pkg/errors"
)

type store struct {
	db *couchdb.CouchDatabase

	commonStore
}

func newStore(db *couchdb.CouchDatabase) *store {
	s := store {
		db: db,
	}

	return &s
}


func (s *store) Init(btlPolicy pvtdatapolicy.BTLPolicy) {

}

func (s *store) InitLastCommittedBlock(blockNum uint64) error {
	return errors.New("not implemented")
}

func (s *store) Prepare(blockNum uint64, pvtData []*ledger.TxPvtData) error {
	err := s.prepareInit(blockNum, pvtData)
	if err != nil {
		return err
	}

	// TODO: Add CouchDB logic here!

	s.prepareDone(blockNum, pvtData)
	if err != nil {
		return err
	}

	return errors.New("not implemented")
}

func (s *store) Commit() error {
	return errors.New("not implemented")
}

func (s *store) Rollback() error {
	return errors.New("not implemented")
}

func (s *store) GetPvtDataByBlockNum(blockNum uint64, filter ledger.PvtNsCollFilter) ([]*ledger.TxPvtData, error) {
	return nil, errors.New("not implemented")
}

func (s *store) IsEmpty() (bool, error) {
	return false, errors.New("not implemented")
}

func (s *store) LastCommittedBlockHeight() (uint64, error) {
	return 0, errors.New("not implemented")
}

func (s *store) HasPendingBatch() (bool, error) {
	return false, errors.New("not implemented")
}

func (s *store) Shutdown() {

}
