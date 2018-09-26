/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbpvtdata

import (
	"fmt"
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
	// Note: this is a copy of the base implementation
	s.btlPolicy = btlPolicy
}

func (s *store) InitLastCommittedBlock(blockNum uint64) error {
	// TODO: fill in the rest

	s.isEmpty = false
	s.lastCommittedBlock = blockNum
	logger.Debugf("InitLastCommittedBlock set to block [%d]", blockNum)
	return nil
}

func (s *store) Prepare(blockNum uint64, pvtData []*ledger.TxPvtData) error {
	err := s.prepareInit(blockNum, pvtData)
	if err != nil {
		logger.Debugf("TODO - this will not work until more is filled in [%s]", err)
		// TODO return error
		// return err
	}

	var docs []*couchdb.CouchDoc
	dataEntries, expiryEntries, err := prepareStoreEntries(blockNum, pvtData, s.btlPolicy)
	if err != nil {
		return err
	}

	dataEntryDocs, err := dataEntriesToCouchDocs(dataEntries)
	if err != nil {
		return err
	}
	docs = append(docs, dataEntryDocs...)

	expiryEntryDocs, err := expiryEntriesToCouchDocs(expiryEntries)
	if err != nil {
		return err
	}
	docs = append(docs, expiryEntryDocs...)

	if len(docs) > 0 {
		_, err = s.db.BatchUpdateDocuments(docs)
		if err != nil {
			return errors.WithMessage(err, fmt.Sprintf("writing private data to CouchDB failed [%d]", blockNum))
		}
	}

	return s.prepareDone(blockNum, pvtData)
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
