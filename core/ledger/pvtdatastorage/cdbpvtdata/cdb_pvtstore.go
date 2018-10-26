/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbpvtdata

import (
	"fmt"
	"strconv"

	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"

	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/pkg/errors"
)

type store struct {
	db               *couchdb.CouchDatabase
	couchMetadataRev string

	commonStore
}

func newStore(db *couchdb.CouchDatabase) (*store, error) {
	s := store{
		db: db,
	}

	if ledgerconfig.IsCommitter() {
		err := s.initState()
		if err != nil {
			return nil, err
		}
	}

	return &s, nil
}

func (s *store) initState() error {
	m, ok, err := lookupMetadata(s.db)
	if err != nil {
		return err
	}

	s.isEmpty = !ok
	if ok {
		s.lastCommittedBlock = m.lastCommitedBlock
		s.batchPending = m.pending
	}
	return nil
}

func (s *store) prepareDB(blockNum uint64, pvtData []*ledger.TxPvtData) error {
	var docs []*couchdb.CouchDoc
	dataEntries, expiryEntries, err := prepareStoreEntries(blockNum, pvtData, s.btlPolicy)
	if err != nil {
		return err
	}

	dataEntryDocs, err := dataEntriesToCouchDocs(dataEntries, blockNum)
	if err != nil {
		return err
	}
	docs = append(docs, dataEntryDocs...)

	expiryEntryDocs, err := expiryEntriesToCouchDocs(expiryEntries, blockNum)
	if err != nil {
		return err
	}
	docs = append(docs, expiryEntryDocs...)

	if len(docs) > 0 {
		_, err = s.db.CommitDocuments(docs)
		if err != nil {
			return errors.WithMessage(err, fmt.Sprintf("writing private data to CouchDB failed [%d]", blockNum))
		}
	}

	err = s.updateCommitMetadata(true)
	if err != nil {
		return errors.WithMessage(err, fmt.Sprintf("private data commit metadata update failed in prepare [%d]", blockNum))
	}

	return nil
}

func (s *store) commitDB(committingBlockNum uint64) error {
	m := metadata{
		pending:           false,
		lastCommitedBlock: committingBlockNum,
	}

	rev, err := updateCommitMetadataDoc(s.db, &m, s.couchMetadataRev)
	if err != nil {
		return errors.WithMessage(err, fmt.Sprintf("private data commit metadata update failed in commit [%d]", committingBlockNum))
	}

	s.couchMetadataRev = rev

	return nil
}

func (s *store) getPvtDataByBlockNumDB(blockNum uint64) (map[string][]byte, error) {
	const queryFmt = `
	{
		"selector": {
			"` + blockNumberField + `": {
				"$eq": "%s"
			}
		},
		"use_index": ["_design/` + blockNumberIndexDoc + `", "` + blockNumberIndexName + `"]
	}`

	return retrievePvtDataQuery(s.db, fmt.Sprintf(queryFmt, fmt.Sprintf("%064s", strconv.FormatUint(blockNum, blockNumberBase))))
}

func (s *store) getExpiryEntriesDB(blockNum uint64) (map[string][]byte, error) {
	const queryFmt = `
	{
		"selector": {
			"` + blockNumberExpiryField + `": {
				"$lte": "%s"
			}
		},
		"use_index": ["_design/` + blockNumberExpiryIndexDoc + `", "` + blockNumberExpiryIndexName + `"]
	}`
	results, err := retrievePvtDataQuery(s.db, fmt.Sprintf(queryFmt, fmt.Sprintf("%064s", strconv.FormatUint(blockNum, blockNumberBase))))
	if _, ok := err.(*NotFoundInIndexErr); ok {
		return nil, nil
	}
	return results, err
}

func (s *store) purgeExpiredDataDB(key string) error {
	return s.db.DeleteDoc(key, "")
}

func (s *store) updateCommitMetadata(pending bool) error {
	m := metadata{
		pending:           pending,
		lastCommitedBlock: s.lastCommittedBlock,
	}

	rev, err := updateCommitMetadataDoc(s.db, &m, s.couchMetadataRev)
	if err != nil {
		return err
	}

	s.couchMetadataRev = rev
	return nil
}

func (s *store) initLastCommittedBlockDB(blockNum uint64) error {
	m := metadata{
		pending:           false,
		lastCommitedBlock: blockNum,
	}

	rev, err := updateCommitMetadataDoc(s.db, &m, s.couchMetadataRev)
	if err != nil {
		return err
	}

	s.couchMetadataRev = rev
	return nil

}
