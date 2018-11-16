/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbpvtdata

import (
	"encoding/hex"
	"encoding/json"
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
	purgeInterval    uint64

	commonStore
}

func newStore(db *couchdb.CouchDatabase) (*store, error) {
	s := store{
		db: db,
		purgeInterval: ledgerconfig.GetPvtdataStorePurgeInterval(),
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
	if len(pvtData) > 0 {
		dataEntries, expiryEntries, err := prepareStoreEntries(blockNum, pvtData, s.btlPolicy)
		if err != nil {
			return err
		}

		blockDoc, err := createBlockCouchDoc(dataEntries, expiryEntries, blockNum, s.purgeInterval)
		if err != nil {
			return err
		}
		docs = append(docs, blockDoc)
	}

	// TODO: see if we should save a call by not updating metadata when block has no private data.
	metadataDoc, err := createMetadataDoc(s.couchMetadataRev, true, s.lastCommittedBlock)
	if err != nil {
		return err
	}
	docs = append(docs, metadataDoc)

	revs, err := s.db.CommitDocuments(docs)
	if err != nil {
		return errors.WithMessage(err, fmt.Sprintf("writing private data to CouchDB failed [%d]", blockNum))
	}

	s.couchMetadataRev = revs[metadataKey]

	return nil
}

func (s *store) commitDB(committingBlockNum uint64) error {
	err := s.updateCommitMetadata(false, committingBlockNum)
	if err != nil {
		return errors.WithMessage(err, fmt.Sprintf("private data commit metadata update failed in commit [%d]", committingBlockNum))
	}

	return nil
}

func (s *store) getPvtDataByBlockNumDB(blockNum uint64) (map[string][]byte, error) {
	pd, err := retrieveBlockPvtData(s.db, strconv.FormatUint(blockNum, blockNumberBase))
	if err != nil {
		return nil, err
	}

	return pd.Data, nil
}

func (s *store) getExpiryEntriesDB(blockNum uint64) (map[string][]byte, error) {
	pds, err := retrieveBlockExpiryData(s.db, strconv.FormatUint(blockNum, blockNumberBase))
	if err != nil {
		return nil, err
	}

	expiries := make(map[string][]byte)
	for _, pd := range pds {
		for k, v := range pd.Expiry {
			expiries[k] = v
		}
	}

	return expiries, nil
}

func (s *store) purgeExpiredDataDB(maxBlkNum uint64, expiryEntries []*expiryEntry) error {
	blockToExpiryEntries := make(map[uint64][]*expiryEntry)
	for _, e := range expiryEntries {
		blockToExpiryEntries[e.key.committingBlk] = append(blockToExpiryEntries[e.key.committingBlk], e)
	}

	for k, e := range blockToExpiryEntries {
		err := s.purgeExpiredDataForBlockDB(k, maxBlkNum, e)
		if err != nil {
			return nil
		}
	}

	return nil
}

func (s *store) purgeExpiredDataForBlockDB(blockNumber uint64, maxBlkNum uint64, expiryEntries []*expiryEntry) error {
	blockPvtData, err := retrieveBlockPvtData(s.db, blockNumberToKey(blockNumber))
	if err != nil {
		return err
	}

	for _, expiryKey := range expiryEntries {
		dataKeys := deriveDataKeys(expiryKey)
		for _, dataKey := range dataKeys {
			keyBytes := encodeDataKey(dataKey)
			delete(blockPvtData.Data, hex.EncodeToString(keyBytes))
		}

		expiryBytes := encodeExpiryKey(expiryKey.key)
		delete(blockPvtData.Expiry, hex.EncodeToString(expiryBytes))
	}

	var purgeBlockNumbers []string
	for _, pvtBlockNum := range blockPvtData.PurgeBlocks {
		if pvtBlockNum != blockNumberToKey(maxBlkNum) {
			purgeBlockNumbers = append(purgeBlockNumbers, pvtBlockNum)
		}
	}
	blockPvtData.PurgeBlocks = purgeBlockNumbers

	var expiryBlockNumbers []string
	for _, pvtBlockNum := range blockPvtData.ExpiryBlocks {
		n, err := strconv.ParseUint(pvtBlockNum, blockNumberBase, 64)
		if err != nil {
			return err
		}
		if n > maxBlkNum {
			expiryBlockNumbers = append(expiryBlockNumbers, pvtBlockNum)
		}
	}
	blockPvtData.ExpiryBlocks = expiryBlockNumbers

	jsonBytes, err := json.Marshal(blockPvtData)
	if err != nil {
		return err
	}

	couchDoc := couchdb.CouchDoc{JSONValue: jsonBytes}
	_, err = s.db.UpdateDoc(blockPvtData.ID, blockPvtData.Rev, &couchDoc)
	if err != nil {
		return err
	}

	return nil
}

func (s *store) updateCommitMetadata(pending bool, blockNum uint64) error {
	m := metadata{
		pending:           pending,
		lastCommitedBlock: blockNum,
	}

	rev, err := updateCommitMetadataDoc(s.db, &m, s.couchMetadataRev)
	if err != nil {
		return err
	}

	s.couchMetadataRev = rev
	return nil
}
