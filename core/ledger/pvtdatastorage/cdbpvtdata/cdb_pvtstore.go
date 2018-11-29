/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbpvtdata

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"strconv"

	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/pkg/errors"
)

type store struct {
	db               *couchdb.CouchDatabase
	purgeInterval    uint64
	pendingDocs      []*couchdb.CouchDoc

	commonStore
}

func newStore(db *couchdb.CouchDatabase) (*store, error) {
	s := store{
		db:            db,
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
	}
	return nil
}

func (s *store) prepareDB(blockNum uint64, pvtData []*ledger.TxPvtData) error {
	if s.pendingDocs != nil {
		return errors.New("previous commit is pending")
	}

	dataEntries, expiryEntries, err := prepareStoreEntries(blockNum, pvtData, s.btlPolicy)
	if err != nil {
		return err
	}

	blockDoc, err := createBlockCouchDoc(dataEntries, expiryEntries, blockNum, s.purgeInterval)
	if err != nil {
		return err
	}
	s.pendingDocs = append(s.pendingDocs, blockDoc)

	return nil
}

func (s *store) commitDB(committingBlockNum uint64) error {
	if s.pendingDocs == nil {
		return errors.New("no commit is pending")
	}
	_, err := s.db.CommitDocuments(s.pendingDocs)
	if err != nil {
		return errors.WithMessage(err, fmt.Sprintf("writing private data to CouchDB failed [%d]", committingBlockNum))
	}
	s.pendingDocs = nil

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
	docs := make([]*couchdb.CouchDoc, 0)
	for k, e := range blockToExpiryEntries {
		doc, err := s.purgeExpiredDataForBlockDB(k, maxBlkNum, e)
		if err != nil {
			return nil
		}
		docs = append(docs, doc)
	}
	if len(docs) > 0 {
		_, err := s.db.BatchUpdateDocuments(docs)
		if err != nil {
			return errors.WithMessage(err, fmt.Sprintf("BatchUpdateDocuments failed for [%d] documents", len(docs)))
		}
	}
	return nil
}

func (s *store) purgeExpiredDataForBlockDB(blockNumber uint64, maxBlkNum uint64, expiryEntries []*expiryEntry) (*couchdb.CouchDoc, error) {
	blockPvtData, err := retrieveBlockPvtData(s.db, blockNumberToKey(blockNumber))
	if err != nil {
		return nil, err
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
			return nil, err
		}
		if n > maxBlkNum {
			expiryBlockNumbers = append(expiryBlockNumbers, pvtBlockNum)
		}
	}
	blockPvtData.ExpiryBlocks = expiryBlockNumbers

	jsonBytes, err := json.Marshal(blockPvtData)
	if err != nil {
		return nil, err
	}
	return &couchdb.CouchDoc{JSONValue: jsonBytes}, nil
}
