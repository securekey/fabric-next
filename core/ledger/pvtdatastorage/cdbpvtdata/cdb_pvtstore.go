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

	"github.com/pkg/errors"

	"sync"

	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
)

type store struct {
	db             *couchdb.CouchDatabase
	purgeInterval  uint64
	pendingPvtDocs map[uint64][]*couchdb.CouchDoc
	pendingPvtMtx  sync.Mutex
	commonStore
}

func newStore(db *couchdb.CouchDatabase) (*store, error) {
	s := store{
		db:             db,
		purgeInterval:  ledgerconfig.GetPvtdataStorePurgeInterval(),
		pendingPvtDocs: make(map[uint64][]*couchdb.CouchDoc),
	}
	s.isEmpty = true

	return &s, nil
}

func (s *store) prepareDB(blockNum uint64, pvtData []*ledger.TxPvtData) error {
	dataEntries, expiryEntries, err := prepareStoreEntries(blockNum, pvtData, s.btlPolicy)
	if err != nil {
		return err
	}

	if len(dataEntries) > 0 || len(expiryEntries) > 0 {
		blockDoc, err := createBlockCouchDoc(dataEntries, expiryEntries, blockNum, s.purgeInterval)
		if err != nil {
			return err
		}

		if blockDoc != nil {
			pendingDocs := make([]*couchdb.CouchDoc, 0)
			pendingDocs = append(pendingDocs, blockDoc)
			s.pushPendingDoc(blockNum, pendingDocs)
		}
	}

	return nil
}

func (s *store) commitDB(blockNum uint64) error {
	if !s.checkPendingPvt(blockNum) {
		return errors.New("no commit is pending")
	}
	pendingDocs, err := s.popPendingPvt(blockNum)
	if err != nil {
		return err
	}
	_, err = s.db.CommitDocuments(pendingDocs)
	if err != nil {
		return errors.WithMessage(err, fmt.Sprintf("writing private data to CouchDB failed [%d]", blockNum))
	}

	return nil
}

func (s *store) getPvtDataByBlockNumDB(blockNum uint64) (map[string][]byte, error) {
	pd, err := retrieveBlockPvtData(s.db, blockNumberToKey(blockNum))
	if err != nil {
		return nil, err
	}

	return pd.Data, nil
}

func (s *store) getExpiryEntriesDB(blockNum uint64) (map[string][]byte, error) {
	pds, err := retrieveBlockExpiryData(s.db, blockNumberToPurgeBlockKey(blockNum))
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

	logger.Debugf("purge: processing [%d] expiry entries for block [%d]", len(expiryEntries), blockNumber)
	for _, expiryKey := range expiryEntries {
		expiryBytesStr := hex.EncodeToString(encodeExpiryKey(expiryKey.key))

		dataKeys := deriveDataKeys(expiryKey)
		allPurged := true
		for _, dataKey := range dataKeys {
			keyBytesStr := hex.EncodeToString(encodeDataKey(dataKey))
			if dataKey.purge {
				logger.Debugf("purge: deleting data key[%s] for expiry entry[%s]", keyBytesStr, expiryBytesStr)
				delete(blockPvtData.Data, keyBytesStr)
			} else {
				logger.Debugf("purge: skipping data key[%s] for expiry entry[%s]", keyBytesStr, expiryBytesStr)
				allPurged = false
			}
		}

		if allPurged {
			delete(blockPvtData.Expiry, expiryBytesStr)
			logger.Debugf("purge: deleted expiry key [%s]", expiryBytesStr)
		}
	}

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

	purgeBlockNumber, err := getPurgeBlockNumber(expiryBlockNumbers, s.purgeInterval)
	if err != nil {
		return nil, err
	}
	blockPvtData.PurgeBlock = purgeBlockNumber

	logger.Debugf("Setting next purge block[%s] for block[%d], max block[%d]", purgeBlockNumber, blockNumber, maxBlkNum)

	if len(blockPvtData.Data) == 0 {
		blockPvtData.Deleted = true
	}

	jsonBytes, err := json.Marshal(blockPvtData)
	if err != nil {
		return nil, err
	}
	return &couchdb.CouchDoc{JSONValue: jsonBytes}, nil
}

func (s *store) pushPendingDoc(blockNumber uint64, docs []*couchdb.CouchDoc) {
	s.pendingPvtMtx.Lock()
	defer s.pendingPvtMtx.Unlock()

	s.pendingPvtDocs[blockNumber] = docs
}

func (s *store) popPendingPvt(blockNumber uint64) ([]*couchdb.CouchDoc, error) {
	s.pendingPvtMtx.Lock()
	defer s.pendingPvtMtx.Unlock()

	docs, ok := s.pendingPvtDocs[blockNumber]
	if !ok {
		return nil, errors.Errorf("pvt was not prepared [%d]", blockNumber)
	}
	delete(s.pendingPvtDocs, blockNumber)
	return docs, nil
}

func (s *store) checkPendingPvt(blockNumber uint64) bool {
	s.pendingPvtMtx.Lock()
	defer s.pendingPvtMtx.Unlock()

	_, ok := s.pendingPvtDocs[blockNumber]
	return ok
}

func (s *store) pendingPvtSize() int {
	s.pendingPvtMtx.Lock()
	defer s.pendingPvtMtx.Unlock()
	return len(s.pendingPvtDocs)
}
