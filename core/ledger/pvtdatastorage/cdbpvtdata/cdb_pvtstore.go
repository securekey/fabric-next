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

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"

	"sync"

	"sort"

	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/willf/bitset"
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

func (s *store) prepareDB(blockNum uint64, pvtData []*ledger.TxPvtData, missingPvtData ledger.TxMissingPvtDataMap) error {
	storeEntries, err := prepareStoreEntries(blockNum, pvtData, s.btlPolicy, missingPvtData)
	if err != nil {
		return err
	}

	if len(storeEntries.dataEntries) > 0 || len(storeEntries.missingDataEntries) > 0 || len(storeEntries.expiryEntries) > 0 {
		blockDoc, err := createBlockCouchDoc(storeEntries, blockNum, s.purgeInterval)
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

func (s *store) commitPvtDataOfOldBlocksDB(blocksPvtData map[uint64][]*ledger.TxPvtData) error {
	docs := make([]*couchdb.CouchDoc, 0)

	// construct dataEntries for all pvtData
	for blkNum, pvtData := range blocksPvtData {
		// construct update entries (i.e., dataEntries, expiryEntries, missingDataEntries) from the above created data entries
		updateEntries, err := s.constructUpdateEntriesFromDataEntries(prepareDataEntries(blkNum, pvtData))
		if err != nil {
			return err
		}
		// create a db update batch from the update entries
		logger.Debug("Constructing update batch from pvtdatastore entries")
		if len(updateEntries.dataEntries) > 0 || len(updateEntries.missingDataEntries) > 0 || len(updateEntries.expiryEntries) > 0 {
			var dataEntries []*dataEntry
			for k, v := range updateEntries.dataEntries {
				dataEntries = append(dataEntries, &dataEntry{key: &k, value: v})
			}

			missingDataEntries := make(map[missingDataKey]*bitset.BitSet)
			for k, v := range updateEntries.missingDataEntries {
				missingDataEntries[missingDataKey{nsCollBlk: k, isEligible: true}] = v
			}

			var expiryEntries []*expiryEntry
			for k, v := range updateEntries.expiryEntries {
				expiryEntries = append(expiryEntries, &expiryEntry{key: &k, value: v})
			}

			blockDoc, err := createBlockCouchDoc(&storeEntries{
				dataEntries:        dataEntries,
				expiryEntries:      expiryEntries,
				missingDataEntries: missingDataEntries}, blkNum, s.purgeInterval)
			if err != nil {
				return err
			}

			if blockDoc != nil {
				docs = append(docs, blockDoc)
			}
		}
	}
	// (4) add lastUpdatedOldBlocksList to the batch
	addLastUpdatedOldBlocksList(updateEntries)

	s.db.BatchUpdateDocuments(docs)
	return nil
}

func addLastUpdatedOldBlocksList(entries *entriesForPvtDataOfOldBlocks) {
	// create a list of blocks' pvtData which are being stored. If this list is
	// found during the recovery, the stateDB may not be in sync with the pvtData
	// and needs recovery. In a normal flow, once the stateDB is synced, the
	// block list would be deleted.
	updatedBlksListMap := make(map[uint64]bool)

	for dataKey := range entries.dataEntries {
		updatedBlksListMap[dataKey.blkNum] = true
	}

	var updatedBlksList lastUpdatedOldBlocksList
	for blkNum := range updatedBlksListMap {
		updatedBlksList = append(updatedBlksList, blkNum)
	}

	// better to store as sorted list
	sort.SliceStable(updatedBlksList, func(i, j int) bool {
		return updatedBlksList[i] < updatedBlksList[j]
	})

	buf := proto.NewBuffer(nil)
	buf.EncodeVarint(uint64(len(updatedBlksList)))
	for _, blkNum := range updatedBlksList {
		buf.EncodeVarint(blkNum)
	}

	batch.Put(lastUpdatedOldBlocksKey, buf.Bytes())
}

func (s *store) getPvtDataByBlockNumDB(blockNum uint64) (map[string][]byte, error) {
	pd, err := retrieveBlockPvtData(s.db, blockNumberToKey(blockNum))
	if err != nil {
		return nil, err
	}

	return pd.Data, nil
}

func (s *store) getRangeMissingPvtDataByMaxBlockNumDB(startBlockNum, endBlockNum uint64) (map[string][]byte, error) {
	pds, err := retrieveRangeBlockPvtData(s.db, blockNumberToKey(startBlockNum), blockNumberToKey(endBlockNum))
	if err != nil {
		return nil, err
	}

	missingPvtData := make(map[string][]byte)
	for _, pd := range pds {
		for k, v := range pd.Expiry {
			missingPvtData[k] = v
		}
	}

	return missingPvtData, nil
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
		dataKeys, missingDataKeys := deriveKeys(expiryKey)
		allPurged := true
		for _, dataKey := range dataKeys {
			keyBytesStr := hex.EncodeToString(encodeDataKey(dataKey))
			//TODO purge not set
			if dataKey.purge {
				logger.Debugf("purge: deleting data key[%s] for expiry entry[%s]", keyBytesStr, expiryBytesStr)
				delete(blockPvtData.Data, keyBytesStr)
			} else {
				logger.Debugf("purge: skipping data key[%s] for expiry entry[%s]", keyBytesStr, expiryBytesStr)
				allPurged = false
			}
		}
		for _, missingDataKey := range missingDataKeys {
			missingDatakeyBytesStr := hex.EncodeToString(encodeMissingDataKey(missingDataKey))
			//TODO purge not set
			if missingDataKey.purge {
				logger.Debugf("purge: deleting missing data key[%s] for expiry entry[%s]", missingDatakeyBytesStr, expiryBytesStr)
				delete(blockPvtData.MissingData, missingDatakeyBytesStr)
			} else {
				logger.Debugf("purge: skipping missing data key[%s] for expiry entry[%s]", missingDatakeyBytesStr, expiryBytesStr)
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
