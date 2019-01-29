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

	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/willf/bitset"
)

type store struct {
	db                 *couchdb.CouchDatabase
	purgeInterval      uint64
	pendingPvtData     map[uint64]*pendingData
	pendingPvtMtx      sync.Mutex
	missingDataIndexDB *leveldbhelper.DBHandle
	commonStore
	collElgProcSync *collElgProcSync
}

type pendingData struct {
	docs               []*couchdb.CouchDoc
	missingDataEntries map[missingDataKey]*bitset.BitSet
}

func newStore(db *couchdb.CouchDatabase, missingDataIndexDB *leveldbhelper.DBHandle) (*store, error) {
	s := store{
		db:                 db,
		purgeInterval:      ledgerconfig.GetPvtdataStorePurgeInterval(),
		pendingPvtData:     make(map[uint64]*pendingData),
		missingDataIndexDB: missingDataIndexDB,
		collElgProcSync: &collElgProcSync{
			notification: make(chan bool, 1),
			procComplete: make(chan bool, 1),
		},
	}

	s.launchCollElgProc()

	s.isEmpty = true

	return &s, nil
}

func (s *store) prepareDB(blockNum uint64, pvtData []*ledger.TxPvtData, missingPvtData ledger.TxMissingPvtDataMap) error {
	storeEntries, err := prepareStoreEntries(blockNum, pvtData, s.btlPolicy, missingPvtData)
	if err != nil {
		return err
	}

	if len(storeEntries.dataEntries) > 0 || len(storeEntries.expiryEntries) > 0 {
		blockDoc, err := createBlockCouchDoc(storeEntries.dataEntries, storeEntries.expiryEntries, blockNum, s.purgeInterval)
		if err != nil {
			return err
		}

		if blockDoc != nil {
			pendingDocs := make([]*couchdb.CouchDoc, 0)
			pendingDocs = append(pendingDocs, blockDoc)
			s.pushPendingPvtData(blockNum, &pendingData{docs: pendingDocs, missingDataEntries: storeEntries.missingDataEntries})
		}
	}

	return nil
}

func (s *store) commitDB(blockNum uint64) error {
	if !s.checkPendingPvtData(blockNum) {
		return errors.New("no commit is pending")
	}
	pendingData, err := s.popPendingPvtData(blockNum)
	if err != nil {
		return err
	}
	_, err = s.db.CommitDocuments(pendingData.docs)
	if err != nil {
		return errors.WithMessage(err, fmt.Sprintf("writing private data to CouchDB failed [%d]", blockNum))
	}

	batch := leveldbhelper.NewUpdateBatch()
	for missingDataKey, missingDataValue := range pendingData.missingDataEntries {
		keyBytes := encodeMissingDataKey(&missingDataKey)
		valBytes, err := encodeMissingDataValue(missingDataValue)
		if err != nil {
			return err
		}
		batch.Put(keyBytes, valBytes)
	}
	if err := s.missingDataIndexDB.WriteBatch(batch, true); err != nil {
		return err
	}

	return nil
}

func (s *store) commitPvtDataOfOldBlocksDB(blocksPvtData map[uint64][]*ledger.TxPvtData) error {
	docs := make([]*couchdb.CouchDoc, 0)

	// create a list of blocks' pvtData which are being stored. If this list is
	// found during the recovery, the stateDB may not be in sync with the pvtData
	// and needs recovery. In a normal flow, once the stateDB is synced, the
	// block list would be deleted.
	updatedBlksListMap := make(map[uint64]bool)
	batch := leveldbhelper.NewUpdateBatch()

	// construct dataEntries for all pvtData
	for blkNum, pvtData := range blocksPvtData {

		updatedBlksListMap[blkNum] = true

		// construct update entries (i.e., dataEntries, expiryEntries, missingDataEntries) from the above created data entries
		updateEntries, err := s.constructUpdateEntriesFromDataEntries(prepareDataEntries(blkNum, pvtData))
		if err != nil {
			return err
		}

		s.addUpdatedMissingDataEntriesToUpdateBatch(batch, updateEntries)

		if len(updateEntries.dataEntries) > 0 || len(updateEntries.missingDataEntries) > 0 || len(updateEntries.expiryEntries) > 0 {
			var dataEntries []*dataEntry
			for k, v := range updateEntries.dataEntries {
				dataEntries = append(dataEntries, &dataEntry{key: &k, value: v})
			}
			var expiryEntries []*expiryEntry
			for k, v := range updateEntries.expiryEntries {
				expiryEntries = append(expiryEntries, &expiryEntry{key: &k, value: v})
			}

			blockDoc, err := createBlockCouchDoc(dataEntries, expiryEntries, blkNum, s.purgeInterval)
			if err != nil {
				return err
			}

			if blockDoc != nil {
				docs = append(docs, blockDoc)
			}
		}
	}

	var updatedBlksList lastUpdatedOldBlocksList
	for blkNum := range updatedBlksListMap {
		updatedBlksList = append(updatedBlksList, blkNum)
	}
	sort.SliceStable(updatedBlksList, func(i, j int) bool {
		return updatedBlksList[i] < updatedBlksList[j]
	})
	doc, err := createLastUpdatedOldBlocksDoc(updatedBlksList)
	if err != nil {
		return err
	}
	if doc != nil {
		docs = append(docs, doc)
	}
	if _, err := s.db.BatchUpdateDocuments(docs); err != nil {
		return nil
	}

	if err := s.missingDataIndexDB.WriteBatch(batch, true); err != nil {
		return err
	}

	return nil
}

func (s *store) addUpdatedMissingDataEntriesToUpdateBatch(batch *leveldbhelper.UpdateBatch, entries *entriesForPvtDataOfOldBlocks) error {
	var keyBytes, valBytes []byte
	var err error
	for nsCollBlk, missingData := range entries.missingDataEntries {
		keyBytes = encodeMissingDataKey(&missingDataKey{nsCollBlk, true})
		// if the missingData is empty, we need to delete the missingDataKey
		if missingData.None() {
			batch.Delete(keyBytes)
			continue
		}
		if valBytes, err = encodeMissingDataValue(missingData); err != nil {
			return err
		}
		batch.Put(keyBytes, valBytes)
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

func (s *store) deleteLastUpdatedOldBlocksList() error {
	return s.db.DeleteDoc(lastUpdatedOldBlocksKey, "")
}

func (s *store) getLastUpdatedOldBlocksList() ([]uint64, error) {
	var v []byte
	var err error

	if v, err = retrieveLastUpdatedOldBlocksDoc(s.db); err != nil {
		return nil, err
	}
	if v == nil {
		return nil, nil
	}

	var updatedBlksList []uint64
	buf := proto.NewBuffer(v)
	numBlks, err := buf.DecodeVarint()
	if err != nil {
		return nil, err
	}
	for i := 0; i < int(numBlks); i++ {
		blkNum, err := buf.DecodeVarint()
		if err != nil {
			return nil, err
		}
		updatedBlksList = append(updatedBlksList, blkNum)
	}
	return updatedBlksList, nil
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
	batch := leveldbhelper.NewUpdateBatch()
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

		if allPurged {
			delete(blockPvtData.Expiry, expiryBytesStr)
			logger.Debugf("purge: deleted expiry key [%s]", expiryBytesStr)
		}
		for _, missingDataKey := range missingDataKeys {
			batch.Delete(encodeMissingDataKey(missingDataKey))
		}
		s.missingDataIndexDB.WriteBatch(batch, false)
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

func (s *store) pushPendingPvtData(blockNumber uint64, data *pendingData) {
	s.pendingPvtMtx.Lock()
	defer s.pendingPvtMtx.Unlock()

	s.pendingPvtData[blockNumber] = data
}

func (s *store) popPendingPvtData(blockNumber uint64) (*pendingData, error) {
	s.pendingPvtMtx.Lock()
	defer s.pendingPvtMtx.Unlock()

	pendingData, ok := s.pendingPvtData[blockNumber]
	if !ok {
		return nil, errors.Errorf("pvt was not prepared [%d]", blockNumber)
	}
	delete(s.pendingPvtData, blockNumber)
	return pendingData, nil
}

func (s *store) checkPendingPvtData(blockNumber uint64) bool {
	s.pendingPvtMtx.Lock()
	defer s.pendingPvtMtx.Unlock()

	_, ok := s.pendingPvtData[blockNumber]
	return ok
}

func (s *store) pendingPvtSize() int {
	s.pendingPvtMtx.Lock()
	defer s.pendingPvtMtx.Unlock()
	return len(s.pendingPvtData)
}
