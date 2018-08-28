/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package pvtdatastorage

import (
	"fmt"
	"sync"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/core/ledger/pvtdatapolicy"
	"github.com/hyperledger/fabric/protos/ledger/rwset"
)

var logger = flogging.MustGetLogger("pvtdatastorage")

type provider struct {
	dbProvider *leveldbhelper.Provider
}

type store struct {
	db        *leveldbhelper.DBHandle
	ledgerid  string
	btlPolicy pvtdatapolicy.BTLPolicy

	isEmpty            bool
	lastCommittedBlock uint64
	batchPending       bool
	purgerLock         sync.Mutex
}

type blkTranNumKey []byte

type dataEntry struct {
	key   *dataKey
	value *rwset.CollectionPvtReadWriteSet
}

type expiryEntry struct {
	key   *expiryKey
	value *ExpiryData
}

type expiryKey struct {
	expiringBlk   uint64
	committingBlk uint64
}

type dataKey struct {
	blkNum   uint64
	txNum    uint64
	ns, coll string
}

//////// Provider functions  /////////////
//////////////////////////////////////////

// NewProvider instantiates a StoreProvider
func NewProvider() Provider {
	dbPath := ledgerconfig.GetPvtdataStorePath()
	dbProvider := leveldbhelper.NewProvider(&leveldbhelper.Conf{DBPath: dbPath})
	return &provider{dbProvider: dbProvider}
}

// OpenStore returns a handle to a store
func (p *provider) OpenStore(ledgerid string) (Store, error) {
	dbHandle := p.dbProvider.GetDBHandle(ledgerid)
	s := &store{db: dbHandle, ledgerid: ledgerid}
	if err := s.initState(); err != nil {
		return nil, err
	}
	logger.Debugf("Pvtdata store opened. Initial state: isEmpty [%t], lastCommittedBlock [%d], batchPending [%t]",
		s.isEmpty, s.lastCommittedBlock, s.batchPending)
	return s, nil
}

// Close closes the store
func (p *provider) Close() {
	p.dbProvider.Close()
}

//////// store functions  ////////////////
//////////////////////////////////////////

func (s *store) initState() error {
	var err error
	if s.isEmpty, s.lastCommittedBlock, err = s.getLastCommittedBlockNum(); err != nil {
		return err
	}
	if s.batchPending, err = s.hasPendingCommit(); err != nil {
		return err
	}
	return nil
}

func (s *store) Init(btlPolicy pvtdatapolicy.BTLPolicy) {
	s.btlPolicy = btlPolicy
}

// Prepare implements the function in the interface `Store`
func (s *store) Prepare(blockNum uint64, pvtData []*ledger.TxPvtData) error {
	if s.batchPending {
		return &ErrIllegalCall{`A pending batch exists as as result of last invoke to "Prepare" call.
			 Invoke "Commit" or "Rollback" on the pending batch before invoking "Prepare" function`}
	}
	expectedBlockNum := s.nextBlockNum()
	if expectedBlockNum != blockNum {
		return &ErrIllegalArgs{fmt.Sprintf("Expected block number=%d, recived block number=%d", expectedBlockNum, blockNum)}
	}

	batch := leveldbhelper.NewUpdateBatch()
	var err error
	var keyBytes, valBytes []byte
	dataEntries, expiryEntries, err := prepareStoreEntries(blockNum, pvtData, s.btlPolicy)
	if err != nil {
		return err
	}
	for _, dataEntry := range dataEntries {
		keyBytes = encodeDataKey(dataEntry.key)
		if valBytes, err = encodeDataValue(dataEntry.value); err != nil {
			return err
		}
		batch.Put(keyBytes, valBytes)
	}
	for _, expiryEntry := range expiryEntries {
		keyBytes = encodeExpiryKey(expiryEntry.key)
		if valBytes, err = encodeExpiryValue(expiryEntry.value); err != nil {
			return err
		}
		batch.Put(keyBytes, valBytes)
	}
	batch.Put(pendingCommitKey, emptyValue)
	if err := s.db.WriteBatch(batch, true); err != nil {
		return err
	}
	s.batchPending = true
	logger.Debugf("Saved %d private data write sets for block [%d]", len(pvtData), blockNum)
	return nil
}

// Commit implements the function in the interface `Store`
func (s *store) Commit() error {
	if !s.batchPending {
		return &ErrIllegalCall{"No pending batch to commit"}
	}
	committingBlockNum := s.nextBlockNum()
	logger.Debugf("Committing private data for block [%d]", committingBlockNum)
	batch := leveldbhelper.NewUpdateBatch()
	batch.Delete(pendingCommitKey)
	batch.Put(lastCommittedBlkkey, encodeLastCommittedBlockVal(committingBlockNum))
	if err := s.db.WriteBatch(batch, true); err != nil {
		return err
	}
	s.batchPending = false
	s.isEmpty = false
	s.lastCommittedBlock = committingBlockNum
	logger.Debugf("Committed private data for block [%d]", committingBlockNum)
	s.performPurgeIfScheduled(committingBlockNum)
	return nil
}

// Rollback implements the function in the interface `Store`
// Not deleting the existing data entries and expiry entries for now
// Because the next try would have exact same entries and will overwrite those
func (s *store) Rollback() error {
	if !s.batchPending {
		return &ErrIllegalCall{"No pending batch to rollback"}
	}
	s.batchPending = false
	return nil
}

// GetPvtDataByBlockNum implements the function in the interface `Store`.
// If the store is empty or the last committed block number is smaller then the
// requested block number, an 'ErrOutOfRange' is thrown
func (s *store) GetPvtDataByBlockNum(blockNum uint64, filter ledger.PvtNsCollFilter) ([]*ledger.TxPvtData, error) {
	logger.Debugf("Get private data for block [%d], filter=%#v", blockNum, filter)
	if s.isEmpty {
		return nil, &ErrOutOfRange{"The store is empty"}
	}
	if blockNum > s.lastCommittedBlock {
		return nil, &ErrOutOfRange{fmt.Sprintf("Last committed block=%d, block requested=%d", s.lastCommittedBlock, blockNum)}
	}
	startKey, endKey := getDataKeysForRangeScanByBlockNum(blockNum)
	logger.Debugf("Querying private data storage for write sets using startKey=%#v, endKey=%#v", startKey, endKey)
	itr := s.db.GetIterator(startKey, endKey)
	defer itr.Release()

	var blockPvtdata []*ledger.TxPvtData
	var currentTxNum uint64
	var currentTxWsetAssember *txPvtdataAssembler
	firstItr := true

	for itr.Next() {
		dataKeyBytes := itr.Key()
		if v11Format(dataKeyBytes) {
			return v11RetrievePvtdata(itr, filter)
		}
		dataValueBytes := itr.Value()
		dataKey := decodeDatakey(dataKeyBytes)
		expired, err := isExpired(dataKey, s.btlPolicy, s.lastCommittedBlock)
		if err != nil {
			return nil, err
		}
		if expired || !passesFilter(dataKey, filter) {
			continue
		}
		dataValue, err := decodeDataValue(dataValueBytes)
		if err != nil {
			return nil, err
		}

		if firstItr {
			currentTxNum = dataKey.txNum
			currentTxWsetAssember = newTxPvtdataAssembler(blockNum, currentTxNum)
			firstItr = false
		}

		if dataKey.txNum != currentTxNum {
			blockPvtdata = append(blockPvtdata, currentTxWsetAssember.getTxPvtdata())
			currentTxNum = dataKey.txNum
			currentTxWsetAssember = newTxPvtdataAssembler(blockNum, currentTxNum)
		}
		currentTxWsetAssember.add(dataKey.ns, dataValue)
	}
	if currentTxWsetAssember != nil {
		blockPvtdata = append(blockPvtdata, currentTxWsetAssember.getTxPvtdata())
	}
	return blockPvtdata, nil
}

// InitLastCommittedBlock implements the function in the interface `Store`
func (s *store) InitLastCommittedBlock(blockNum uint64) error {
	if !(s.isEmpty && !s.batchPending) {
		return &ErrIllegalCall{"The private data store is not empty. InitLastCommittedBlock() function call is not allowed"}
	}
	batch := leveldbhelper.NewUpdateBatch()
	batch.Put(lastCommittedBlkkey, encodeLastCommittedBlockVal(blockNum))
	if err := s.db.WriteBatch(batch, true); err != nil {
		return err
	}
	s.isEmpty = false
	s.lastCommittedBlock = blockNum
	logger.Debugf("InitLastCommittedBlock set to block [%d]", blockNum)
	return nil
}

func (s *store) performPurgeIfScheduled(latestCommittedBlk uint64) {
	if latestCommittedBlk%ledgerconfig.GetPvtdataStorePurgeInterval() != 0 {
		return
	}
	go func() {
		s.purgerLock.Lock()
		logger.Infof("Purger started: Purging expired private data till block number [%d]", latestCommittedBlk)
		defer s.purgerLock.Unlock()
		err := s.purgeExpiredData(0, latestCommittedBlk)
		if err != nil {
			logger.Warningf("Could not purge data from pvtdata store:%s", err)
		}
		logger.Info("Purger finished")
	}()
}

func (s *store) purgeExpiredData(minBlkNum, maxBlkNum uint64) error {
	batch := leveldbhelper.NewUpdateBatch()
	expiryEntries, err := s.retrieveExpiryEntries(minBlkNum, maxBlkNum)
	if err != nil {
		return nil
	}
	for _, expiryEntry := range expiryEntries {
		// this encoding could have been saved if the function retrieveExpiryEntries also returns the encoded expiry keys.
		// However, keeping it for better readability
		batch.Delete(encodeExpiryKey(expiryEntry.key))
		for _, dataKey := range deriveDataKeys(expiryEntry) {
			batch.Delete(encodeDataKey(dataKey))
		}
		s.db.WriteBatch(batch, false)
	}
	logger.Debugf("[%d] Entries purged from private data storage", len(expiryEntries))
	return nil
}

func (s *store) retrieveExpiryEntries(minBlkNum, maxBlkNum uint64) ([]*expiryEntry, error) {
	startKey, endKey := getExpiryKeysForRangeScan(minBlkNum, maxBlkNum)
	logger.Debugf("retrieveExpiryEntries(): startKey=%#v, endKey=%#v", startKey, endKey)
	itr := s.db.GetIterator(startKey, endKey)
	defer itr.Release()

	var expiryEntries []*expiryEntry
	for itr.Next() {
		expiryKeyBytes := itr.Key()
		expiryValueBytes := itr.Value()
		expiryKey := decodeExpiryKey(expiryKeyBytes)
		expiryValue, err := decodeExpiryValue(expiryValueBytes)
		if err != nil {
			return nil, err
		}
		expiryEntries = append(expiryEntries, &expiryEntry{key: expiryKey, value: expiryValue})
	}
	return expiryEntries, nil
}

// LastCommittedBlockHeight implements the function in the interface `Store`
func (s *store) LastCommittedBlockHeight() (uint64, error) {
	if s.isEmpty {
		return 0, nil
	}
	return s.lastCommittedBlock + 1, nil
}

// HasPendingBatch implements the function in the interface `Store`
func (s *store) HasPendingBatch() (bool, error) {
	return s.batchPending, nil
}

// IsEmpty implements the function in the interface `Store`
func (s *store) IsEmpty() (bool, error) {
	return s.isEmpty, nil
}

// Shutdown implements the function in the interface `Store`
func (s *store) Shutdown() {
	// do nothing
}

func (s *store) nextBlockNum() uint64 {
	if s.isEmpty {
		return 0
	}
	return s.lastCommittedBlock + 1
}

func (s *store) hasPendingCommit() (bool, error) {
	var v []byte
	var err error
	if v, err = s.db.Get(pendingCommitKey); err != nil {
		return false, err
	}
	return v != nil, nil
}

func (s *store) getLastCommittedBlockNum() (bool, uint64, error) {
	var v []byte
	var err error
	if v, err = s.db.Get(lastCommittedBlkkey); v == nil || err != nil {
		return true, 0, err
	}
	return false, decodeLastCommittedBlockVal(v), nil
}
