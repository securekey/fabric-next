/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package memtransientdata

import (
	"encoding/base64"
	"encoding/hex"

	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/transientstore"
	"github.com/hyperledger/fabric/protos/ledger/rwset"
	"github.com/hyperledger/fabric/protos/peer"
	pb "github.com/hyperledger/fabric/protos/transientstore"
	"github.com/pkg/errors"
)

type store struct {
	cache            map[string]map[string]string
	blockHeightCache map[uint64][]string
	sync.RWMutex
}

func newStore() (*store, error) {
	s := store{
		cache:            make(map[string]map[string]string),
		blockHeightCache: make(map[uint64][]string),
	}
	return &s, nil
}

func (s *store) persistDB(txid string, blockHeight uint64, privateSimulationResults *rwset.TxPvtReadWriteSet) error {
	uuid := util.GenerateUUID()
	compositeKeyPvtRWSet := createCompositeKeyForPvtRWSet(txid, uuid, blockHeight)
	privateSimulationResultsBytes, err := proto.Marshal(privateSimulationResults)
	if err != nil {
		return err
	}
	s.Lock()
	defer s.Unlock()
	value, exist := s.cache[txid]
	blockHeightValue, blockHeightExist := s.blockHeightCache[blockHeight]
	if !exist {
		value = make(map[string]string)
	}
	if !blockHeightExist {
		blockHeightValue = make([]string, 0)
	}
	value[hex.EncodeToString(compositeKeyPvtRWSet)] = base64.StdEncoding.EncodeToString(privateSimulationResultsBytes)
	s.cache[txid] = value
	blockHeightValue = append(blockHeightValue, txid)
	s.blockHeightCache[blockHeight] = blockHeightValue

	return nil
}

func (s *store) persistWithConfigDB(txid string, blockHeight uint64, privateSimulationResultsWithConfig *pb.TxPvtReadWriteSetWithConfigInfo) error {
	uuid := util.GenerateUUID()
	compositeKeyPvtRWSet := createCompositeKeyForPvtRWSet(txid, uuid, blockHeight)
	privateSimulationResultsWithConfigBytes, err := proto.Marshal(privateSimulationResultsWithConfig)
	if err != nil {
		return err
	}

	s.Lock()
	defer s.Unlock()
	value, exist := s.cache[txid]
	blockHeightValue, blockHeightExist := s.blockHeightCache[blockHeight]
	if !exist {
		value = make(map[string]string)
	}
	if !blockHeightExist {
		blockHeightValue = make([]string, 0)
	}
	value[hex.EncodeToString(compositeKeyPvtRWSet)] = base64.StdEncoding.EncodeToString(privateSimulationResultsWithConfigBytes)
	s.cache[txid] = value
	blockHeightValue = append(blockHeightValue, txid)
	s.blockHeightCache[blockHeight] = blockHeightValue
	return nil
}

func (s *store) getTxPvtRWSetByTxidDB(txid string, filter ledger.PvtNsCollFilter, endorsers []*peer.Endorsement) (transientstore.RWSetScanner, error) {
	s.RLock()
	value, _ := s.cache[txid]
	s.RUnlock()
	return &RwsetScanner{filter: filter, results: value}, nil

}

func (s *store) getMinTransientBlkHtDB() (uint64, error) {
	s.RLock()
	var minTransientBlkHt uint64
	for key := range s.blockHeightCache {
		if minTransientBlkHt == 0 || key < minTransientBlkHt {
			minTransientBlkHt = key
		}
	}
	s.RUnlock()
	return minTransientBlkHt, nil
}

func (s *store) purgeByTxidsDB(txids []string) error {
	s.Lock()
	defer s.Unlock()
	for _, txID := range txids {
		delete(s.cache, txID)
	}
	return nil
}

func (s *store) purgeByHeightDB(maxBlockNumToRetain uint64) error {
	txIDs := make([]string, 0)
	s.Lock()
	for key, value := range s.blockHeightCache {
		if key < maxBlockNumToRetain {
			for _, txID := range value {
				txIDs = append(txIDs, txID)
			}
			delete(s.blockHeightCache, key)
		}
	}
	s.Unlock()
	return s.purgeByTxidsDB(txIDs)

}

type RwsetScanner struct {
	filter  ledger.PvtNsCollFilter
	results map[string]string
}

// Next moves the iterator to the next key/value pair.
// It returns whether the iterator is exhausted.
// TODO: Once the related gossip changes are made as per FAB-5096, remove this function
func (scanner *RwsetScanner) Next() (*transientstore.EndorserPvtSimulationResults, error) {
	for key, value := range scanner.results {
		delete(scanner.results, key)
		keyBytes, err := hex.DecodeString(key)
		if err != nil {
			return nil, err
		}
		_, blockHeight := splitCompositeKeyOfPvtRWSet(keyBytes)
		logger.Debugf("scanner next blockHeight %d", blockHeight)
		txPvtRWSet := &rwset.TxPvtReadWriteSet{}
		valueBytes, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			return nil, errors.Wrapf(err, "error from DecodeString for transientDataField")
		}

		if err := proto.Unmarshal(valueBytes, txPvtRWSet); err != nil {
			return nil, err
		}
		filteredTxPvtRWSet := trimPvtWSet(txPvtRWSet, scanner.filter)
		logger.Debugf("scanner next filteredTxPvtRWSet %v", filteredTxPvtRWSet)

		return &transientstore.EndorserPvtSimulationResults{
			ReceivedAtBlockHeight: blockHeight,
			PvtSimulationResults:  filteredTxPvtRWSet,
		}, nil
	}
	return nil, nil

}

// Next moves the iterator to the next key/value pair.
// It returns whether the iterator is exhausted.
// TODO: Once the related gossip changes are made as per FAB-5096, rename this function to Next
func (scanner *RwsetScanner) NextWithConfig() (*transientstore.EndorserPvtSimulationResultsWithConfig, error) {
	for key, value := range scanner.results {
		delete(scanner.results, key)
		keyBytes, err := hex.DecodeString(key)
		if err != nil {
			return nil, err
		}
		_, blockHeight := splitCompositeKeyOfPvtRWSet(keyBytes)
		logger.Debugf("scanner NextWithConfig blockHeight %d", blockHeight)
		txPvtRWSet := &rwset.TxPvtReadWriteSet{}
		valueBytes, err := base64.StdEncoding.DecodeString(value)
		if err != nil {
			return nil, errors.Wrapf(err, "error from DecodeString for transientDataField")
		}

		if err := proto.Unmarshal(valueBytes, txPvtRWSet); err != nil {
			return nil, err
		}

		filteredTxPvtRWSet := &rwset.TxPvtReadWriteSet{}
		txPvtRWSetWithConfig := &pb.TxPvtReadWriteSetWithConfigInfo{}

		// new proto, i.e., TxPvtReadWriteSetWithConfigInfo
		if err := proto.Unmarshal(valueBytes, txPvtRWSetWithConfig); err != nil {
			return nil, err
		}

		logger.Debugf("scanner NextWithConfig txPvtRWSetWithConfig %v", txPvtRWSetWithConfig)

		filteredTxPvtRWSet = trimPvtWSet(txPvtRWSetWithConfig.GetPvtRwset(), scanner.filter)
		logger.Debugf("scanner NextWithConfig filteredTxPvtRWSet %v", filteredTxPvtRWSet)
		configs, err := trimPvtCollectionConfigs(txPvtRWSetWithConfig.CollectionConfigs, scanner.filter)
		if err != nil {
			return nil, err
		}
		logger.Debugf("scanner NextWithConfig configs %v", configs)
		txPvtRWSetWithConfig.CollectionConfigs = configs

		txPvtRWSetWithConfig.PvtRwset = filteredTxPvtRWSet

		return &transientstore.EndorserPvtSimulationResultsWithConfig{
			ReceivedAtBlockHeight:          blockHeight,
			PvtSimulationResultsWithConfig: txPvtRWSetWithConfig,
		}, nil
	}
	return nil, nil
}

// Close releases resource held by the iterator
func (scanner *RwsetScanner) Close() {

}
