/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbtransientdata

import (
	"encoding/hex"
	"fmt"
	"strconv"

	"bytes"
	"encoding/base64"
	"encoding/json"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/metrics"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/hyperledger/fabric/core/transientstore"
	"github.com/hyperledger/fabric/protos/ledger/rwset"
	"github.com/hyperledger/fabric/protos/peer"
	pb "github.com/hyperledger/fabric/protos/transientstore"
	"github.com/pkg/errors"
)

const queryByTxIDFmt = `
	{
		"selector": {
			"` + txIDField + `": {
				"$eq": "%s"
			}
		},
		"use_index": ["_design/` + txIDIndexDoc + `", "` + txIDIndexName + `"]
	}`

type store struct {
	db          *couchdb.CouchDatabase
	endorserDBs map[string]*couchdb.CouchDatabase
	ledgerID    string
}

func newStore(db *couchdb.CouchDatabase, ledgerID string) (*store, error) {
	s := store{
		db:          db,
		endorserDBs: make(map[string]*couchdb.CouchDatabase),
		ledgerID:    ledgerID,
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
	indices := map[string]string{blockNumberField: fmt.Sprintf("%064s", strconv.FormatUint(blockHeight, blockNumberBase)), txIDField: txid}

	doc, err := keyValueToCouchDoc(compositeKeyPvtRWSet, privateSimulationResultsBytes, indices)
	if err != nil {
		return err
	}
	logger.Debugf("persistDB save doc key %s in local db %s", hex.EncodeToString(compositeKeyPvtRWSet), s.db.DBName)

	_, err = s.db.SaveDoc(hex.EncodeToString(compositeKeyPvtRWSet), "", doc)
	if err != nil {
		return errors.WithMessage(err, "SaveDoc failed for persist transient store")
	}

	return nil
}
func (s *store) persistWithConfigDB(txid string, blockHeight uint64, privateSimulationResultsWithConfig *pb.TxPvtReadWriteSetWithConfigInfo) error {
	uuid := util.GenerateUUID()
	compositeKeyPvtRWSet := createCompositeKeyForPvtRWSet(txid, uuid, blockHeight)
	privateSimulationResultsWithConfigBytes, err := proto.Marshal(privateSimulationResultsWithConfig)
	if err != nil {
		return err
	}

	indices := map[string]string{blockNumberField: fmt.Sprintf("%064s", strconv.FormatUint(blockHeight, blockNumberBase)), txIDField: txid}

	doc, err := keyValueToCouchDoc(compositeKeyPvtRWSet, privateSimulationResultsWithConfigBytes, indices)
	if err != nil {
		return err
	}
	logger.Errorf("Persisting private data to transient store for txid [%s] at block height [%d] key %s", txid, blockHeight, compositeKeyPvtRWSet)

	_, err = s.db.SaveDoc(hex.EncodeToString(compositeKeyPvtRWSet), "", doc)
	if err != nil {
		return errors.WithMessage(err, "SaveDoc failed for persist transient store")
	}

	return nil
}

func (s *store) getTxPvtRWSetByTxidDB(txid string, filter ledger.PvtNsCollFilter, endorsers []*peer.Endorsement) (transientstore.RWSetScanner, error) {
	var results []*couchdb.QueryResult
	var err error
	logger.Errorf("getTxPvtRWSetByTxidDB txID %s from local peer db %s", txid, s.db.DBName)
	results, err = s.db.QueryDocuments(fmt.Sprintf(queryByTxIDFmt, txid))
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		logger.Debugf("getTxPvtRWSetByTxidDB txID %s didn't find pvt rwset in local db %s", txid, s.db.DBName)
		logger.Warningf("getTxPvtRWSetByTxidDB didn't find pvt rwset in local db %s", s.db.DBName)
	} else {
		logger.Debugf("getTxPvtRWSetByTxidDB txID %s find pvt rwset in local db %s", txid, s.db.DBName)
		return &RwsetScanner{txid: txid, filter: filter, results: results}, nil
	}
	for _, endorser := range endorsers {
		hash, err := hashPeerIdentity(endorser.Endorser)
		if err != nil {
			return nil, err
		}
		logger.Debugf("getTxPvtRWSetByTxidDB txID %s from endorser peer db %s", txid, fmt.Sprintf(transientDataStoreName, hash))
		db, err := getCouchDB(s.db.CouchInstance, couchdb.ConstructBlockchainDBName(s.ledgerID, fmt.Sprintf(transientDataStoreName, hash)))
		if err != nil {
			return nil, err
		}
		results, err = db.QueryDocuments(fmt.Sprintf(queryByTxIDFmt, txid))
		if err != nil {
			return nil, err
		}

		if len(results) == 0 {
			logger.Debugf("getTxPvtRWSetByTxidDB txID %s didn't find pvt rwset in endorser db %s", txid, fmt.Sprintf(transientDataStoreName, hash))
			logger.Warningf("getTxPvtRWSetByTxidDB didn't find pvt rwset in endorser db %s", fmt.Sprintf(transientDataStoreName, hash))
		} else {
			logger.Debugf("getTxPvtRWSetByTxidDB txID %s find pvt rwset in endorser db %s", txid, fmt.Sprintf(transientDataStoreName, hash))
			return &RwsetScanner{txid: txid, filter: filter, results: results}, nil
		}
	}
	logger.Debugf("getTxPvtRWSetByTxidDB didn't find pvt rwset in endorser dbs")
	return nil, errors.New("getTxPvtRWSetByTxidDB didn't find pvt rwset in endorser dbs")

}

func (s *store) getMinTransientBlkHtDB() (uint64, error) {
	const queryFmt = `{
   "selector":{
      "` + blockNumberField + `":{
         "$gt":"%s"
      }
   },
   "limit":1,
   "sort":[
      {
         "` + blockNumberField + `":"asc"
      }
   ],
   "use_index":[
      "_design/` + blockNumberIndexDoc + `", "` + blockNumberIndexName + `"
   ]
}`

	logger.Debugf("getMinTransientBlkHtDB from local peer db %s", s.db.DBName)
	results, err := s.db.QueryDocuments(fmt.Sprintf(queryFmt, fmt.Sprintf("%064s", strconv.FormatUint(0, blockNumberBase))))
	if err != nil {
		return 0, err
	}
	if len(results) == 0 {
		logger.Debugf("getMinTransientBlkHtDB didn't find pvt rwset in local db %s", s.db.DBName)
		return 0, transientstore.ErrStoreEmpty
	}
	dbKey, err := hex.DecodeString(results[0].ID)
	if err != nil {
		return 0, err
	}
	_, blockHeight := splitCompositeKeyOfPvtRWSet(dbKey)
	logger.Debugf("getMinTransientBlkHtDB return blockHeight %d in local db %s", blockHeight, s.db.DBName)
	return blockHeight, nil
}

func (s *store) purgeByTxidsDB(txids []string) error {
	logger.Errorf("Purging private data from transient store for committed txids %s", txids)

	for _, txID := range txids {
		queryResponse, err := s.db.Query(fmt.Sprintf(queryByTxIDFmt, txID))
		if err != nil {
			return errors.Wrap(err, "purgeByTxidsDB failed")
		}
		if len(queryResponse.Docs) == 0 {
			return nil
		}

		batchUpdateDocs, err := asBatchDeleteCouchDocs(queryResponse.Docs)
		if err != nil {
			return errors.Wrap(err, "purgeByTxidsDB failed")
		}

		// Do the bulk update into couchdb. Note that this will do retries if the entire bulk update fails or times out
		_, err = s.db.CommitDocuments(batchUpdateDocs)
		if err != nil {
			return err
		}

	}
	return nil
}

func (s *store) purgeByHeightDB(maxBlockNumToRetain uint64) error {
	logger.Errorf("Purging orphaned private data from transient store received prior to block [%d]", maxBlockNumToRetain)

	const queryFmt = `{
		"selector": {
			"` + blockNumberField + `": {
				"$lt": "%s"
			}
		},
		"use_index": ["_design/` + blockNumberIndexDoc + `", "` + blockNumberIndexName + `"]
	}`
	queryResponse, err := s.db.Query(fmt.Sprintf(queryFmt, fmt.Sprintf("%064s", strconv.FormatUint(maxBlockNumToRetain, blockNumberBase))))
	if err != nil {
		return errors.Wrap(err, "purgeByHeightDB failed")
	}

	if len(queryResponse.Docs) == 0 {
		return nil
	}

	batchUpdateDocs, err := asBatchDeleteCouchDocs(queryResponse.Docs)
	if err != nil {
		return errors.Wrap(err, "purgeByHeightDB failed")
	}

	logger.Debugf("Purging %d transient private data entries", len(queryResponse.Docs))
	// Do the bulk update into couchdb. Note that this will do retries if the entire bulk update fails or times out
	_, err = s.db.CommitDocuments(batchUpdateDocs)
	if err != nil {
		return err
	}

	return nil
}

type RwsetScanner struct {
	txid    string
	filter  ledger.PvtNsCollFilter
	results []*couchdb.QueryResult
}

// Next moves the iterator to the next key/value pair.
// It returns whether the iterator is exhausted.
// TODO: Once the related gossip changes are made as per FAB-5096, remove this function
func (scanner *RwsetScanner) Next() (*transientstore.EndorserPvtSimulationResults, error) {
	if len(scanner.results) < 1 {
		logger.Debugf("scanner next is empty return")
		return nil, nil
	}
	defer func() { scanner.results = append(scanner.results[:0], scanner.results[1:]...) }()

	/*stopWatch := metrics.StopWatch("cdbtransientdata_couchdb_next_duration")
	defer stopWatch()*/

	dbKey, err := hex.DecodeString(scanner.results[0].ID)
	if err != nil {
		return nil, err
	}

	logger.Debugf("scanner next key value %s", scanner.results[0].Value)

	jsonResult := make(map[string]interface{})
	decoder := json.NewDecoder(bytes.NewBuffer(scanner.results[0].Value))
	decoder.UseNumber()

	err = decoder.Decode(&jsonResult)
	if err != nil {
		return nil, errors.Wrapf(err, "result from DB is not JSON encoded")
	}

	dbVal, err := base64.StdEncoding.DecodeString(jsonResult[transientDataField].(string))
	if err != nil {
		return nil, errors.Wrapf(err, "error from DecodeString for transientDataField")
	}

	_, blockHeight := splitCompositeKeyOfPvtRWSet(dbKey)

	logger.Debugf("scanner next blockHeight %d", blockHeight)

	txPvtRWSet := &rwset.TxPvtReadWriteSet{}
	if err := proto.Unmarshal(dbVal, txPvtRWSet); err != nil {
		return nil, err
	}
	filteredTxPvtRWSet := trimPvtWSet(txPvtRWSet, scanner.filter)
	logger.Debugf("scanner next filteredTxPvtRWSet %v", filteredTxPvtRWSet)

	return &transientstore.EndorserPvtSimulationResults{
		ReceivedAtBlockHeight: blockHeight,
		PvtSimulationResults:  filteredTxPvtRWSet,
	}, nil
}

// Next moves the iterator to the next key/value pair.
// It returns whether the iterator is exhausted.
// TODO: Once the related gossip changes are made as per FAB-5096, rename this function to Next
func (scanner *RwsetScanner) NextWithConfig() (*transientstore.EndorserPvtSimulationResultsWithConfig, error) {
	if len(scanner.results) < 1 {
		logger.Debugf("scanner NextWithConfig is empty return")
		return nil, nil
	}
	defer func() { scanner.results = append(scanner.results[:0], scanner.results[1:]...) }()

/*	stopWatch := metrics.StopWatch("cdbtransientdata_couchdb_nextwithconfig_duration")
	defer stopWatch()*/

	dbKey, err := hex.DecodeString(scanner.results[0].ID)
	if err != nil {
		return nil, err
	}

	logger.Errorf("NextWithConfig() txID %s key %s", scanner.txid, dbKey)

	jsonResult := make(map[string]interface{})
	decoder := json.NewDecoder(bytes.NewBuffer(scanner.results[0].Value))
	decoder.UseNumber()

	err = decoder.Decode(&jsonResult)
	if err != nil {
		return nil, errors.Wrapf(err, "result from DB is not JSON encoded")
	}

	dbVal, err := base64.StdEncoding.DecodeString(jsonResult[transientDataField].(string))
	if err != nil {
		return nil, errors.Wrapf(err, "error from DecodeString for transientDataField")
	}

	_, blockHeight := splitCompositeKeyOfPvtRWSet(dbKey)

	logger.Debugf("scanner NextWithConfig blockHeight %d", blockHeight)

	filteredTxPvtRWSet := &rwset.TxPvtReadWriteSet{}
	txPvtRWSetWithConfig := &pb.TxPvtReadWriteSetWithConfigInfo{}

	// new proto, i.e., TxPvtReadWriteSetWithConfigInfo
	if err := proto.Unmarshal(dbVal, txPvtRWSetWithConfig); err != nil {
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

// Close releases resource held by the iterator
func (scanner *RwsetScanner) Close() {

}
