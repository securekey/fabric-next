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
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/hyperledger/fabric/core/transientstore"
	"github.com/hyperledger/fabric/protos/ledger/rwset"
	"github.com/hyperledger/fabric/protos/peer"
	pb "github.com/hyperledger/fabric/protos/transientstore"
	"github.com/pkg/errors"
)

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

	_, err = s.db.SaveDoc(hex.EncodeToString(compositeKeyPvtRWSet), "", doc)
	if err != nil {
		return errors.WithMessage(err, "SaveDoc failed for persist transient store")
	}

	////TODO
	//indices = map[string]string{blockNumberField: fmt.Sprintf("%064s", strconv.FormatUint(blockHeight+1, blockNumberBase)), txIDField: txid}
	//
	//doc, err = keyValueToCouchDoc([]byte("what"), privateSimulationResultsWithConfigBytes, indices)
	//if err != nil {
	//	return err
	//}
	//
	//_, err = s.db.SaveDoc(hex.EncodeToString([]byte("what")), "", doc)
	//if err != nil {
	//	return errors.WithMessage(err, "SaveDoc failed for persist transient store")
	//}

	return nil
}

func (s *store) getTxPvtRWSetByTxidDB(txid string, filter ledger.PvtNsCollFilter, endorsers []*peer.Endorsement) (transientstore.RWSetScanner, error) {
	const queryFmt = `
	{
		"selector": {
			"` + txIDField + `": {
				"$eq": "%s"
			}
		},
		"use_index": ["_design/` + txIDIndexDoc + `", "` + txIDIndexName + `"]
	}`
	var results []*couchdb.QueryResult
	var err error
	results, err = s.db.QueryDocuments(fmt.Sprintf(queryFmt, txid))
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		logger.Warningf("getTxPvtRWSetByTxidDB didn't find pvt rwset in db %s", s.db.DBName)
	} else {
		return &RwsetScanner{filter: filter, results: results}, nil
	}
	for _, endorser := range endorsers {
		hash, err := hashPeerIdentity(endorser.Endorser)
		if err != nil {
			return nil, err
		}
		db, err := getCouchDB(s.db.CouchInstance, couchdb.ConstructBlockchainDBName(s.ledgerID, fmt.Sprintf(transientDataStoreName, hash)))
		if err != nil {
			return nil, err
		}
		results, err = db.QueryDocuments(fmt.Sprintf(queryFmt, txid))
		if err != nil {
			return nil, err
		}

		if len(results) == 0 {
			logger.Warningf("getTxPvtRWSetByTxidDB didn't find pvt rwset in endorser db %s", fmt.Sprintf(transientDataStoreName, hash))
		} else {
			return &RwsetScanner{filter: filter, results: results}, nil
		}
	}
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

	results, err := s.db.QueryDocuments(fmt.Sprintf(queryFmt, fmt.Sprintf("%064s", strconv.FormatUint(0, blockNumberBase))))
	if err != nil {
		return 0, err
	}
	if len(results) == 0 {
		return 0, transientstore.ErrStoreEmpty
	}
	dbKey, err := hex.DecodeString(results[0].ID)
	if err != nil {
		return 0, err
	}
	_, blockHeight := splitCompositeKeyOfPvtRWSet(dbKey)
	return blockHeight, nil
}

type RwsetScanner struct {
	filter  ledger.PvtNsCollFilter
	results []*couchdb.QueryResult
}

// Next moves the iterator to the next key/value pair.
// It returns whether the iterator is exhausted.
// TODO: Once the related gossip changes are made as per FAB-5096, remove this function
func (scanner *RwsetScanner) Next() (*transientstore.EndorserPvtSimulationResults, error) {
	if len(scanner.results) < 1 {
		return nil, nil
	}
	dbKey, err := hex.DecodeString(scanner.results[0].ID)
	if err != nil {
		return nil, err
	}

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

	txPvtRWSet := &rwset.TxPvtReadWriteSet{}
	if err := proto.Unmarshal(dbVal, txPvtRWSet); err != nil {
		return nil, err
	}
	filteredTxPvtRWSet := trimPvtWSet(txPvtRWSet, scanner.filter)

	scanner.results = append(scanner.results[:0], scanner.results[1:]...)

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
		return nil, nil
	}
	dbKey, err := hex.DecodeString(scanner.results[0].ID)
	if err != nil {
		return nil, err
	}

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

	filteredTxPvtRWSet := &rwset.TxPvtReadWriteSet{}
	txPvtRWSetWithConfig := &pb.TxPvtReadWriteSetWithConfigInfo{}

	// new proto, i.e., TxPvtReadWriteSetWithConfigInfo
	if err := proto.Unmarshal(dbVal, txPvtRWSetWithConfig); err != nil {
		return nil, err
	}

	filteredTxPvtRWSet = trimPvtWSet(txPvtRWSetWithConfig.GetPvtRwset(), scanner.filter)
	configs, err := trimPvtCollectionConfigs(txPvtRWSetWithConfig.CollectionConfigs, scanner.filter)
	if err != nil {
		return nil, err
	}
	txPvtRWSetWithConfig.CollectionConfigs = configs

	txPvtRWSetWithConfig.PvtRwset = filteredTxPvtRWSet

	scanner.results = append(scanner.results[:0], scanner.results[1:]...)

	return &transientstore.EndorserPvtSimulationResultsWithConfig{
		ReceivedAtBlockHeight:          blockHeight,
		PvtSimulationResultsWithConfig: txPvtRWSetWithConfig,
	}, nil
}

// Close releases resource held by the iterator
func (scanner *RwsetScanner) Close() {

}
