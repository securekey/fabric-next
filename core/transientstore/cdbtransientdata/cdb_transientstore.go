/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbtransientdata

import (
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/gogo/protobuf/proto"
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
	for _, endorser := range endorsers {
		hash, err := hashPeerIdentity(endorser.Endorser)
		if err != nil {
			return nil, err
		}
		fmt.Printf("*** getTxPvtRWSetByTxidDB database %s\n", fmt.Sprintf(transientDataStoreName, hash))
		db, err := getCouchDB(s.db.CouchInstance, couchdb.ConstructBlockchainDBName(s.ledgerID, fmt.Sprintf(transientDataStoreName, hash)))
		if err != nil {
			return nil, err
		}
		fmt.Printf("*** getTxPvtRWSetByTxidDB queryFmt %s\n", fmt.Sprintf(queryFmt, txid))
		results, err = db.QueryDocuments(fmt.Sprintf(queryFmt, txid))
		if err != nil {
			return nil, err
		}
		fmt.Printf("*** getTxPvtRWSetByTxidDB results %s\n", results)

		if len(results) == 0 {
			logger.Warningf("getTxPvtRWSetByTxidDB didn't find pvt rwset in endorser db %s", fmt.Sprintf(transientDataStoreName, hash))
		} else {
			return nil, nil
		}
	}
	return nil, errors.New("getTxPvtRWSetByTxidDB didn't find pvt rwset in endorser dbs")

}
