/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbid

import (
	"fmt"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/metrics/disabled"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("peer")

type Store struct {
	db               *couchdb.CouchDatabase
	couchMetadataRev string
}

func OpenStore() (*Store, error) {
	const systemID = "fabric_system_"
	const inventoryName = "inventory"

	couchInstance, err := createCouchInstance()
	if err != nil {
		return nil, err
	}

	inventoryDBName := couchdb.ConstructBlockchainDBName(systemID, inventoryName)

	if ledgerconfig.IsCommitter() {
		return newCommitterStore(couchInstance, inventoryDBName)
	}

	return newStore(couchInstance, inventoryDBName)
}

func newStore(couchInstance *couchdb.CouchInstance, dbName string) (*Store, error) {
	db, err := couchdb.NewCouchDatabase(couchInstance, dbName)
	if err != nil {
		return nil, err
	}

	dbExists, err := db.ExistsWithRetry()
	if err != nil {
		return nil, err
	}
	if !dbExists {
		return nil, errors.Errorf("DB not found: [%s]", db.DBName)
	}

	indexExists, err := db.IndexDesignDocExistsWithRetry(inventoryTypeIndexDoc)
	if err != nil {
		return nil, err
	}
	if !indexExists {
		return nil, errors.Errorf("DB index not found: [%s]", db.DBName)
	}

	s := Store{db, ""}
	return &s, nil
}

func newCommitterStore(couchInstance *couchdb.CouchInstance, dbName string) (*Store, error) {
	db, err := couchdb.CreateCouchDatabase(couchInstance, dbName)
	if err != nil {
		return nil, err
	}

	err = createIndices(db)
	if err != nil {
		return nil, err
	}

	s := Store{db, ""}

	return &s, nil
}

func createIndices(db *couchdb.CouchDatabase) error {
	err := db.CreateNewIndexWithRetry(inventoryTypeIndexDef, inventoryTypeIndexDoc)
	if err != nil {
		return errors.WithMessage(err, "creation of inventory metadata index failed")
	}
	return nil
}

func createCouchInstance() (*couchdb.CouchInstance, error) {
	logger.Debugf("constructing CouchDB block storage provider")
	couchDBDef := couchdb.GetCouchDBDefinition()
	//TODO add metrics provider
	couchInstance, err := couchdb.CreateCouchInstance(couchDBDef.URL, couchDBDef.Username, couchDBDef.Password,
		couchDBDef.MaxRetries, couchDBDef.MaxRetriesOnStartup, couchDBDef.RequestTimeout, couchDBDef.CreateGlobalChangesDB, &disabled.Provider{})
	if err != nil {
		return nil, errors.WithMessage(err, "obtaining CouchDB instance failed")
	}

	return couchInstance, nil
}

func (s *Store) SetUnderConstructionFlag(ledgerID string) error {
	doc, err := createMetadataDoc(ledgerID)
	if err != nil {
		return err
	}

	rev, err := s.db.SaveDoc(metadataKey, s.couchMetadataRev, doc)
	if err != nil {
		return errors.WithMessage(err, "update of metadata in CouchDB failed [%s]")
	}

	s.couchMetadataRev = rev

	logger.Debugf("updated metadata in CouchDB inventory [%s]", rev)
	return nil
}

func (s *Store) UnsetUnderConstructionFlag() error {
	doc, err := createMetadataDoc("")
	if err != nil {
		return err
	}

	rev, err := s.db.SaveDoc(metadataKey, s.couchMetadataRev, doc)
	if err != nil {
		return errors.WithMessage(err, "update of metadata in CouchDB failed [%s]")
	}

	s.couchMetadataRev = rev

	logger.Debugf("updated metadata in CouchDB inventory [%s]", rev)
	return nil
}

func (s *Store) GetUnderConstructionFlag() (string, error) {
	doc, _, err := s.db.ReadDoc(metadataKey)
	if err != nil {
		return "", errors.WithMessage(err, "retrieval of metadata from CouchDB inventory failed")
	}

	// if metadata does not exist, assume that there is nothing under construction.
	if doc == nil {
		return "", nil
	}

	metadata, err := couchDocToJSON(doc)
	if err != nil {
		return "", errors.WithMessage(err, "metadata in CouchDB inventory is invalid")
	}

	constructionLedgerUT := metadata[underConstructionLedgerKey]
	constructionLedger, ok := constructionLedgerUT.(string)
	if !ok {
		return "", errors.New("metadata under construction key in CouchDB inventory is invalid")
	}

	return constructionLedger, nil
}

func (s *Store) CreateLedgerID(ledgerID string, gb *common.Block) error {
	exists, err := s.LedgerIDExists(ledgerID)
	if err != nil {
		return err
	}

	if exists {
		return errors.Errorf("ledger already exists [%s]", ledgerID)
	}

	doc, err := ledgerToCouchDoc(ledgerID, gb)
	if err != nil {
		return err
	}

	id := ledgerIDToKey(ledgerID)
	rev, err := s.db.UpdateDoc(id, "", doc)
	if err != nil {
		return errors.WithMessage(err, fmt.Sprintf("creation of ledger failed [%s]", ledgerID))
	}

	err = s.UnsetUnderConstructionFlag()
	if err != nil {
		return err
	}

	logger.Debugf("created ledger in CouchDB inventory [%s, %s]", ledgerID, rev)
	return nil
}

func (s *Store) LedgerIDExists(ledgerID string) (bool, error) {
	doc, _, err := s.db.ReadDoc(ledgerIDToKey(ledgerID))
	if err != nil {
		return false, err
	}

	exists := doc != nil
	return exists, nil
}

func (s *Store) GetAllLedgerIds() ([]string, error) {
	results, err := queryInventory(s.db, typeLedgerName)
	if err != nil {
		return nil, err
	}

	ledgers := make([]string, 0)
	for _, r := range results {
		ledgerJSON, err := couchValueToJSON(r.Value)
		if err != nil {
			return nil, err
		}

		ledgerIDUT, ok := ledgerJSON[inventoryNameLedgerIDField]
		if !ok {
			return nil, errors.Errorf("ledger inventory document is invalid [%s]", r.ID)
		}

		ledgerID, ok := ledgerIDUT.(string)
		if !ok {
			return nil, errors.Errorf("ledger inventory document value is invalid [%s]", r.ID)
		}

		ledgers = append(ledgers, ledgerID)
	}

	return ledgers, nil
}

func (s *Store) Close() {
}
