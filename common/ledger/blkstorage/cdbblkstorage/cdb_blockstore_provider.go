/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbblkstorage

import (
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("peer")

const (
	blockStoreName = "blocks"
	txnStoreName   = "transactions"
)

// CDBBlockstoreProvider provides block storage in CouchDB
type CDBBlockstoreProvider struct {
	couchInstance *couchdb.CouchInstance
	indexConfig   *blkstorage.IndexConfig
}

// NewProvider creates a new CouchDB BlockStoreProvider
func NewProvider(indexConfig *blkstorage.IndexConfig) (blkstorage.BlockStoreProvider, error) {
	logger.Debugf("constructing CouchDB block storage provider")
	couchDBDef := couchdb.GetCouchDBDefinition()
	couchInstance, err := couchdb.CreateCouchInstance(couchDBDef.URL, couchDBDef.Username, couchDBDef.Password,
		couchDBDef.MaxRetries, couchDBDef.MaxRetriesOnStartup, couchDBDef.RequestTimeout)
	if err != nil {
		return nil, errors.WithMessage(err, "obtaining CouchDB instance failed")
	}
	return &CDBBlockstoreProvider{couchInstance, indexConfig}, nil
}

// CreateBlockStore creates a block store instance for the given ledger ID
func (p *CDBBlockstoreProvider) CreateBlockStore(ledgerid string) (blkstorage.BlockStore, error) {
	return p.OpenBlockStore(ledgerid)
}

// OpenBlockStore opens the block store for the given ledger ID
func (p *CDBBlockstoreProvider) OpenBlockStore(ledgerID string) (blkstorage.BlockStore, error) {
	blockStoreDBName := couchdb.ConstructBlockchainDBName(ledgerID, blockStoreName)
	txnStoreDBName := couchdb.ConstructBlockchainDBName(ledgerID, txnStoreName)

	if ledgerconfig.IsCommitter() {
		return createCommitterBlockStore(p.couchInstance, ledgerID, blockStoreDBName, txnStoreDBName, p.indexConfig)
	}

	return createBlockStore(p.couchInstance, ledgerID, blockStoreDBName, txnStoreDBName, p.indexConfig)
}


func createCommitterBlockStore(couchInstance *couchdb.CouchInstance, ledgerID string, blockStoreDBName string, tnxStoreDBName string, indexConfig *blkstorage.IndexConfig) (blkstorage.BlockStore, error) {
	blockStoreDB, err := createCommitterBlockStoreDB(couchInstance, blockStoreDBName)
	if err != nil {
		return nil, err
	}

	txnStoreDB, err := createCommitterTxnStoreDB(couchInstance, tnxStoreDBName)
	if err != nil {
		return nil, err
	}

	return newCDBBlockStore(blockStoreDB, txnStoreDB, ledgerID, indexConfig), nil
}

func createCommitterBlockStoreDB(couchInstance *couchdb.CouchInstance, dbName string) (*couchdb.CouchDatabase, error) {
	db, err := couchdb.CreateCouchDatabase(couchInstance, dbName)
	if err != nil {
		return nil, err
	}

	err = createBlockStoreIndices(db)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func createCommitterTxnStoreDB(couchInstance *couchdb.CouchInstance, dbName string) (*couchdb.CouchDatabase, error) {
	db, err := couchdb.CreateCouchDatabase(couchInstance, dbName)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func createBlockStore(couchInstance *couchdb.CouchInstance, ledgerID string, blockStoreDBName string, tnxStoreDBName string, indexConfig *blkstorage.IndexConfig) (blkstorage.BlockStore, error) {
	blockStoreDB, err := createBlockStoreDB(couchInstance, blockStoreDBName)
	if err != nil {
		return nil, err
	}

	txnStoreDB, err := createTxnStoreDB(couchInstance, tnxStoreDBName)
	if err != nil {
		return nil, err
	}

	return newCDBBlockStore(blockStoreDB, txnStoreDB, ledgerID, indexConfig), nil
}

func createBlockStoreDB(couchInstance *couchdb.CouchInstance, dbName string) (*couchdb.CouchDatabase, error) {
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

	indexExists, err := db.IndexDesignDocExistsWithRetry(blockHashIndexDoc)
	if err != nil {
		return nil, err
	}
	if !indexExists {
		return nil, errors.Errorf("DB index not found: [%s]", db.DBName)
	}

	return db, nil
}

func createTxnStoreDB(couchInstance *couchdb.CouchInstance, dbName string) (*couchdb.CouchDatabase, error) {
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

	return db, nil
}

func createBlockStoreIndices(db *couchdb.CouchDatabase) error {
	_, err := db.CreateNewIndexWithRetry(blockHashIndexDef, blockHashIndexDoc)
	if err != nil {
		return errors.WithMessage(err, "creation of block hash index failed")
	}

	return nil
}

// Exists returns whether or not the given ledger ID exists
func (p *CDBBlockstoreProvider) Exists(ledgerid string) (bool, error) {
	return false, errors.New("not implemented")
}

// List returns the available ledger IDs
func (p *CDBBlockstoreProvider) List() ([]string, error) {
	return []string{}, errors.New("not implemented")
}

// Close cleans up the Provider
func (p *CDBBlockstoreProvider) Close() {
}
