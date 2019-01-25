/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbblkstorage

import (
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/common/metrics/disabled"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("peer")

const (
	blockStoreName = "blocks"
)

// CDBBlockstoreProvider provides block storage in CouchDB
type CDBBlockstoreProvider struct {
	couchInstance     *couchdb.CouchInstance
	blockIndexEnabled bool
}

// NewProvider creates a new CouchDB BlockStoreProvider
func NewProvider(blockIndexEnabled bool) (blkstorage.BlockStoreProvider, error) {
	logger.Debugf("constructing CouchDB block storage provider")
	couchDBDef := couchdb.GetCouchDBDefinition()

	return newProvider(couchDBDef, blockIndexEnabled)
}

func newProvider(couchDBDef *couchdb.CouchDBDef, blockIndexEnabled bool) (blkstorage.BlockStoreProvider, error) {
	//TODO add metrics provider
	couchInstance, err := couchdb.CreateCouchInstance(couchDBDef.URL, couchDBDef.Username, couchDBDef.Password,
		couchDBDef.MaxRetries, couchDBDef.MaxRetriesOnStartup, couchDBDef.RequestTimeout, couchDBDef.CreateGlobalChangesDB, &disabled.Provider{})
	if err != nil {
		return nil, errors.WithMessage(err, "obtaining CouchDB instance failed")
	}
	return &CDBBlockstoreProvider{couchInstance, blockIndexEnabled}, nil
}

// CreateBlockStore creates a block store instance for the given ledger ID
func (p *CDBBlockstoreProvider) CreateBlockStore(ledgerid string) (blkstorage.BlockStore, error) {
	return p.OpenBlockStore(ledgerid)
}

// OpenBlockStore opens the block store for the given ledger ID
func (p *CDBBlockstoreProvider) OpenBlockStore(ledgerID string) (blkstorage.BlockStore, error) {
	blockStoreDBName := couchdb.ConstructBlockchainDBName(ledgerID, blockStoreName)
	if ledgerconfig.IsCommitter() {
		return createCommitterBlockStore(p.couchInstance, ledgerID, blockStoreDBName, p.blockIndexEnabled)
	}

	return createBlockStore(p.couchInstance, ledgerID, blockStoreDBName, p.blockIndexEnabled)
}

func createCommitterBlockStore(couchInstance *couchdb.CouchInstance, ledgerID string, blockStoreDBName string, blockIndexEnabled bool) (blkstorage.BlockStore, error) {
	blockStoreDB, err := createCommitterBlockStoreDB(couchInstance, blockStoreDBName, blockIndexEnabled)
	if err != nil {
		return nil, err
	}

	return newCDBBlockStore(blockStoreDB, ledgerID, blockIndexEnabled), nil
}

func createCommitterBlockStoreDB(couchInstance *couchdb.CouchInstance, dbName string, blockIndexEnabled bool) (*couchdb.CouchDatabase, error) {
	db, err := couchdb.CreateCouchDatabase(couchInstance, dbName)
	if err != nil {
		return nil, err
	}

	if blockIndexEnabled {
		err = createBlockStoreIndices(db)
		if err != nil {
			return nil, err
		}
	}
	return db, nil
}

func createBlockStore(couchInstance *couchdb.CouchInstance, ledgerID string, blockStoreDBName string, blockIndexEnabled bool) (blkstorage.BlockStore, error) {
	blockStoreDB, err := createBlockStoreDB(couchInstance, blockStoreDBName, blockIndexEnabled)
	if err != nil {
		return nil, err
	}

	return newCDBBlockStore(blockStoreDB, ledgerID, blockIndexEnabled), nil
}

func createBlockStoreDB(couchInstance *couchdb.CouchInstance, dbName string, blockIndexEnabled bool) (*couchdb.CouchDatabase, error) {
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

	if blockIndexEnabled {
		indexExists, err := db.IndexDesignDocExistsWithRetry(blockHashIndexDoc)
		if err != nil {
			return nil, err
		}
		if !indexExists {
			return nil, errors.Errorf("DB index not found: [%s]", db.DBName)
		}
	}

	return db, nil
}

func createBlockStoreIndices(db *couchdb.CouchDatabase) error {
	err := db.CreateNewIndexWithRetry(blockHashIndexDef, blockHashIndexDoc)
	if err != nil {
		return errors.WithMessage(err, "creation of block hash index failed")
	}

	err = db.CreateNewIndexWithRetry(blockTxnIndexDef, blockTxnIndexDoc)
	if err != nil {
		return errors.WithMessage(err, "creation of txn index failed")
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
