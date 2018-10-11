/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbblkstorage

import (
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/common/util/retry"
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
	// TODO: Make configurable
	const maxAttempts = 10

	dbUT, err := retry.Invoke(
		func() (interface{}, error) {
			db, err := couchdb.CreateCouchDatabase(couchInstance, dbName)
			if err != nil {
				return nil, err
			}

			err = createBlockStoreIndicesIfNotExist(db)
			if err != nil {
				return nil, err
			}
			return db, nil
		},
		retry.WithMaxAttempts(maxAttempts),
	)

	if err != nil {
		return nil, err
	}

	db := dbUT.(*couchdb.CouchDatabase)
	return db, nil
}

func createCommitterTxnStoreDB(couchInstance *couchdb.CouchInstance, dbName string) (*couchdb.CouchDatabase, error) {
	// TODO: Make configurable
	const maxAttempts = 10

	dbUT, err := retry.Invoke(
		func() (interface{}, error) {
			db, err := couchdb.CreateCouchDatabase(couchInstance, dbName)
			if err != nil {
				return nil, err
			}

			return db, nil
		},
		retry.WithMaxAttempts(maxAttempts),
	)

	if err != nil {
		return nil, err
	}

	db := dbUT.(*couchdb.CouchDatabase)
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
	// TODO: Make configurable
	maxAttempts := 10

	db, err := couchdb.NewCouchDatabase(couchInstance, dbName)
	if err != nil {
		return nil, err
	}

	_, err = retry.Invoke(
		func() (interface{}, error) {
			dbInfo, couchDBReturn, err := db.GetDatabaseInfo()
			if err != nil {
				return nil, err
			}

			//If the dbInfo returns populated and status code is 200, then the database exists
			if dbInfo == nil || couchDBReturn.StatusCode != 200 {
				return nil, errors.Errorf("DB not found: [%s]", db.DBName)
			}

			indicesExist, err := blockStoreIndicesCreated(db)
			if err != nil {
				return nil, err
			}
			if !indicesExist {
				return nil, errors.Errorf("DB indices not found: [%s]", db.DBName)
			}

			// DB and indices exists
			return nil, nil
		},
		retry.WithMaxAttempts(maxAttempts),
	)

	if err != nil {
		return nil, err
	}

	return db, nil
}

func createTxnStoreDB(couchInstance *couchdb.CouchInstance, dbName string) (*couchdb.CouchDatabase, error) {
	// TODO: Make configurable
	maxAttempts := 10

	db, err := couchdb.NewCouchDatabase(couchInstance, dbName)
	if err != nil {
		return nil, err
	}

	_, err = retry.Invoke(
		func() (interface{}, error) {
			dbInfo, couchDBReturn, err := db.GetDatabaseInfo()
			if err != nil {
				return nil, err
			}

			//If the dbInfo returns populated and status code is 200, then the database exists
			if dbInfo == nil || couchDBReturn.StatusCode != 200 {
				return nil, errors.Errorf("DB not found: [%s]", db.DBName)
			}

			// DB and indices exists
			return nil, nil
		},
		retry.WithMaxAttempts(maxAttempts),
	)

	if err != nil {
		return nil, err
	}

	return db, nil
}

func createBlockStoreIndicesIfNotExist(db *couchdb.CouchDatabase) error {

	indexExists, err := blockStoreIndicesCreated(db)
	if err != nil {
		return err
	}

	if !indexExists {
		err = createBlockStoreIndices(db)
		if err != nil {
			return err
		}
	}

	return nil
}

func createBlockStoreIndices(db *couchdb.CouchDatabase) error {
	_, err := db.CreateIndex(blockHashIndexDef)
	if err != nil {
		return errors.WithMessage(err, "creation of block hash index failed")
	}

	return nil
}

func blockStoreIndicesCreated(db *couchdb.CouchDatabase) (bool, error) {
	var blockHashIndexExists bool

	indices, err := db.ListIndex()
	if err != nil {
		return false, errors.WithMessage(err, "retrieval of DB index list failed")
	}

	for _, i := range indices {
		if i.DesignDocument == blockHashIndexDoc {
			blockHashIndexExists = true
		}
	}

	return blockHashIndexExists, nil
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
