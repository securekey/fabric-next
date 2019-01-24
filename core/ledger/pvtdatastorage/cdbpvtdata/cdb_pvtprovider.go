/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbpvtdata

import (
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/metrics/disabled"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/core/ledger/pvtdatastorage"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("pvtdatastorage")

const (
	pvtDataStoreName = "pvtdata"
)

type Provider struct {
	couchInstance *couchdb.CouchInstance
}

// NewProvider instantiates a private data storage provider backed by CouchDB
func NewProvider() (*Provider, error) {
	logger.Debugf("constructing CouchDB private data storage provider")
	couchDBDef := couchdb.GetCouchDBDefinition()

	return newProviderWithDBDef(couchDBDef)
}

func newProviderWithDBDef(couchDBDef *couchdb.CouchDBDef) (*Provider, error) {
	//TODO add metrics provider
	couchInstance, err := couchdb.CreateCouchInstance(couchDBDef.URL, couchDBDef.Username, couchDBDef.Password,
		couchDBDef.MaxRetries, couchDBDef.MaxRetriesOnStartup, couchDBDef.RequestTimeout, couchDBDef.CreateGlobalChangesDB, &disabled.Provider{})
	if err != nil {
		return nil, errors.WithMessage(err, "obtaining CouchDB instance failed")
	}

	return &Provider{couchInstance}, nil
}

// OpenStore creates a handle to the private data store for the given ledger ID
func (p *Provider) OpenStore(ledgerid string) (pvtdatastorage.Store, error) {
	pvtDataStoreDBName := couchdb.ConstructBlockchainDBName(ledgerid, pvtDataStoreName)

	if ledgerconfig.IsCommitter() {
		return createCommitterPvtDataStore(p.couchInstance, pvtDataStoreDBName)
	}

	return createPvtDataStore(p.couchInstance, pvtDataStoreDBName)
}

func createPvtDataStore(couchInstance *couchdb.CouchInstance, dbName string) (pvtdatastorage.Store, error) {
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
	indexExists, err := db.IndexDesignDocExistsWithRetry(purgeBlockNumberIndexDoc)
	if err != nil {
		return nil, err
	}
	if !indexExists {
		return nil, errors.Errorf("DB index not found: [%s]", db.DBName)
	}
	return newStore(db)
}

func createCommitterPvtDataStore(couchInstance *couchdb.CouchInstance, dbName string) (pvtdatastorage.Store, error) {
	db, err := couchdb.CreateCouchDatabase(couchInstance, dbName)
	if err != nil {
		return nil, err
	}

	err = createPvtStoreIndices(db)
	if err != nil {
		return nil, err
	}

	return newStore(db)
}

func createPvtStoreIndices(db *couchdb.CouchDatabase) error {
	err := db.CreateNewIndexWithRetry(purgeBlockNumberIndexDef, purgeBlockNumberIndexDoc)
	if err != nil {
		return errors.WithMessage(err, "creation of purge block number index failed")
	}
	return nil
}

// Close cleans up the provider
func (p *Provider) Close() {
}
