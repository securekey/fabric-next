/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbtransientdata

import (
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/hyperledger/fabric/core/transientstore"
	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("peer")

const (
	transientDataStoreName = "transientdata"
)

type Provider struct {
	couchInstance *couchdb.CouchInstance
}

// NewProvider instantiates a transient data storage provider backed by CouchDB
func NewProvider() (*Provider, error) {
	logger.Debugf("constructing CouchDB transient data storage provider")
	couchDBDef := couchdb.GetCouchDBDefinition()
	couchInstance, err := couchdb.CreateCouchInstance(couchDBDef.URL, couchDBDef.Username, couchDBDef.Password,
		couchDBDef.MaxRetries, couchDBDef.MaxRetriesOnStartup, couchDBDef.RequestTimeout)
	if err != nil {
		return nil, errors.WithMessage(err, "obtaining CouchDB instance failed")
	}

	return &Provider{couchInstance}, nil
}

// OpenStore creates a handle to the transient data store for the given ledger ID
func (p *Provider) OpenStore(ledgerid string) (transientstore.Store, error) {
	transientDataStoreDBName := couchdb.ConstructBlockchainDBName(ledgerid, transientDataStoreName)

	if ledgerconfig.IsCommitter() {
		return createCommitterTransientStore(p.couchInstance, transientDataStoreDBName)
	}

	return createTransientStore(p.couchInstance, transientDataStoreDBName)
}

func createTransientStore(couchInstance *couchdb.CouchInstance, dbName string) (transientstore.Store, error) {
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

	return newStore(db)
}

func createCommitterTransientStore(couchInstance *couchdb.CouchInstance, dbName string) (transientstore.Store, error) {
	db, err := couchdb.CreateCouchDatabase(couchInstance, dbName)
	if err != nil {
		return nil, err
	}

	err = createTransientStoreIndices(db)
	if err != nil {
		return nil, err
	}
	return newStore(db)
}

func createTransientStoreIndices(db *couchdb.CouchDatabase) error {
	return nil
}

func transientStoreIndicesCreated(db *couchdb.CouchDatabase) (bool, error) {
	return false, nil
}

// Close cleans up the provider
func (p *Provider) Close() {
}
