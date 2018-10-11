/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbtransientdata

import (
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/util/retry"
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

			indicesExist, err := transientStoreIndicesCreated(db)
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

	return newStore(db)
}

func createCommitterTransientStore(couchInstance *couchdb.CouchInstance, dbName string) (transientstore.Store, error) {
	// TODO: Make configurable
	const maxAttempts = 10

	dbUT, err := retry.Invoke(
		func() (interface{}, error) {
			db, err := couchdb.CreateCouchDatabase(couchInstance, dbName)
			if err != nil {
				return nil, err
			}

			err = createTransientStoreIndicesIfNotExist(db)
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

	return newStore(db)
}


func createTransientStoreIndicesIfNotExist(db *couchdb.CouchDatabase) error {

	indexExists, err := transientStoreIndicesCreated(db)
	if err != nil {
		return err
	}

	if !indexExists {
		err = createTransientStoreIndices(db)
		if err != nil {
			return err
		}
	}

	return nil
}

func createTransientStoreIndices(db *couchdb.CouchDatabase) error {
	// TODO: only create index if it doesn't exist
	return nil
}

func transientStoreIndicesCreated(db *couchdb.CouchDatabase) (bool, error) {
	return false, nil
}

// Close cleans up the provider
func (p *Provider) Close() {
}
