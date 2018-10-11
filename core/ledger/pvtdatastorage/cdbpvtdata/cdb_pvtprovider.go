/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbpvtdata

import (
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/ledger/pvtdatastorage"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("peer")

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
	couchInstance, err := couchdb.CreateCouchInstance(couchDBDef.URL, couchDBDef.Username, couchDBDef.Password,
		couchDBDef.MaxRetries, couchDBDef.MaxRetriesOnStartup, couchDBDef.RequestTimeout)
	if err != nil {
		return nil, errors.WithMessage(err, "obtaining CouchDB instance failed")
	}

	return &Provider{couchInstance}, nil
}

// OpenStore creates a handle to the private data store for the given ledger ID
func (p *Provider) OpenStore(ledgerid string) (pvtdatastorage.Store, error) {
	pvtDataStoreDBName := couchdb.ConstructBlockchainDBName(ledgerid, pvtDataStoreName)
	pvtStoreDB, err := couchdb.CreateCouchDatabase(p.couchInstance, pvtDataStoreDBName)
	if err != nil {
		return nil, err
	}

	indexExists, err := pvtStoreIndicesCreated(pvtStoreDB)
	if err != nil {
		return nil, err
	}

	if !indexExists {
		err = createPvtStoreIndices(pvtStoreDB)
		if err != nil {
			return nil, err
		}
	}
	return newStore(pvtStoreDB)
}

func createPvtStoreIndices(db *couchdb.CouchDatabase) error {
	_, err := db.CreateIndex(blockNumberIndexDef)
	if err != nil {
		return errors.WithMessage(err, "creation of block number index failed")
	}
	_, err = db.CreateIndex(blockNumberExpiryIndexDef)
	if err != nil {
		return errors.WithMessage(err, "creation of block number index failed")
	}
	return nil
}

func pvtStoreIndicesCreated(db *couchdb.CouchDatabase) (bool, error) {
	var blockNumberIndexExists, blockNumberExpiryIndexExists bool

	indices, err := db.ListIndex()
	if err != nil {
		return false, errors.WithMessage(err, "retrieval of DB index list failed")
	}

	for _, i := range indices {
		if i.DesignDocument == blockNumberIndexDoc {
			blockNumberIndexExists = true
		}
		if i.DesignDocument == blockNumberExpiryIndexDoc {
			blockNumberExpiryIndexExists = true
		}
	}

	exists := blockNumberIndexExists && blockNumberExpiryIndexExists
	return exists, nil
}

// Close cleans up the provider
func (p *Provider) Close() {
}
