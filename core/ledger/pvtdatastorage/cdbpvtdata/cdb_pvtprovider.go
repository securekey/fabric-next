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
	err = p.createPvtStoreIndices(pvtStoreDB)
	if err != nil {
		return nil, err
	}

	return newStore(pvtStoreDB)
}

func (p *Provider) createPvtStoreIndices(db *couchdb.CouchDatabase) error {
	// TODO: only create index if it doesn't exist
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

// Close cleans up the provider
func (p *Provider) Close() {
}
