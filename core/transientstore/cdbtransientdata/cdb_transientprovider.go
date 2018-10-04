/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbtransientdata

import (
	"github.com/hyperledger/fabric/common/flogging"
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
	pvtStoreDB, err := couchdb.CreateCouchDatabase(p.couchInstance, transientDataStoreDBName)
	if err != nil {
		return nil, err
	}
	err = p.createTransientStoreIndices(pvtStoreDB)
	if err != nil {
		return nil, err
	}

	return newStore(pvtStoreDB)
}

func (p *Provider) createTransientStoreIndices(db *couchdb.CouchDatabase) error {
	// TODO: only create index if it doesn't exist
	return nil
}

// Close cleans up the provider
func (p *Provider) Close() {
}
