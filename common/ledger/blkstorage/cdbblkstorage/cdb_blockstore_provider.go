/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbblkstorage

import (
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("peer")

const (
	blockStoreDBName = "blocks"
)

// CDBBlockstoreProvider provides block storage in CouchDB
type CDBBlockstoreProvider struct {
	couchInstance *couchdb.CouchInstance
	indexConfig *blkstorage.IndexConfig
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
func (p *CDBBlockstoreProvider) OpenBlockStore(ledgerid string) (blkstorage.BlockStore, error) {
	dbName := couchdb.ConstructBlockchainDBName(ledgerid, blockStoreDBName)

	db, err := couchdb.CreateCouchDatabase(p.couchInstance, dbName)
	if err != nil {
		return nil, err
	}

	err = p.createIndices(db)
	if err != nil {
		return nil, err
	}

	return newCDBBlockStore(db, ledgerid, p.indexConfig), nil
}

func (p *CDBBlockstoreProvider) createIndices(db *couchdb.CouchDatabase) error {
	// TODO: only create index if it doesn't exist
	_, err := db.CreateIndex(blockHashIndexDef)
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
