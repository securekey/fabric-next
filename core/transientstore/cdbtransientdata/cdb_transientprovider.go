/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbtransientdata

import (
	"fmt"

	"crypto/sha256"

	"encoding/base64"

	"strings"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/metrics/disabled"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/hyperledger/fabric/core/transientstore"
	mspmgmt "github.com/hyperledger/fabric/msp/mgmt"
	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("transientstore.couchdb")

const (
	transientDataStoreName = "transientdata_%s"
)

type Provider struct {
	couchInstance *couchdb.CouchInstance
}

// NewProvider instantiates a transient data storage provider backed by CouchDB
func NewProvider() (*Provider, error) {
	logger.Debugf("constructing CouchDB transient data storage provider")
	couchDBDef := couchdb.GetCouchDBDefinition()
	//TODO add metrics provider
	couchInstance, err := couchdb.CreateCouchInstance(couchDBDef.URL, couchDBDef.Username, couchDBDef.Password,
		couchDBDef.MaxRetries, couchDBDef.MaxRetriesOnStartup, couchDBDef.RequestTimeout, couchDBDef.CreateGlobalChangesDB, &disabled.Provider{})
	if err != nil {
		return nil, errors.WithMessage(err, "obtaining CouchDB instance failed")
	}

	return &Provider{couchInstance}, nil
}

// OpenStore creates a handle to the transient data store for the given ledger ID
func (p *Provider) OpenStore(ledgerid string) (transientstore.Store, error) {
	signingIdentity, err := mspmgmt.GetLocalMSP().GetDefaultSigningIdentity()
	if err != nil {
		return nil, errors.WithMessage(err, "obtaining signing identity failed")
	}
	s, err := signingIdentity.GetPublicVersion().Serialize()
	if err != nil {
		return nil, errors.WithMessage(err, "serialize publicversion failed")
	}
	hash, err := hashPeerIdentity(s)
	transientDataStoreDBName := couchdb.ConstructBlockchainDBName(ledgerid, fmt.Sprintf(transientDataStoreName, hash))

	return createTransientStore(p.couchInstance, transientDataStoreDBName, ledgerid)
}

func hashPeerIdentity(identity []byte) (string, error) {
	hash := sha256.New()
	_, err := hash.Write(identity)
	if err != nil {
		return "", errors.WithMessage(err, "hash identity failed")
	}
	sha := base64.RawURLEncoding.EncodeToString(hash.Sum(nil))
	return strings.ToLower(sha), nil

}

func createTransientStore(couchInstance *couchdb.CouchInstance, dbName, ledgerID string) (transientstore.Store, error) {
	db, err := couchdb.CreateCouchDatabase(couchInstance, dbName)
	if err != nil {
		return nil, err
	}

	err = createTransientStoreIndices(db)
	if err != nil {
		return nil, err
	}
	return newStore(db, ledgerID)
}

func createTransientStoreIndices(db *couchdb.CouchDatabase) error {
	err := db.CreateNewIndexWithRetry(blockNumberIndexDef, blockNumberIndexDoc)
	if err != nil {
		return errors.WithMessage(err, "creation of block number index failed")
	}
	err = db.CreateNewIndexWithRetry(txIDIndexDef, txIDIndexDoc)
	if err != nil {
		return errors.WithMessage(err, "creation of block number index failed")
	}
	return nil
}

func getCouchDB(couchInstance *couchdb.CouchInstance, dbName string) (*couchdb.CouchDatabase, error) {
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

// Close cleans up the provider
func (p *Provider) Close() {
}
