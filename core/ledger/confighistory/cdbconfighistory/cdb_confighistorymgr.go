/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbconfighistory

import (
	"fmt"

	"encoding/binary"
	"math"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/common/privdata"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/confighistory"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/hyperledger/fabric/protos/ledger/rwset/kvrwset"
	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("peer")

const (
	confHistoryDataStoreName = "confighistory"
	keyPrefix                = "s"
	separatorByte            = byte(0)
)

type ConfigHistoryMgr struct {
	couchInstance *couchdb.CouchInstance
	dbs           map[string]*couchdb.CouchDatabase
}

// NewMgr instantiates a config history data storage provider backed by CouchDB
func NewMgr() (confighistory.Mgr, error) {
	logger.Debugf("constructing CouchDB config history data storage provider")
	couchDBDef := couchdb.GetCouchDBDefinition()
	couchInstance, err := couchdb.CreateCouchInstance(couchDBDef.URL, couchDBDef.Username, couchDBDef.Password,
		couchDBDef.MaxRetries, couchDBDef.MaxRetriesOnStartup, couchDBDef.RequestTimeout)
	if err != nil {
		return nil, errors.WithMessage(err, "obtaining CouchDB instance failed")
	}
	return &ConfigHistoryMgr{couchInstance, make(map[string]*couchdb.CouchDatabase)}, nil
}

// OpenStore creates a handle to the transient data store for the given ledger ID
func (p *ConfigHistoryMgr) openStore(ledgerid string) error {
	configHistoryStoreDBName := couchdb.ConstructBlockchainDBName(ledgerid, confHistoryDataStoreName)

	if ledgerconfig.IsCommitter() {
		return p.createCommitterConfigHistoryStore(p.couchInstance, configHistoryStoreDBName, ledgerid)
	}

	return p.createConfigHistoryStore(p.couchInstance, configHistoryStoreDBName, ledgerid)
}

func (c *ConfigHistoryMgr) createConfigHistoryStore(couchInstance *couchdb.CouchInstance, dbName, ledgerID string) error {
	db, err := couchdb.NewCouchDatabase(couchInstance, dbName)
	if err != nil {
		return err
	}

	dbExists, err := db.ExistsWithRetry()
	if err != nil {
		return err
	}
	if !dbExists {
		return errors.Errorf("DB not found: [%s]", db.DBName)
	}

	c.dbs[ledgerID] = db
	return nil
}

func (c *ConfigHistoryMgr) createCommitterConfigHistoryStore(couchInstance *couchdb.CouchInstance, dbName, ledgerID string) error {
	db, err := couchdb.CreateCouchDatabase(couchInstance, dbName)
	if err != nil {
		return err
	}

	err = c.createConfigHistoryStoreIndices(db)
	if err != nil {
		return err
	}
	c.dbs[ledgerID] = db
	return nil
}

func (c *ConfigHistoryMgr) createConfigHistoryStoreIndices(db *couchdb.CouchDatabase) error {
	return nil
}

func (c *ConfigHistoryMgr) getDB(ledgerID string) (*couchdb.CouchDatabase, error) {
	if _, ok := c.dbs[ledgerID]; !ok {
		err := c.openStore(ledgerID)
		if err != nil {
			return nil, err
		}
	}
	return c.dbs[ledgerID], nil
}

func (c *ConfigHistoryMgr) prepareDBBatch(stateUpdates ledger.StateUpdates, committingBlock uint64, ledgerID string) error {
	var docs []*couchdb.CouchDoc
	lsccWrites := stateUpdates[lsccNamespace]
	for _, kv := range lsccWrites.([]*kvrwset.KVWrite) {
		if !privdata.IsCollectionConfigKey(kv.Key) {
			continue
		}
		key := encodeCompositeKey(lsccNamespace, kv.Key, committingBlock)
		doc, err := keyValueToCouchDoc(key, kv.Value, nil)
		if err != nil {
			return err
		}
		docs = append(docs, doc)
	}
	if len(docs) > 0 {
		db, err := c.getDB(ledgerID)
		if err != nil {
			return err
		}
		_, err = db.BatchUpdateDocuments(docs)
		if err != nil {
			return errors.WithMessage(err, fmt.Sprintf("writing config history data to CouchDB failed [%d]", committingBlock))
		}
	}
	return nil
}

func encodeCompositeKey(ns, key string, blockNum uint64) []byte {
	b := []byte(keyPrefix + ns)
	b = append(b, separatorByte)
	b = append(b, []byte(key)...)
	return append(b, encodeBlockNum(blockNum)...)
}

func encodeBlockNum(blockNum uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, math.MaxUint64-blockNum)
	return b
}
