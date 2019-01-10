/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbconfighistory

import (
	"bytes"
	"fmt"
	"sync"

	"encoding/binary"
	"math"

	"encoding/hex"

	"strings"

	"strconv"

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
	sync.RWMutex
}

type retriever struct {
	ledgerInfoRetriever confighistory.LedgerInfoRetriever
	db                  *couchdb.CouchDatabase
}

// NewMgr instantiates a config history data storage provider backed by CouchDB
func NewMgr() (confighistory.Mgr, error) {
	logger.Warningf("constructing CouchDB config history data storage provider")
	couchDBDef := couchdb.GetCouchDBDefinition()
	couchInstance, err := couchdb.CreateCouchInstance(couchDBDef.URL, couchDBDef.Username, couchDBDef.Password,
		couchDBDef.MaxRetries, couchDBDef.MaxRetriesOnStartup, couchDBDef.RequestTimeout, couchDBDef.CreateGlobalChangesDB)
	if err != nil {
		return nil, errors.WithMessage(err, "obtaining CouchDB instance failed")
	}
	return &ConfigHistoryMgr{couchInstance: couchInstance, dbs: make(map[string]*couchdb.CouchDatabase)}, nil
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
	c.Lock()
	c.dbs[ledgerID] = db
	c.Unlock()
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
	c.Lock()
	c.dbs[ledgerID] = db
	c.Unlock()
	return nil
}

func (c *ConfigHistoryMgr) createConfigHistoryStoreIndices(db *couchdb.CouchDatabase) error {
	err := db.CreateNewIndexWithRetry(blockNumberCCNameIndexDef, blockNumberCCNameIndexDoc)
	if err != nil {
		return errors.WithMessage(err, "creation of block number and cc name index failed")
	}
	return nil
}

func (c *ConfigHistoryMgr) getDB(ledgerID string) (*couchdb.CouchDatabase, error) {
	c.RLock()
	db, ok := c.dbs[ledgerID]
	c.RUnlock()
	if ok {
		return db, nil
	}
	err := c.openStore(ledgerID)
	if err != nil {
		return nil, err
	}
	c.RLock()
	db = c.dbs[ledgerID]
	c.RUnlock()
	return db, nil
}

func (c *ConfigHistoryMgr) prepareDBBatch(stateUpdates ledger.StateUpdates, committingBlock uint64, ledgerID string) error {
	var docs []*couchdb.CouchDoc
	lsccWrites := stateUpdates[lsccNamespace]
	for _, kv := range lsccWrites.([]*kvrwset.KVWrite) {
		if !privdata.IsCollectionConfigKey(kv.Key) {
			continue
		}
		ccName := strings.Split(string(kv.Key), "~")[0]
		key := encodeCompositeKey(lsccNamespace, kv.Key, committingBlock)

		indices := map[string]string{blockNumberField: fmt.Sprintf("%064s", strconv.FormatUint(committingBlock, blockNumberBase)),
			ccNameField: ccName}

		doc, err := keyValueToCouchDoc(key, kv.Value, indices)
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
		_, err = db.CommitDocuments(docs)
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

func (r *retriever) mostRecentEntryBelow(blockNum uint64, ns, key, ccName string) (*compositeKV, error) {
	logger.Debugf("mostRecentEntryBelow() - {%s, %s, %d}", ns, key, blockNum)
	if blockNum == 0 {
		return nil, fmt.Errorf("blockNum should be greater than 0")
	}
	const queryFmt = `{
   "selector":{
      "` + blockNumberField + `":{
         "$lt":"%s"
      },
      "` + ccNameField + `":{
         "$eq":"%s"
      }
   },
   "limit":1,
   "sort":[
      {
         "` + blockNumberField + `":"desc"
      }
   ],
   "use_index":[
      "_design/` + blockNumberCCNameIndexDoc + `", "` + blockNumberCCNameIndexName + `"
   ]
}`

	results,_, err := r.db.QueryDocuments(fmt.Sprintf(queryFmt, fmt.Sprintf("%064s", strconv.FormatUint(blockNum, blockNumberBase)), ccName))
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		logger.Debugf("QueryDocuments return nil for blockNum %d ccName %s", blockNum, ccName)
		return nil, nil
	}

	compositeKey := encodeCompositeKey(ns, key, blockNum)

	value, err := docValueToConfigHistoryValue(results[0].Value)
	if err != nil {
		return nil, err
	}

	k, v := decodeCompositeKey(compositeKey), value
	return &compositeKV{k, v}, nil
}

func (r *retriever) entryAt(blockNum uint64, ns, key string) (*compositeKV, error) {
	logger.Debugf("entryAt() - {%s, %s, %d}", ns, key, blockNum)
	compositeKey := encodeCompositeKey(ns, key, blockNum)
	doc, _, err := r.db.ReadDoc(hex.EncodeToString(compositeKey))
	if err != nil {
		return nil, err
	}
	value, err := docValueToConfigHistoryValue(doc.JSONValue)
	if err != nil {
		return nil, err
	}
	k, v := decodeCompositeKey(compositeKey), value
	return &compositeKV{k, v}, nil
}

func decodeBlockNum(blockNumBytes []byte) uint64 {
	return math.MaxUint64 - binary.BigEndian.Uint64(blockNumBytes)
}

func encodeBlockNum(blockNum uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, math.MaxUint64-blockNum)
	return b
}
func decodeCompositeKey(b []byte) *compositeKey {
	blockNumStartIndex := len(b) - 8
	nsKeyBytes, blockNumBytes := b[1:blockNumStartIndex], b[blockNumStartIndex:]
	separatorIndex := bytes.Index(nsKeyBytes, []byte{separatorByte})
	ns, key := nsKeyBytes[0:separatorIndex], nsKeyBytes[separatorIndex+1:]
	return &compositeKey{string(ns), string(key), decodeBlockNum(blockNumBytes)}
}
