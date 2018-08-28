/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package leveldbhelper

import (
	"bytes"
	"sync"

	"fmt"
	"strings"

	"github.com/hyperledger/fabric/common/metrics"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/uber-go/tally"
)

var writeBatchTimer tally.Timer

func init() {
	writeBatchTimer = metrics.RootScope.Timer("leveldbhelper_WriteBatch_time_seconds")
}

var dbNameKeySep = []byte{0x00}
var lastKeyIndicator = byte(0x01)

// Provider enables to use a single leveldb as multiple logical leveldbs
type Provider struct {
	db        *DB
	dbHandles map[string]*DBHandle
	mux       sync.Mutex
	dbPath    string
}

// NewProvider constructs a Provider
func NewProvider(conf *Conf) *Provider {
	db := CreateDB(conf)
	db.Open()
	return &Provider{db, make(map[string]*DBHandle), sync.Mutex{}, conf.DBPath}
}

// GetDBHandle returns a handle to a named db
func (p *Provider) GetDBHandle(dbName string) *DBHandle {
	p.mux.Lock()
	defer p.mux.Unlock()
	dbHandle := p.dbHandles[dbName]
	if dbHandle == nil {
		dbHandle = &DBHandle{dbName, p.db, p.dbPath}
		p.dbHandles[dbName] = dbHandle
	}
	return dbHandle
}

// Close closes the underlying leveldb
func (p *Provider) Close() {
	p.db.Close()
}

// DBHandle is an handle to a named db
type DBHandle struct {
	dbName string
	db     *DB
	dbPath string
}

// Get returns the value for the given key
func (h *DBHandle) Get(key []byte) ([]byte, error) {
	return h.db.Get(constructLevelKey(h.dbName, key))
}

// Put saves the key/value
func (h *DBHandle) Put(key []byte, value []byte, sync bool) error {
	return h.db.Put(constructLevelKey(h.dbName, key), value, sync)
}

// Delete deletes the given key
func (h *DBHandle) Delete(key []byte, sync bool) error {
	return h.db.Delete(constructLevelKey(h.dbName, key), sync)
}

// WriteBatch writes a batch in an atomic way
func (h *DBHandle) WriteBatch(batch *UpdateBatch, sync bool) error {
	stopWatch := writeBatchTimer.Start()
	defer stopWatch.Stop()
	levelBatch := &leveldb.Batch{}
	for k, v := range batch.KVs {
		key := constructLevelKey(h.dbName, []byte(k))
		if v == nil {
			levelBatch.Delete(key)
		} else {
			levelBatch.Put(key, v)
		}
	}
	if err := h.db.WriteBatch(levelBatch, sync); err != nil {
		return err
	}
	return nil
}

// GetIterator gets an handle to iterator. The iterator should be released after the use.
// The resultset contains all the keys that are present in the db between the startKey (inclusive) and the endKey (exclusive).
// A nil startKey represents the first available key and a nil endKey represent a logical key after the last available key
func (h *DBHandle) GetIterator(startKey []byte, endKey []byte) *Iterator {
	dbPath := h.dbPath
	if strings.Contains(dbPath, "ledgersData/") {
		dbPath = strings.Replace(strings.Split(dbPath, "ledgersData/")[1], "/", "_", -1)
	} else {
		dbPath = strings.Replace(dbPath, "/", "_", -1)
	}
	ccTimer := metrics.RootScope.Timer(strings.Replace(fmt.Sprintf("leveldb_GetIterator_%s_%s_processing_time_seconds", h.dbName, dbPath), "/", "_", -1))
	stopwatch := ccTimer.Start()
	sKey := constructLevelKey(h.dbName, startKey)
	eKey := constructLevelKey(h.dbName, endKey)
	if endKey == nil {
		// replace the last byte 'dbNameKeySep' by 'lastKeyIndicator'
		eKey[len(eKey)-1] = lastKeyIndicator
	}
	logger.Debugf("Getting iterator %s for range [%#v] - [%#v]\n", dbPath, sKey, eKey)
	return &Iterator{h.db.GetIterator(sKey, eKey), stopwatch}
}

// UpdateBatch encloses the details of multiple `updates`
type UpdateBatch struct {
	KVs map[string][]byte
}

// NewUpdateBatch constructs an instance of a Batch
func NewUpdateBatch() *UpdateBatch {
	return &UpdateBatch{make(map[string][]byte)}
}

// Put adds a KV
func (batch *UpdateBatch) Put(key []byte, value []byte) {
	if value == nil {
		panic("Nil value not allowed")
	}
	batch.KVs[string(key)] = value
}

// Delete deletes a Key and associated value
func (batch *UpdateBatch) Delete(key []byte) {
	batch.KVs[string(key)] = nil
}

// Iterator extends actual leveldb iterator
type Iterator struct {
	iterator.Iterator
	stopwatch tally.Stopwatch
}

// Key wraps actual leveldb iterator method
func (itr *Iterator) Key() []byte {
	return retrieveAppKey(itr.Iterator.Key())
}

// Key wraps actual leveldb iterator method
func (itr *Iterator) Release() {
	defer itr.stopwatch.Stop()
	itr.Iterator.Release()
}

func constructLevelKey(dbName string, key []byte) []byte {
	return append(append([]byte(dbName), dbNameKeySep...), key...)
}

func retrieveAppKey(levelKey []byte) []byte {
	return bytes.SplitN(levelKey, dbNameKeySep, 2)[1]
}
