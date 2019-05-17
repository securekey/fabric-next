/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mocks

import (
	"sync"

	"github.com/hyperledger/fabric/extensions/collections/offledger/storeprovider/store/api"
)

// DBProvider is a mock DB provider
type DBProvider struct {
	mutex sync.Mutex
	dbs   map[string]*DB
	err   error
}

// NewDBProvider returns a new mock DB provider
func NewDBProvider() *DBProvider {
	return &DBProvider{
		dbs: make(map[string]*DB),
	}
}

// WithValue sets a value for the given key
func (m *DBProvider) WithValue(ns, coll, key string, value *api.Value) *DBProvider {
	m.MockDB(ns, coll).WithValue(key, value)
	return m
}

// WithError simulates an error on the provider
func (m *DBProvider) WithError(err error) *DBProvider {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.err = err
	return m
}

// MockDB is a mock database
func (m *DBProvider) MockDB(ns, coll string) *DB {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	dbKey := ns + "_" + coll
	db, ok := m.dbs[dbKey]
	if !ok {
		db = newMockDB()
		m.dbs[dbKey] = db
	}
	return db
}

// GetDB returns a mock DB for the given namespace/collection
func (m *DBProvider) GetDB(ns, coll string) (api.DB, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.MockDB(ns, coll), nil
}

// Close currently does nothing
func (m *DBProvider) Close() {
}

// DB implements a mock DB
type DB struct {
	mutex sync.RWMutex
	data  map[string]*api.Value
	err   error
}

func newMockDB() *DB {
	return &DB{
		data: make(map[string]*api.Value),
	}
}

// WithValue sets a value for the given key
func (m *DB) WithValue(key string, value *api.Value) *DB {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.data[key] = value
	return m
}

// WithError simulates an error on the DB
func (m *DB) WithError(err error) *DB {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	m.err = err
	return m
}

// Put sets the given values
func (m *DB) Put(keyVals ...*api.KeyValue) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.err != nil {
		return m.err
	}

	for _, kv := range keyVals {
		m.data[kv.Key] = kv.Value
	}

	return nil
}

// Get retrieves the value for the given key
func (m *DB) Get(key string) (*api.Value, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	return m.data[key], m.err
}

// GetMultiple retrieves multiple keys at once
func (m *DB) GetMultiple(keys ...string) ([]*api.Value, error) {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	values := make([]*api.Value, len(keys))
	for i, k := range keys {
		values[i] = m.data[k]
	}
	return values, m.err
}

// DeleteExpiredKeys currently does nothing
func (m *DB) DeleteExpiredKeys() error {
	m.mutex.RLock()
	defer m.mutex.RUnlock()
	return m.err
}
