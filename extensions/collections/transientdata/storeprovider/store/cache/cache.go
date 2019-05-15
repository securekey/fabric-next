/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cache

import (
	"fmt"
	"time"

	"github.com/bluele/gcache"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/extensions/collections/transientdata/storeprovider/store/api"
	"github.com/hyperledger/fabric/extensions/config"
)

var logger = flogging.MustGetLogger("memtransientdatastore")

// Cache is an in-memory key-value cache
type Cache struct {
	cache   gcache.Cache
	ticker  *time.Ticker
	dbstore transientDB
}

// transientDB - an interface for persisting and retrieving keys
type transientDB interface {
	AddKey(api.Key, *api.Value) error
	DeleteExpiredKeys() error
	GetKey(key api.Key) (*api.Value, error)
	Close()
}

// New return a new in-memory key-value cache
func New(size int, dbstore transientDB) *Cache {
	cache := gcache.New(size).LoaderExpireFunc(func(key interface{}) (interface{}, *time.Duration, error) {
		logger.Debugf("LoaderExpireFunc for key %s", key)
		value, err := dbstore.GetKey(key.(api.Key))
		if value == nil || err != nil {
			if err != nil {
				logger.Error(err.Error())
			}
			logger.Debugf("Key [%s] not found in DB", key)
			return nil, nil, gcache.KeyNotFoundError
		}
		isExpired, diff := checkExpiryTime(value.ExpiryTime)
		if isExpired {
			logger.Debugf("Key [%s] from DB has expired", key)
			return nil, nil, gcache.KeyNotFoundError
		}
		logger.Debugf("Loaded key [%s] from DB", key)
		return value, &diff, nil

	}).
		EvictedFunc(func(key, value interface{}) {
			logger.Debugf("EvictedFunc for key %s", key)
			if value != nil {
				k := key.(api.Key)
				v := value.(*api.Value)
				isExpired, _ := checkExpiryTime(v.ExpiryTime)
				if !isExpired {
					dbstoreErr := dbstore.AddKey(k, v)
					if dbstoreErr != nil {
						logger.Error(dbstoreErr.Error())
					} else {
						logger.Debugf("Key [%s] offloaded to DB", key)
					}
				}
			}

		}).ARC().Build()

	// cleanup expired data in db
	ticker := time.NewTicker(config.GetTransientDataExpiredIntervalTime())
	go func() {
		for range ticker.C {
			dbstoreErr := dbstore.DeleteExpiredKeys()
			if dbstoreErr != nil {
				logger.Error(dbstoreErr.Error())
			}
		}
	}()

	return &Cache{
		cache:   cache,
		ticker:  ticker,
		dbstore: dbstore,
	}
}

// Close closes the cache
func (c *Cache) Close() {
	c.cache.Purge()
	c.ticker.Stop()
}

// Put adds the transient value for the given key.
// Returns the previous value (if any)
func (c *Cache) Put(key api.Key, value []byte, txID string) {
	if err := c.cache.Set(key,
		&api.Value{
			Value: value,
			TxID:  txID,
		}); err != nil {
		panic("Set must never return an error")
	}
}

// PutWithExpire adds the transient value for the given key.
// Returns the previous value (if any)
func (c *Cache) PutWithExpire(key api.Key, value []byte, txID string, expiry time.Duration) {
	if err := c.cache.SetWithExpire(key,
		&api.Value{
			Value:      value,
			TxID:       txID,
			ExpiryTime: time.Now().UTC().Add(expiry),
		}, expiry); err != nil {
		panic("Set must never return an error")
	}
}

// Get returns the transient value for the given key
func (c *Cache) Get(key api.Key) *api.Value {
	value, err := c.cache.Get(key)
	if err != nil {
		if err != gcache.KeyNotFoundError {
			panic(fmt.Sprintf("Get must never return an error other than KeyNotFoundError err:%s", err))
		}
		return nil
	}

	return value.(*api.Value)
}

func checkExpiryTime(expiryTime time.Time) (bool, time.Duration) {
	if expiryTime.IsZero() {
		return false, 0
	}
	timeNow := time.Now().UTC()
	logger.Debugf("time now %s", timeNow)
	logger.Debugf("expiry time %s", expiryTime)
	diff := expiryTime.Sub(timeNow)
	logger.Debugf("diff time %s", diff)

	if diff <= 0 {
		return true, diff
	}
	return false, diff
}
