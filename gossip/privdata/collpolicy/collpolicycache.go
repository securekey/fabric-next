/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package collpolicy

import (
	"sync"

	"github.com/hyperledger/fabric/core/common/privdata"
)

type collKey struct {
	namespace string
	collName  string
}

type policyRetriever func(channelID, namespace, col string) privdata.CollectionAccessPolicy

// Cache is a cache of collection policies
type Cache struct {
	channelID       string
	mutex           sync.RWMutex
	cache           map[collKey]privdata.CollectionAccessPolicy
	policyRetriever policyRetriever
}

// NewCache creates a new collection policy cache
func NewCache(channelID string, policyRetriever policyRetriever) *Cache {
	return &Cache{
		channelID:       channelID,
		cache:           make(map[collKey]privdata.CollectionAccessPolicy),
		policyRetriever: policyRetriever,
	}
}

// Get returns the collection policy for the given namespace and collection name.
// nil is returned if the collection policy could not be retrieved.
func (c *Cache) Get(namespace, collName string) privdata.CollectionAccessPolicy {
	key := collKey{
		namespace: namespace,
		collName:  collName,
	}

	c.mutex.RLock()
	policy, ok := c.cache[key]
	c.mutex.RUnlock()

	if ok {
		return policy
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	policy, ok = c.cache[key]
	if !ok {
		policy = c.policyRetriever(c.channelID, namespace, collName)
		c.cache[key] = policy
	}

	return policy
}
