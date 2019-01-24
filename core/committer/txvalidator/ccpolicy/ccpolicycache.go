/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ccpolicy

import (
	"sync"

	"github.com/hyperledger/fabric/common/policies"
)

type policyRetriever func(policyBytes []byte) (policies.Policy, error)

// Cache is a cache of chaincode policies
type Cache struct {
	mutex           sync.RWMutex
	cache           map[string]policies.Policy
	policyRetriever policyRetriever
}

// NewCache creates a new chaincode policy cache
func NewCache(policyRetriever policyRetriever) *Cache {
	return &Cache{
		cache:           make(map[string]policies.Policy),
		policyRetriever: policyRetriever,
	}
}

// Get returns the chaincode policy for the given policy bytes.
func (c *Cache) Get(policyBytes []byte) (policies.Policy, error) {
	key := string(policyBytes)

	c.mutex.RLock()
	policy, ok := c.cache[key]
	c.mutex.RUnlock()

	if ok {
		return policy, nil
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	policy, ok = c.cache[key]
	if !ok {
		var err error
		policy, err = c.policyRetriever(policyBytes)
		if err != nil {
			return nil, err
		}
		c.cache[key] = policy
	}

	return policy, nil
}
