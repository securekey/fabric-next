/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package validationinfo

import (
	"sync"

	"github.com/hyperledger/fabric/core/common/sysccprovider"
)

type validationInfo struct {
	cc     *sysccprovider.ChaincodeInstance
	vscc   *sysccprovider.ChaincodeInstance
	policy []byte
}

type valInfoRetriever func(channelID, ccID string) (cc *sysccprovider.ChaincodeInstance, vscc *sysccprovider.ChaincodeInstance, policy []byte, err error)

// Cache is a cache of validation info
type Cache struct {
	mutex            sync.RWMutex
	cache            map[string]*validationInfo
	valInfoRetriever valInfoRetriever
}

// NewCache creates a new validation info cache
func NewCache(valInfoRetriever valInfoRetriever) *Cache {
	return &Cache{
		cache:            make(map[string]*validationInfo),
		valInfoRetriever: valInfoRetriever,
	}
}

// Get returns the chaincode instance, VSCC instance and policy bytes for the given chaincode.
func (c *Cache) Get(channelID, ccID string) (*sysccprovider.ChaincodeInstance, *sysccprovider.ChaincodeInstance, []byte, error) {
	c.mutex.RLock()
	valInfo, ok := c.cache[ccID]
	c.mutex.RUnlock()

	if ok {
		return valInfo.cc, valInfo.vscc, valInfo.policy, nil
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	valInfo, ok = c.cache[ccID]
	if !ok {
		cc, vscc, policy, err := c.valInfoRetriever(channelID, ccID)
		if err != nil {
			return nil, nil, nil, err
		}
		valInfo = &validationInfo{cc: cc, vscc: vscc, policy: policy}
		c.cache[ccID] = valInfo
	}

	return valInfo.cc, valInfo.vscc, valInfo.policy, nil
}
