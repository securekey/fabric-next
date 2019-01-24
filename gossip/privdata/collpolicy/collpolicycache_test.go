/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package collpolicy

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/hyperledger/fabric/core/common/privdata"
	"github.com/stretchr/testify/assert"
)

func TestCache(t *testing.T) {
	namespace1 := "n1"
	coll1 := "c1"
	coll2 := "c2"

	timesCalled := 0

	retriever := func(channelID, namespace, coll string) privdata.CollectionAccessPolicy {
		timesCalled++
		return &mockPolicy{namespace: namespace, coll: coll}
	}

	c := NewCache("testChannel", retriever)

	policy1 := c.Get(namespace1, coll1)
	assert.NotNil(t, policy1)
	assert.Equal(t, 1, timesCalled)

	policy2 := c.Get(namespace1, coll1)
	assert.Equal(t, policy1, policy2)
	assert.Equal(t, 1, timesCalled)

	policy3 := c.Get(namespace1, coll2)
	assert.NotEqual(t, policy2, policy3)
	assert.Equal(t, 2, timesCalled)
}

func TestConcurrency(t *testing.T) {
	namespace := "n1"
	coll := []string{"col1", "col2", "col3"}

	var timesCalled uint32

	retriever := func(channelID, namespace, coll string) privdata.CollectionAccessPolicy {
		atomic.AddUint32(&timesCalled, 1)
		return &mockPolicy{namespace: namespace, coll: coll}
	}

	c := NewCache("testChannel", retriever)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < 1000; j++ {
				c.Get(namespace, coll[j%len(coll)])
			}
			wg.Done()
		}()
	}
	wg.Wait()

	assert.Equal(t, uint32(3), atomic.LoadUint32(&timesCalled))
}

type mockPolicy struct {
	namespace string
	coll      string
}

func (_ *mockPolicy) AccessFilter() privdata.Filter {
	return nil
}

func (_ *mockPolicy) RequiredPeerCount() int {
	return 0
}

func (_ *mockPolicy) MaximumPeerCount() int {
	return 0
}

func (_ *mockPolicy) MemberOrgs() []string {
	return nil
}
