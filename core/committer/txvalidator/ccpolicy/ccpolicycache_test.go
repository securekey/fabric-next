/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ccpolicy

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/hyperledger/fabric/common/policies"
	cb "github.com/hyperledger/fabric/protos/common"
	"github.com/stretchr/testify/assert"
)

func TestCache(t *testing.T) {
	cc1 := "c1"
	cc2 := "c2"

	timesCalled := 0

	retriever := func(policyBytes []byte) (policies.Policy, error) {
		timesCalled++
		return &mockPolicy{policyBytes: policyBytes}, nil
	}

	c := NewCache(retriever)

	policy1, err := c.Get([]byte(cc1))
	assert.NoError(t, err)
	assert.NotNil(t, policy1)
	assert.Equal(t, 1, timesCalled)

	policy2, err := c.Get([]byte(cc1))
	assert.NoError(t, err)
	assert.Equal(t, policy1, policy2)
	assert.Equal(t, 1, timesCalled)

	policy3, err := c.Get([]byte(cc2))
	assert.NoError(t, err)
	assert.NotEqual(t, policy2, policy3)
	assert.Equal(t, 2, timesCalled)
}

func TestConcurrency(t *testing.T) {
	cc := [][]byte{[]byte("cc1"), []byte("cc2"), []byte("cc3")}

	var timesCalled uint32

	retriever := func(policyBytes []byte) (policies.Policy, error) {
		atomic.AddUint32(&timesCalled, 1)
		return &mockPolicy{policyBytes: policyBytes}, nil
	}

	c := NewCache(retriever)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < 1000; j++ {
				c.Get(cc[j%len(cc)])
			}
			wg.Done()
		}()
	}
	wg.Wait()

	assert.Equal(t, uint32(3), atomic.LoadUint32(&timesCalled))
}

type mockPolicy struct {
	policyBytes []byte
}

func (_ *mockPolicy) Evaluate(signatureSet []*cb.SignedData) error {
	return nil
}
