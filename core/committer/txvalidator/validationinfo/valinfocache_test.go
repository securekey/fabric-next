/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package validationinfo

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/hyperledger/fabric/core/common/sysccprovider"
	"github.com/stretchr/testify/assert"
)

func TestCache(t *testing.T) {
	channelID := "testchannel"
	cc1 := "c1"
	cc2 := "c2"

	timesCalled := 0

	retriever := func(channelID, ccID string) (*sysccprovider.ChaincodeInstance, *sysccprovider.ChaincodeInstance, []byte, error) {
		timesCalled++
		return nil, nil, []byte(ccID), nil
	}

	c := NewCache(retriever)

	_, _, policy1, err := c.Get(channelID, cc1)
	assert.NoError(t, err)
	assert.NotNil(t, policy1)
	assert.Equal(t, 1, timesCalled)

	_, _, policy2, err := c.Get(channelID, cc1)
	assert.NoError(t, err)
	assert.Equal(t, policy1, policy2)
	assert.Equal(t, 1, timesCalled)

	_, _, policy3, err := c.Get(channelID, cc2)
	assert.NoError(t, err)
	assert.NotEqual(t, policy2, policy3)
	assert.Equal(t, 2, timesCalled)
}

func TestConcurrency(t *testing.T) {
	channelID := "testchannel"
	cc := []string{"cc1", "cc2", "cc3"}

	var timesCalled uint32

	retriever := func(channelID, ccID string) (*sysccprovider.ChaincodeInstance, *sysccprovider.ChaincodeInstance, []byte, error) {
		atomic.AddUint32(&timesCalled, 1)
		return nil, nil, []byte(ccID), nil
	}

	c := NewCache(retriever)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			for j := 0; j < 1000; j++ {
				c.Get(channelID, cc[j%len(cc)])
			}
			wg.Done()
		}()
	}
	wg.Wait()

	assert.Equal(t, uint32(3), atomic.LoadUint32(&timesCalled))
}
