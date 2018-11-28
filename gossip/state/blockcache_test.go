/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"github.com/hyperledger/fabric/protos/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"testing"
)

func TestBlockCache(t *testing.T) {
	cache := newBlockCache()

	cache.Add(newMockBlock(10))
	cache.Add(newMockBlock(11))
	cache.Add(newMockBlock(13))

	assert.Equal(t, 3, cache.Size())

	b := cache.Remove(10)
	require.NotNil(t, b)
	assert.Equal(t, uint64(10), b.Header.Number)
	assert.Equal(t, 2, cache.Size())

	b = cache.Remove(13) // Should also remove 11
	require.NotNil(t, b)
	assert.Equal(t, uint64(13), b.Header.Number)
	assert.Equal(t, 0, cache.Size())
}

func newMockBlock(num uint64) *common.Block {
	return &common.Block{
		Header: &common.BlockHeader{
			Number: num,
		},
	}
}
