/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package discovery

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	pg1 = PeerGroup{}
	pg2 = PeerGroup{}
	pg3 = PeerGroup{}
	pg4 = PeerGroup{}
)

func TestIterator(t *testing.T) {

	peerGroups := PeerGroups{pg1, pg2, pg3, pg4}

	startingIndex := 2

	it := NewIterator(peerGroups, startingIndex)

	assert.Equal(t, pg3, it.Next())
	assert.Equal(t, pg4, it.Next())
	assert.Equal(t, pg1, it.Next())
	assert.Equal(t, pg2, it.Next())
	assert.Nil(t, it.Next())
}
