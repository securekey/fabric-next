/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package discovery

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPeerGroupsSort(t *testing.T) {
	pg1 := PeerGroup{p2, p1, p3}
	pg2 := PeerGroup{p5, p4}
	pg3 := PeerGroup{p7, p6}

	pgs := PeerGroups{pg3, pg1, pg2}

	pgs.Sort()

	// The peer groups should be sorted
	assert.Equal(t, pg1, pgs[0])
	assert.Equal(t, pg2, pgs[1])
	assert.Equal(t, pg3, pgs[2])

	// Each peer group should be sorted
	assert.Equal(t, p1, pg1[0])
	assert.Equal(t, p2, pg1[1])
	assert.Equal(t, p3, pg1[2])

	assert.Equal(t, p4, pg2[0])
	assert.Equal(t, p5, pg2[1])

	assert.Equal(t, p6, pg3[0])
	assert.Equal(t, p7, pg3[1])
}

func TestPeerGroupsContains(t *testing.T) {
	pg1 := PeerGroup{p1, p2, p3}
	pg2 := PeerGroup{p3, p4, p5}
	pg3 := PeerGroup{p1, p4}
	pg4 := PeerGroup{p2, p5, p6}
	pg5 := PeerGroup{p6, p7}

	pgs1 := PeerGroups{pg1, pg2}

	assert.True(t, pgs1.Contains(p1))
	assert.True(t, pgs1.Contains(p2))
	assert.True(t, pgs1.Contains(p3))
	assert.True(t, pgs1.Contains(p4))
	assert.True(t, pgs1.Contains(p5))
	assert.False(t, pgs1.Contains(p6))

	assert.True(t, pgs1.ContainsAll(pg3))
	assert.False(t, pgs1.ContainsAll(pg4))
	assert.True(t, pgs1.ContainsAny(pg4))
	assert.False(t, pgs1.ContainsAny(pg5))
}
