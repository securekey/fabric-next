/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package discovery

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPermutations(t *testing.T) {
	p1Org1 := newMember(org1MSPID, p1Org1Endpoint, false)
	p2Org1 := newMember(org1MSPID, p2Org1Endpoint, false)
	p3Org1 := newMember(org1MSPID, p3Org1Endpoint, false)

	p1Org2 := newMember(org2MSPID, p1Org2Endpoint, false)
	p2Org2 := newMember(org2MSPID, p2Org2Endpoint, false)

	p1Org3 := newMember(org3MSPID, p1Org3Endpoint, false)
	p2Org3 := newMember(org3MSPID, p2Org3Endpoint, false)

	t.Run("1 Of", func(t *testing.T) {
		perm := NewPermutations().
			Groups(NewPeerGroup(p2Org1, p1Org1))

		peerGroups := perm.Evaluate().Sort()
		require.Equal(t, 2, len(peerGroups))

		t.Logf("%s", peerGroups)

		assert.Equal(t, asEndpoints(p1Org1), asEndpoints(peerGroups[0]...))
		assert.Equal(t, asEndpoints(p2Org1), asEndpoints(peerGroups[1]...))
	})

	t.Run("2 Of", func(t *testing.T) {
		perm := NewPermutations().
			Groups(
				NewPeerGroup(p1Org2, p2Org2),
				NewPeerGroup(p2Org1, p1Org1),
			)

		peerGroups := perm.Evaluate().Sort()
		require.Equal(t, 4, len(peerGroups))

		t.Logf("%s", peerGroups)

		assert.Equal(t, asEndpoints(p1Org1, p1Org2), asEndpoints(peerGroups[0]...))
		assert.Equal(t, asEndpoints(p1Org1, p2Org2), asEndpoints(peerGroups[1]...))
		assert.Equal(t, asEndpoints(p1Org2, p2Org1), asEndpoints(peerGroups[2]...))
		assert.Equal(t, asEndpoints(p2Org1, p2Org2), asEndpoints(peerGroups[3]...))
	})

	t.Run("3 Of", func(t *testing.T) {
		perm := NewPermutations().
			Groups(NewPeerGroup(p1Org2)).
			Groups(NewPeerGroup(p2Org1, p1Org1, p3Org1)).
			Groups(NewPeerGroup(p2Org3, p1Org3))

		peerGroups := perm.Evaluate().Sort()
		require.Equal(t, 6, len(peerGroups))

		t.Logf("%s", peerGroups)

		assert.Equal(t, asEndpoints(p1Org1, p1Org2, p1Org3), asEndpoints(peerGroups[0]...))
		assert.Equal(t, asEndpoints(p1Org1, p1Org2, p2Org3), asEndpoints(peerGroups[1]...))
		assert.Equal(t, asEndpoints(p1Org2, p1Org3, p2Org1), asEndpoints(peerGroups[2]...))
		assert.Equal(t, asEndpoints(p1Org2, p1Org3, p3Org1), asEndpoints(peerGroups[3]...))
		assert.Equal(t, asEndpoints(p1Org2, p2Org1, p2Org3), asEndpoints(peerGroups[4]...))
		assert.Equal(t, asEndpoints(p1Org2, p2Org3, p3Org1), asEndpoints(peerGroups[5]...))
	})

}

func NewPeerGroup(members ...*Member) PeerGroup {
	var peerGroup PeerGroup
	for _, member := range members {
		peerGroup = append(peerGroup, member)
	}
	return peerGroup
}
