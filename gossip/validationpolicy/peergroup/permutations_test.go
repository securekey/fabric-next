/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package peergroup

import (
	"testing"

	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/gossip/discovery"
	"github.com/hyperledger/fabric/gossip/roleutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	org1MSPID      = "Org1MSP"
	p1Org1Endpoint = "p1.org1.com"
	p1Org1PKIID    = common.PKIidType("pkiid_P1O1")
	p2Org1Endpoint = "p2.org1.com"
	p2Org1PKIID    = common.PKIidType("pkiid_P2O1")
	p3Org1Endpoint = "p3.org1.com"
	p3Org1PKIID    = common.PKIidType("pkiid_P3O1")

	org2MSPID      = "Org2MSP"
	p1Org2Endpoint = "p1.org2.com"
	p1Org2PKIID    = common.PKIidType("pkiid_P1O2")
	p2Org2Endpoint = "p2.org2.com"
	p2Org2PKIID    = common.PKIidType("pkiid_P2O2")

	org3MSPID      = "Org3MSP"
	p1Org3Endpoint = "p1.org3.com"
	p1Org3PKIID    = common.PKIidType("pkiid_P1O3")
	p2Org3Endpoint = "p2.org3.com"
	p2Org3PKIID    = common.PKIidType("pkiid_P2O3")

	validatorRole = string(ledgerconfig.ValidatorRole)
	endorserRole  = string(ledgerconfig.EndorserRole)
)

func TestPermutations(t *testing.T) {
	p1Org1 := NewMember(org1MSPID, p1Org1Endpoint)
	p2Org1 := NewMember(org1MSPID, p2Org1Endpoint)
	p3Org1 := NewMember(org1MSPID, p3Org1Endpoint)

	p1Org2 := NewMember(org2MSPID, p1Org2Endpoint)
	p2Org2 := NewMember(org2MSPID, p2Org2Endpoint)

	p1Org3 := NewMember(org3MSPID, p1Org3Endpoint)
	p2Org3 := NewMember(org3MSPID, p2Org3Endpoint)

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

func NewPeerGroup(members ...*roleutil.Member) PeerGroup {
	var peerGroup PeerGroup
	for _, member := range members {
		peerGroup = append(peerGroup, member)
	}
	return peerGroup
}

func NewMember(mspID, endpoint string) *roleutil.Member {
	return &roleutil.Member{
		NetworkMember: discovery.NetworkMember{
			Endpoint: endpoint,
		},
	}
}

func asEndpoints(members ...*roleutil.Member) []string {
	var endpoints []string
	for _, member := range members {
		endpoints = append(endpoints, member.Endpoint)
	}
	return endpoints
}
