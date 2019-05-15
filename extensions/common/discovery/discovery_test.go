/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package discovery

import (
	"testing"

	"github.com/hyperledger/fabric/extensions/mocks"
	gcommon "github.com/hyperledger/fabric/gossip/common"
	"github.com/stretchr/testify/assert"
)

const (
	channelID = "testchannel"

	ns1   = "chaincode1"
	ns2   = "chaincode2"
	coll1 = "collection1"
	coll2 = "collection2"
	key1  = "key1"
	key2  = "key2"
)

var (
	org1MSPID      = "Org1MSP"
	p1Org1Endpoint = "p1.org1.com"
	p1Org1PKIID    = gcommon.PKIidType("pkiid_P1O1")
	p2Org1Endpoint = "p2.org1.com"
	p2Org1PKIID    = gcommon.PKIidType("pkiid_P2O1")
	p3Org1Endpoint = "p3.org1.com"
	p3Org1PKIID    = gcommon.PKIidType("pkiid_P3O1")

	org2MSPID      = "Org2MSP"
	p1Org2Endpoint = "p1.org2.com"
	p1Org2PKIID    = gcommon.PKIidType("pkiid_P1O2")
	p2Org2Endpoint = "p2.org2.com"
	p2Org2PKIID    = gcommon.PKIidType("pkiid_P2O2")
	p3Org2Endpoint = "p3.org2.com"
	p3Org2PKIID    = gcommon.PKIidType("pkiid_P3O2")

	org3MSPID      = "Org3MSP"
	p1Org3Endpoint = "p1.org3.com"
	p1Org3PKIID    = gcommon.PKIidType("pkiid_P1O3")
	p2Org3Endpoint = "p2.org3.com"
	p2Org3PKIID    = gcommon.PKIidType("pkiid_P2O3")
	p3Org3Endpoint = "p3.org3.com"
	p3Org3PKIID    = gcommon.PKIidType("pkiid_P3O3")
)

func TestDiscovery(t *testing.T) {
	p1Org1 := mocks.NewMember(p1Org1Endpoint, p1Org1PKIID)
	p2Org1 := mocks.NewMember(p2Org1Endpoint, p2Org1PKIID)
	p3Org1 := mocks.NewMember(p3Org1Endpoint, p3Org1PKIID)
	p1Org2 := mocks.NewMember(p1Org2Endpoint, p1Org2PKIID)
	p2Org2 := mocks.NewMember(p2Org2Endpoint, p2Org2PKIID)
	p3Org2 := mocks.NewMember(p3Org2Endpoint, p3Org2PKIID)
	p1Org3 := mocks.NewMember(p1Org3Endpoint, p1Org3PKIID)
	p2Org3 := mocks.NewMember(p2Org3Endpoint, p2Org3PKIID)
	p3Org3 := mocks.NewMember(p3Org3Endpoint, p3Org3PKIID)
	pInvalid := mocks.NewMember("invalid", gcommon.PKIidType("invalid"))

	gossip := mocks.NewMockGossipAdapter().
		Self(org1MSPID, p1Org1).
		Member(org1MSPID, p2Org1).
		Member(org1MSPID, p3Org1).
		Member(org2MSPID, p1Org2).
		Member(org2MSPID, p2Org2).
		Member(org2MSPID, p3Org2).
		Member(org3MSPID, p1Org3).
		Member(org3MSPID, p2Org3).
		Member(org3MSPID, p3Org3).
		MemberWithNoPKIID(org3MSPID, pInvalid) // Should be ignored

	d := New(channelID, gossip)

	t.Run("ChannelID", func(t *testing.T) {
		assert.Equal(t, channelID, d.ChannelID())
	})

	t.Run("Self", func(t *testing.T) {
		s := d.Self()
		assert.NotNil(t, s)
		assert.Equal(t, p1Org1.Endpoint, s.Endpoint)
	})

	t.Run("GetMembers", func(t *testing.T) {
		f := func(m *Member) bool {
			e := m.Endpoint
			if e == p1Org1.Endpoint ||
				e == p2Org1.Endpoint ||
				e == p1Org2.Endpoint ||
				e == p3Org2.Endpoint ||
				e == p1Org3.Endpoint ||
				e == p3Org3.Endpoint {
				return true
			}
			return false
		}
		members := d.GetMembers(f)
		assert.Equal(t, 6, len(members))

		expectedEndpoints := []string{p1Org1Endpoint, p2Org1Endpoint, p1Org2Endpoint, p3Org2Endpoint, p1Org3Endpoint, p3Org3Endpoint}

		for _, m := range members {
			assert.True(t, contains(expectedEndpoints, m.Endpoint))
		}

		memberEndpoints := asEndpoints(members...)
		for _, e := range expectedEndpoints {
			assert.True(t, contains(memberEndpoints, e))
		}
	})
}

func contains(endpoints []string, endpoint string) bool {
	for _, e := range endpoints {
		if endpoint == e {
			return true
		}
	}
	return false
}

func asEndpoints(members ...*Member) []string {
	endpoints := make([]string, len(members))
	for i, m := range members {
		endpoints[i] = m.Endpoint
	}
	return endpoints
}
