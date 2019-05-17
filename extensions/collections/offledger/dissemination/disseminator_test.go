/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package dissemination

import (
	"os"
	"testing"

	"github.com/hyperledger/fabric/extensions/common"
	"github.com/hyperledger/fabric/extensions/mocks"
	"github.com/hyperledger/fabric/extensions/roles"
	gcommon "github.com/hyperledger/fabric/gossip/common"
	cb "github.com/hyperledger/fabric/protos/common"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	ns1   = "chaincode1"
	ns2   = "chaincode2"
	coll1 = "collection1"
	coll2 = "collection2"
	key1  = "key1"
	key2  = "key2"

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

	org4MSPID      = "Org4MSP"
	p1Org4Endpoint = "p1.org4.com"
	p1Org4PKIID    = gcommon.PKIidType("pkiid_P1O4")
	p2Org4Endpoint = "p2.org4.com"
	p2Org4PKIID    = gcommon.PKIidType("pkiid_P2O4")
	p3Org4Endpoint = "p3.org4.com"
	p3Org4PKIID    = gcommon.PKIidType("pkiid_P3O4")

	committerRole = string(roles.CommitterRole)
	endorserRole  = string(roles.EndorserRole)
	validatorRole = string(roles.ValidatorRole)
)

func TestDisseminator_ResolvePeersForDissemination(t *testing.T) {
	channelID := "testchannel"

	gossip := mocks.NewMockGossipAdapter().
		Self(org1MSPID, mocks.NewMember(p1Org1Endpoint, p1Org1PKIID)).
		Member(org1MSPID, mocks.NewMember(p2Org1Endpoint, p2Org1PKIID, committerRole)).
		Member(org1MSPID, mocks.NewMember(p3Org1Endpoint, p3Org1PKIID, endorserRole)).
		Member(org2MSPID, mocks.NewMember(p1Org2Endpoint, p1Org2PKIID, endorserRole)).
		Member(org2MSPID, mocks.NewMember(p2Org2Endpoint, p2Org2PKIID, committerRole)).
		Member(org2MSPID, mocks.NewMember(p3Org2Endpoint, p3Org2PKIID, endorserRole)).
		Member(org3MSPID, mocks.NewMember(p1Org3Endpoint, p1Org3PKIID, endorserRole)).
		Member(org3MSPID, mocks.NewMember(p2Org3Endpoint, p2Org3PKIID, committerRole)).
		Member(org3MSPID, mocks.NewMember(p3Org3Endpoint, p3Org3PKIID, endorserRole)).
		Member(org4MSPID, mocks.NewMember(p1Org4Endpoint, p1Org4PKIID, committerRole)).
		Member(org4MSPID, mocks.NewMember(p2Org4Endpoint, p2Org4PKIID, endorserRole))

	t.Run("Enough committers", func(t *testing.T) {
		d := New(channelID, ns1, coll1,
			&mocks.MockAccessPolicy{
				ReqPeerCount: 1,
				Orgs:         []string{org1MSPID, org2MSPID, org3MSPID},
			}, gossip)

		peers := d.resolvePeersForDissemination()
		require.Equal(t, 4, len(peers))

		peersStr := peers.String()

		assert.Contains(t, peersStr, p1Org1Endpoint)
		assert.Contains(t, peersStr, p2Org1Endpoint)
		assert.Contains(t, peersStr, p2Org2Endpoint)
		assert.Contains(t, peersStr, p2Org3Endpoint)
	})

	t.Run("Not enough committers", func(t *testing.T) {
		d := New(channelID, ns1, coll1,
			&mocks.MockAccessPolicy{
				ReqPeerCount: 1,
				MaxPeerCount: 7,
				Orgs:         []string{org1MSPID, org2MSPID, org3MSPID},
			}, gossip)

		peers := d.resolvePeersForDissemination()
		require.Equal(t, 7, len(peers))

		var numCommitters int
		for _, p := range peers {
			if p.HasRole(roles.CommitterRole) {
				numCommitters++
			}
		}
		assert.Equal(t, 4, numCommitters)
		assert.NotContains(t, peers.String(), "org4")
	})

	t.Run("Not enough committers and endorsers", func(t *testing.T) {
		d := New(channelID, ns1, coll1,
			&mocks.MockAccessPolicy{
				ReqPeerCount: 1,
				MaxPeerCount: 20,
				Orgs:         []string{org1MSPID, org2MSPID, org3MSPID},
			}, gossip)

		peers := d.resolvePeersForDissemination()
		require.Equal(t, 9, len(peers))
		assert.NotContains(t, peers.String(), "org4")
	})
}

func TestDisseminator_ResolvePeersForRetrieval(t *testing.T) {
	channelID := "testchannel"

	gossip := mocks.NewMockGossipAdapter().
		Self(org1MSPID, mocks.NewMember(p1Org1Endpoint, p1Org1PKIID)).
		Member(org1MSPID, mocks.NewMember(p2Org1Endpoint, p2Org1PKIID, committerRole)).
		Member(org1MSPID, mocks.NewMember(p3Org1Endpoint, p3Org1PKIID, endorserRole)).
		Member(org2MSPID, mocks.NewMember(p1Org2Endpoint, p1Org2PKIID, endorserRole)).
		Member(org2MSPID, mocks.NewMember(p2Org2Endpoint, p2Org2PKIID, committerRole)).
		Member(org2MSPID, mocks.NewMember(p3Org2Endpoint, p3Org2PKIID, endorserRole)).
		Member(org3MSPID, mocks.NewMember(p1Org3Endpoint, p1Org3PKIID, endorserRole)).
		Member(org3MSPID, mocks.NewMember(p2Org3Endpoint, p2Org3PKIID, committerRole)).
		Member(org3MSPID, mocks.NewMember(p3Org3Endpoint, p3Org3PKIID, endorserRole)).
		Member(org4MSPID, mocks.NewMember(p1Org4Endpoint, p1Org4PKIID, committerRole)).
		Member(org4MSPID, mocks.NewMember(p2Org4Endpoint, p2Org4PKIID, endorserRole))

	t.Run("Enough endorsers", func(t *testing.T) {
		d := New(channelID, ns1, coll1,
			&mocks.MockAccessPolicy{
				ReqPeerCount: 1,
				Orgs:         []string{org1MSPID, org2MSPID, org3MSPID},
			}, gossip)

		peers := d.ResolvePeersForRetrieval()
		require.Equal(t, 2, len(peers))

		for _, p := range peers {
			assert.True(t, p.HasRole(roles.EndorserRole))
		}
		assert.NotContains(t, peers.String(), "org4")
	})

	t.Run("Not enough endorsers", func(t *testing.T) {
		getMaxPeersForRetrieval = func() int { return 7 }

		d := New(channelID, ns1, coll1,
			&mocks.MockAccessPolicy{
				ReqPeerCount: 1,
				Orgs:         []string{org1MSPID, org2MSPID, org3MSPID},
			}, gossip)

		peers := d.ResolvePeersForRetrieval()
		require.Equal(t, 7, len(peers))

		var numEndorsers int
		for _, p := range peers {
			if p.HasRole(roles.EndorserRole) {
				numEndorsers++
			}
		}
		assert.Equal(t, 5, numEndorsers)
		assert.NotContains(t, peers.String(), "org4")
	})

}

func TestComputeDisseminationPlan(t *testing.T) {
	channelID := "testchannel"

	p1Org1 := mocks.NewMember(p1Org1Endpoint, p1Org1PKIID, committerRole)
	p2Org1 := mocks.NewMember(p2Org1Endpoint, p2Org1PKIID, endorserRole)
	p3Org1 := mocks.NewMember(p3Org1Endpoint, p3Org1PKIID, committerRole)
	p1Org2 := mocks.NewMember(p1Org2Endpoint, p1Org2PKIID, endorserRole)
	p2Org2 := mocks.NewMember(p2Org2Endpoint, p2Org2PKIID, committerRole)
	p3Org2 := mocks.NewMember(p3Org2Endpoint, p3Org2PKIID, endorserRole)
	p1Org3 := mocks.NewMember(p1Org3Endpoint, p1Org3PKIID, endorserRole)
	p2Org3 := mocks.NewMember(p2Org3Endpoint, p2Org3PKIID, committerRole)
	p3Org3 := mocks.NewMember(p3Org3Endpoint, p3Org3PKIID, endorserRole)

	gossip := mocks.NewMockGossipAdapter().
		Self(org1MSPID, p1Org1).
		Member(org1MSPID, p2Org1).
		Member(org1MSPID, p3Org1).
		Member(org2MSPID, p1Org2).
		Member(org2MSPID, p2Org2).
		Member(org2MSPID, p3Org2).
		Member(org3MSPID, p1Org3).
		Member(org3MSPID, p2Org3).
		Member(org3MSPID, p3Org3)

	colAP := &mocks.MockAccessPolicy{
		ReqPeerCount: 1,
		Orgs:         []string{org2MSPID, org3MSPID},
	}

	t.Run("Success", func(t *testing.T) {
		rwSet := mocks.NewPvtReadWriteSetCollectionBuilder(coll1).
			Write(key1, []byte("value1")).
			Build()
		colConfig := &cb.StaticCollectionConfig{
			Type: cb.CollectionType_COL_OFFLEDGER,
		}

		dPlan, handled, err := ComputeDisseminationPlan(channelID, ns1, rwSet, colConfig, colAP, nil, gossip)
		assert.NoError(t, err)
		assert.True(t, handled)
		assert.NotNil(t, dPlan)
	})

	t.Run("Invalid CAS Key", func(t *testing.T) {
		rwSet := mocks.NewPvtReadWriteSetCollectionBuilder(coll1).
			Write(key1, []byte("value1")).
			Build()
		colConfig := &cb.StaticCollectionConfig{
			Type: cb.CollectionType_COL_DCAS,
		}

		dPlan, handled, err := ComputeDisseminationPlan(channelID, ns1, rwSet, colConfig, colAP, nil, gossip)
		require.Error(t, err)
		assert.False(t, handled)
		assert.Nil(t, dPlan)
		assert.Contains(t, err.Error(), "the key should be the hash of the value")
	})

	t.Run("Valid CAS Key", func(t *testing.T) {
		rwSet := mocks.NewPvtReadWriteSetCollectionBuilder(coll1).
			Write(common.GetCASKey([]byte("value1")), []byte("value1")).
			Build()
		colConfig := &cb.StaticCollectionConfig{
			Type: cb.CollectionType_COL_DCAS,
		}

		dPlan, handled, err := ComputeDisseminationPlan(channelID, ns1, rwSet, colConfig, colAP, nil, gossip)
		require.NoError(t, err)
		require.True(t, handled)
		require.Equal(t, 1, len(dPlan))

		criteria := dPlan[0].Criteria

		assert.Equal(t, 2, criteria.MaxPeers)

		assert.False(t, criteria.IsEligible(p1Org1))
		assert.False(t, criteria.IsEligible(p2Org1))
		assert.False(t, criteria.IsEligible(p3Org1))
		assert.False(t, criteria.IsEligible(p1Org2))
		assert.True(t, criteria.IsEligible(p2Org2))
		assert.False(t, criteria.IsEligible(p3Org2))
		assert.False(t, criteria.IsEligible(p1Org3))
		assert.True(t, criteria.IsEligible(p2Org3))
		assert.False(t, criteria.IsEligible(p3Org3))
	})

	t.Run("Nil value", func(t *testing.T) {
		rwSet := mocks.NewPvtReadWriteSetCollectionBuilder(coll1).
			Write(key1, nil).
			Build()
		colConfig := &cb.StaticCollectionConfig{
			Type: cb.CollectionType_COL_OFFLEDGER,
		}

		dPlan, handled, err := ComputeDisseminationPlan(channelID, ns1, rwSet, colConfig, colAP, nil, gossip)
		require.Error(t, err)
		assert.False(t, handled)
		assert.Nil(t, dPlan)
		assert.Contains(t, err.Error(), "attempt to store nil value for key")
	})
}

func TestMain(m *testing.M) {
	// The local peer's roles are retrieved from ledgerconfig
	viper.SetDefault("ledger.roles", "committer,endorser")

	os.Exit(m.Run())
}
