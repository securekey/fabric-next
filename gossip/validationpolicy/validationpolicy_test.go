/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package validationpolicy

import (
	"testing"

	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/gossip/roleutil"
	"github.com/hyperledger/fabric/gossip/validationpolicy/mocks"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
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
	p3Org2Endpoint = "p3.org2.com"
	p3Org2PKIID    = common.PKIidType("pkiid_P3O2")

	org3MSPID      = "Org3MSP"
	p1Org3Endpoint = "p1.org3.com"
	p1Org3PKIID    = common.PKIidType("pkiid_P1O3")
	p2Org3Endpoint = "p2.org3.com"
	p2Org3PKIID    = common.PKIidType("pkiid_P2O3")

	validatorRole = string(ledgerconfig.ValidatorRole)
	endorserRole  = string(ledgerconfig.EndorserRole)

	g1Peers = []*mocks.MockPeer{
		mocks.Peer(org1MSPID, p1Org1Endpoint, validatorRole),
		mocks.Peer(org1MSPID, p2Org1Endpoint, validatorRole),
		mocks.Peer(org1MSPID, p3Org1Endpoint, validatorRole),
	}

	g2Peers = []*mocks.MockPeer{
		mocks.Peer(org2MSPID, p1Org2Endpoint, validatorRole),
		mocks.Peer(org2MSPID, p2Org2Endpoint, validatorRole),
		mocks.Peer(org2MSPID, p3Org2Endpoint, endorserRole),
	}

	g3Peers = []*mocks.MockPeer{
		mocks.Peer(org3MSPID, p1Org3Endpoint, validatorRole),
	}
)

func TestEvaluatePeerGroups(t *testing.T) {
	gossip := mocks.NewMockGossipAdapter().
		Self(org1MSPID, mocks.NewMember(p1Org1Endpoint, p1Org1PKIID))

	t.Run("Current Org", func(t *testing.T) {
		RegisterValidatorDiscovery(
			mocks.NewMockValidatorDiscovery().
				Group("g1", g1Peers...).
				Layout("g1"),
		)

		block := mocks.NewBlockBuilder().Number(1000).DataHash([]byte("current_org_test")).Build()

		evaluator := newPolicyEvaluator("testchannel", gossip, &InquireableValidationPolicy{MaxGroups: 0})

		peerGroups, err := evaluator.PeerGroups(block)
		require.NoError(t, err)
		require.Equal(t, 3, len(peerGroups))

		t.Logf("Peer groups: %s", peerGroups)

		assert.Equal(t, []string{p1Org1Endpoint}, asEndpoints(peerGroups[0]...))
		assert.Equal(t, []string{p2Org1Endpoint}, asEndpoints(peerGroups[1]...))
		assert.Equal(t, []string{p3Org1Endpoint}, asEndpoints(peerGroups[2]...))
	})

	t.Run("1 Of 3", func(t *testing.T) {
		RegisterValidatorDiscovery(
			mocks.NewMockValidatorDiscovery().
				Group("g1", g1Peers...).Group("g2", g2Peers...).Group("g3", g3Peers...).
				Layout("g1").Layout("g2").Layout("g3"),
		)

		block := mocks.NewBlockBuilder().Number(1001).DataHash([]byte("1_of_3_test")).Build()

		maxGroups := 3
		evaluator := newPolicyEvaluator("testchannel", gossip, &InquireableValidationPolicy{MaxGroups: maxGroups})

		peerGroups, err := evaluator.PeerGroups(block)
		require.NoError(t, err)
		require.Equal(t, maxGroups, len(peerGroups))

		t.Logf("Peer groups: %s", peerGroups)

		assert.Equal(t, []string{p2Org1Endpoint}, asEndpoints(peerGroups[0]...))
		assert.Equal(t, []string{p2Org2Endpoint}, asEndpoints(peerGroups[1]...))
		assert.Equal(t, []string{p3Org1Endpoint}, asEndpoints(peerGroups[2]...))
	})

	t.Run("2 Of 3", func(t *testing.T) {
		RegisterValidatorDiscovery(
			mocks.NewMockValidatorDiscovery().
				Group("g1", g1Peers...).Group("g2", g2Peers...).Group("g3", g3Peers...).
				Layout("g1", "g2").Layout("g1", "g3").Layout("g2", "g3"),
		)

		block := mocks.NewBlockBuilder().Number(1002).DataHash([]byte("2_of_3_test")).Build()

		evaluator := newPolicyEvaluator("testchannel", gossip, &InquireableValidationPolicy{MaxGroups: 4})
		peerGroups, err := evaluator.PeerGroups(block)
		require.NoError(t, err)
		require.Equal(t, 3, len(peerGroups), "Expecting only 3 unique peer groups")

		t.Logf("Peer groups: %s", peerGroups)

		assert.Equal(t, []string{p1Org3Endpoint, p3Org1Endpoint}, asEndpoints(peerGroups[0]...))
		assert.Equal(t, []string{p2Org1Endpoint, p2Org2Endpoint}, asEndpoints(peerGroups[1]...))
		assert.Equal(t, []string{p1Org1Endpoint, p1Org2Endpoint}, asEndpoints(peerGroups[2]...))
	})

	t.Run("3 Of 3", func(t *testing.T) {
		RegisterValidatorDiscovery(
			mocks.NewMockValidatorDiscovery().
				Group("g1", g1Peers...).Group("g2", g2Peers...).Group("g3", g3Peers...).
				Layout("g1", "g2", "g3"),
		)

		block := mocks.NewBlockBuilder().Number(1003).DataHash([]byte("3_of_3_test")).Build()

		maxGroups := 3
		evaluator := newPolicyEvaluator("testchannel", gossip, &InquireableValidationPolicy{MaxGroups: maxGroups})
		peerGroups, err := evaluator.PeerGroups(block)
		require.NoError(t, err)
		require.Equal(t, maxGroups, len(peerGroups))

		t.Logf("Peer groups: %s", peerGroups)

		assert.Equal(t, []string{p1Org3Endpoint, p2Org2Endpoint, p3Org1Endpoint}, asEndpoints(peerGroups[0]...))
		assert.Equal(t, []string{p1Org1Endpoint, p1Org2Endpoint, p1Org3Endpoint}, asEndpoints(peerGroups[1]...))
		assert.Equal(t, []string{p1Org2Endpoint, p1Org3Endpoint, p2Org1Endpoint}, asEndpoints(peerGroups[2]...))
	})
}

func TestValidateResults(t *testing.T) {
	// TODO
}

func TestValidationPolicy(t *testing.T) {
	// The local peer's roles are retrieved from ledgerconfig
	viper.SetDefault("ledger.roles", "committer,validator")
	gossip := mocks.NewMockGossipAdapter().
		Self(org1MSPID, mocks.NewMember(p1Org1Endpoint, p1Org1PKIID))

	t.Run("Error Retrieving Policy", func(t *testing.T) {
		policy := New("testchannel", gossip,
			func(channelID string) ([]byte, error) {
				return nil, errors.New("test invalid policy")
			},
		)
		require.NotNil(t, policy)

		block := mocks.NewBlockBuilder().Number(1000).DataHash([]byte("policy_error_test")).Build()
		pg, err := policy.evaluator.PeerGroups(block)
		require.Error(t, err)
		require.Nil(t, pg)
	})

	t.Run("Default Policy", func(t *testing.T) {
		policy := New("testchannel", gossip,
			func(channelID string) ([]byte, error) {
				return nil, nil
			},
		)
		require.NotNil(t, policy)

		RegisterValidatorDiscovery(
			mocks.NewMockValidatorDiscovery().
				Group("g1", g1Peers...).
				Layout("g1"),
		)

		block := mocks.NewBlockBuilder().Number(1000).DataHash([]byte("hash1")).Build()
		txFilter := policy.GetTxFilter(block)
		require.NotNil(t, txFilter)

		assert.True(t, txFilter(0))
		assert.False(t, txFilter(1))
		assert.False(t, txFilter(2))
		assert.True(t, txFilter(3))

		block = mocks.NewBlockBuilder().Number(1000).DataHash([]byte("hash2")).Build()
		txFilter = policy.GetTxFilter(block)
		require.NotNil(t, txFilter)

		assert.True(t, txFilter(0))
	})
}

func asEndpoints(members ...*roleutil.Member) []string {
	var endpoints []string
	for _, member := range members {
		endpoints = append(endpoints, member.Endpoint)
	}
	return endpoints
}
