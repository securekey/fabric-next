/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package validationpolicy

import (
	"fmt"
	"os"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/cauthdsl"
	"github.com/hyperledger/fabric/core/committer/txvalidator/validationpolicy/mocks"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/gossip/roleutil"
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
	p3Org3Endpoint = "p3.org3.com"
	p3Org3PKIID    = common.PKIidType("pkiid_P3O3")

	validatorRole = string(ledgerconfig.ValidatorRole)
	endorserRole  = string(ledgerconfig.EndorserRole)

	gossip gossipAdapter

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
		mocks.Peer(org3MSPID, p2Org3Endpoint, validatorRole),
		mocks.Peer(org3MSPID, p3Org3Endpoint, validatorRole),
	}
)

func TestEvaluatePeerGroups(t *testing.T) {
	channelID := "testchannel"

	t.Run("Current Org", func(t *testing.T) {
		RegisterValidatorDiscovery(
			mocks.NewMockValidatorDiscovery().
				Group("g1", g1Peers...).
				Layout("g1"),
		)

		block := mocks.NewBlockBuilder().Number(1000).DataHash([]byte("current_org_test")).Build()

		evaluator, err := newEvaluator(
			channelID, gossip,
			mocks.NewPolicyEvaluator(), mocks.NewPolicyEvaluator(),
			newPolicy(channelID, fmt.Sprintf("AND ('%s.member')", org1MSPID), 0),
		)
		require.NoError(t, err)

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
		evaluator, err := newEvaluator(
			channelID, gossip,
			mocks.NewPolicyEvaluator(), mocks.NewPolicyEvaluator(),
			newPolicy(channelID, fmt.Sprintf("OR ('%s.member','%s.member','%s.member')", org1MSPID, org2MSPID, org3MSPID), maxGroups),
		)
		require.NoError(t, err)

		peerGroups, err := evaluator.PeerGroups(block)
		require.NoError(t, err)
		require.Equal(t, maxGroups, len(peerGroups))

		t.Logf("Peer groups: %s", peerGroups)

		assert.Equal(t, []string{p3Org3Endpoint}, asEndpoints(peerGroups[0]...))
		assert.Equal(t, []string{p1Org1Endpoint}, asEndpoints(peerGroups[1]...))
		assert.Equal(t, []string{p1Org2Endpoint}, asEndpoints(peerGroups[2]...))
	})

	t.Run("2 Of 3", func(t *testing.T) {
		RegisterValidatorDiscovery(
			mocks.NewMockValidatorDiscovery().
				Group("g1", g1Peers...).Group("g2", g2Peers...).Group("g3", g3Peers...).
				Layout("g1", "g2").Layout("g1", "g3").Layout("g2", "g3"),
		)

		block := mocks.NewBlockBuilder().Number(1002).DataHash([]byte("2_of_3_test")).Build()

		maxGroups := 5
		evaluator, err := newEvaluator(
			channelID, gossip,
			mocks.NewPolicyEvaluator(), mocks.NewPolicyEvaluator(),
			newPolicy(channelID, fmt.Sprintf("OutOf(2, '%s.member','%s.member','%s.member')", org1MSPID, org2MSPID, org3MSPID), maxGroups),
		)
		require.NoError(t, err)

		peerGroups, err := evaluator.PeerGroups(block)
		require.NoError(t, err)
		require.Equal(t, 4, len(peerGroups)) // Not enough peers to have 5 unique groups

		t.Logf("Peer groups: %s", peerGroups)

		assert.Equal(t, []string{p1Org1Endpoint, p3Org3Endpoint}, asEndpoints(peerGroups[0]...))
		assert.Equal(t, []string{p1Org2Endpoint, p1Org3Endpoint}, asEndpoints(peerGroups[1]...))
		assert.Equal(t, []string{p2Org1Endpoint, p2Org2Endpoint}, asEndpoints(peerGroups[2]...))
		assert.Equal(t, []string{p2Org3Endpoint, p3Org1Endpoint}, asEndpoints(peerGroups[3]...))
	})

	t.Run("3 Of 3", func(t *testing.T) {
		RegisterValidatorDiscovery(
			mocks.NewMockValidatorDiscovery().
				Group("g1", g1Peers...).Group("g2", g2Peers...).Group("g3", g3Peers...).
				Layout("g1", "g2", "g3"),
		)

		block := mocks.NewBlockBuilder().Number(1003).DataHash([]byte("3_of_3_test")).Build()

		maxGroups := 3
		evaluator, err := newEvaluator(
			channelID, gossip,
			mocks.NewPolicyEvaluator(), mocks.NewPolicyEvaluator(),
			newPolicy(channelID, fmt.Sprintf("AND('%s.member','%s.member','%s.member')", org1MSPID, org2MSPID, org3MSPID), maxGroups),
		)
		require.NoError(t, err)

		peerGroups, err := evaluator.PeerGroups(block)
		require.NoError(t, err)
		require.Equal(t, maxGroups, len(peerGroups))

		t.Logf("Peer groups: %s", peerGroups)

		assert.Equal(t, []string{p1Org2Endpoint, p3Org1Endpoint, p3Org3Endpoint}, asEndpoints(peerGroups[0]...))
		assert.Equal(t, []string{p1Org3Endpoint, p2Org1Endpoint, p2Org2Endpoint}, asEndpoints(peerGroups[1]...))
		assert.Equal(t, []string{p2Org1Endpoint, p2Org2Endpoint, p2Org3Endpoint}, asEndpoints(peerGroups[2]...))
	})
}

func TestValidationPolicy(t *testing.T) {
	t.Run("Error retrieving policy", func(t *testing.T) {
		policy := New("testchannel", gossip, mocks.NewPolicyProvider(),
			&mockPolicyRetriever{err: errors.New("test invalid policy")})
		require.NotNil(t, policy)

		block := mocks.NewBlockBuilder().Number(1000).DataHash([]byte("policy_error_test")).Build()
		pg, err := policy.evaluator.PeerGroups(block)
		require.Error(t, err)
		require.Nil(t, pg)
	})

	t.Run("Default policy", func(t *testing.T) {
		policy := New("testchannel", gossip, mocks.NewPolicyProvider(),
			&mockPolicyRetriever{})
		require.NotNil(t, policy)

		RegisterValidatorDiscovery(
			mocks.NewMockValidatorDiscovery().
				Group("g1", g1Peers...).
				Layout("g1"),
		)

		block := mocks.NewBlockBuilder().Number(1000).DataHash([]byte("default_policy_test")).Build()
		pg, err := policy.evaluator.PeerGroups(block)
		require.NoError(t, err)
		require.NotNil(t, pg)
	})

	t.Run("Local-org policy", func(t *testing.T) {
		maxGroups := 2
		policy := New("testchannel", gossip, mocks.NewPolicyProvider(),
			&mockPolicyRetriever{policyBytes: getPolicyBytes(fmt.Sprintf("MAX(%d,AND('%s.member'))", maxGroups, org1MSPID))})
		require.NotNil(t, policy)

		RegisterValidatorDiscovery(
			mocks.NewMockValidatorDiscovery().
				Group("g1", g1Peers...).
				Layout("g1"),
		)

		block := mocks.NewBlockBuilder().Number(1000).DataHash([]byte("hash1")).Build()
		txFilter := policy.GetTxFilter(block)
		require.NotNil(t, txFilter)

		assert.False(t, txFilter(0))
		assert.True(t, txFilter(1))
		assert.False(t, txFilter(2))
		assert.True(t, txFilter(3))

		// A different hash should shuffle the Tx indexes
		block = mocks.NewBlockBuilder().Number(1000).DataHash([]byte("hash3")).Build()
		txFilter = policy.GetTxFilter(block)
		require.NotNil(t, txFilter)

		assert.True(t, txFilter(0))
		assert.False(t, txFilter(1))
		assert.True(t, txFilter(2))

		peers, err := policy.GetValidatingPeers(block)
		require.NoError(t, err)
		require.Equal(t, maxGroups, len(peers))
	})

	t.Run("Policy update", func(t *testing.T) {
		policyBytes := getPolicyBytes("MAX(3,AND('Org1MSP.member'))")
		policyRetriever := &mockPolicyRetriever{policyBytes: policyBytes}
		policy := New("testchannel", gossip, mocks.NewPolicyProvider(), policyRetriever)
		require.NotNil(t, policy)

		block := mocks.NewBlockBuilder().Number(1000).DataHash([]byte("config_update_test")).Build()
		pg, err := policy.evaluator.PeerGroups(block)
		require.NoError(t, err)
		require.NotNil(t, pg)

		evalWrapper, ok := policy.evaluator.(*evaluatorWrapper)
		require.True(t, ok)
		evaluator1 := evalWrapper.target
		require.NotNil(t, evaluator1)

		// Should use the same (cached) evaluator
		_, err = policy.GetValidatingPeers(block)
		require.NoError(t, err)
		require.Equal(t, evaluator1, policy.evaluator.(*evaluatorWrapper).target)

		updatedPolicyBytes := getPolicyBytes("MAX(3,AND('Org1MSP.member','Org2MSP.member'))")
		assert.NotEqual(t, policyBytes, updatedPolicyBytes)
		policyRetriever.UpdatePolicyBytes(updatedPolicyBytes)

		_, err = policy.GetValidatingPeers(block)
		require.NoError(t, err)
		// A new evaluator should have been created
		require.NotEqual(t, evaluator1, policy.evaluator.(*evaluatorWrapper).target)
	})
}

func TestValidate(t *testing.T) {
	t.Run("Validate local-org local peer", func(t *testing.T) {
		policy := New("testchannel", gossip, mocks.NewPolicyProvider(),
			&mockPolicyRetriever{policyBytes: getPolicyBytes(fmt.Sprintf("MAX(3,AND('%s.member'))", org1MSPID))})
		require.NotNil(t, policy)

		err := policy.Validate(
			[]*ValidationResults{
				{
					BlockNumber: 1000,
					Endpoint:    p1Org1Endpoint,
					MSPID:       org1MSPID,
					TxFlags:     []uint8{0},
					Local:       true,
				},
			},
		)
		require.NoError(t, err)
	})

	t.Run("Validate local-org remote peer", func(t *testing.T) {
		policy := New("testchannel", gossip, mocks.NewPolicyProvider(),
			&mockPolicyRetriever{policyBytes: getPolicyBytes(fmt.Sprintf("MAX(3,AND('%s.member'))", org1MSPID))})
		require.NotNil(t, policy)

		err := policy.Validate(
			[]*ValidationResults{
				{
					BlockNumber: 1000,
					Endpoint:    p2Org1Endpoint,
					MSPID:       org1MSPID,
					TxFlags:     []uint8{0},
					Identity:    []byte(p2Org1Endpoint),
					Signature:   []byte("p2Org1signature"),
				},
			},
		)
		require.NoError(t, err)
	})

	t.Run("Validate cross org", func(t *testing.T) {
		policy := New("testchannel", gossip, mocks.NewPolicyProvider(),
			&mockPolicyRetriever{policyBytes: getPolicyBytes("MAX(3,AND('Org1MSP.member','Org3MSP.member'))")})
		require.NotNil(t, policy)

		err := policy.Validate(
			[]*ValidationResults{
				{
					BlockNumber: 1000,
					Endpoint:    p1Org2Endpoint,
					MSPID:       org2MSPID,
					TxFlags:     []uint8{0},
					Identity:    []byte(p1Org2Endpoint),
					Signature:   []byte("p1Org2signature"),
				},
				{
					BlockNumber: 1000,
					Endpoint:    p1Org3Endpoint,
					MSPID:       org3MSPID,
					TxFlags:     []uint8{0},
					Identity:    []byte(p1Org3Endpoint),
					Signature:   []byte("p1Org3signature"),
				},
			},
		)
		require.NoError(t, err)
	})
}

func TestMain(m *testing.M) {
	localMSPIDRetriever = func() (string, error) { return org1MSPID, nil }
	gossip = mocks.NewMockGossipAdapter().Self(org1MSPID, mocks.NewMember(p1Org1Endpoint, p1Org1PKIID))

	// The local peer's roles are retrieved from ledgerconfig
	viper.SetDefault("ledger.roles", "committer,validator")

	os.Exit(m.Run())
}

func asEndpoints(members ...*roleutil.Member) []string {
	var endpoints []string
	for _, member := range members {
		endpoints = append(endpoints, member.Endpoint)
	}
	return endpoints
}

func newPolicy(channelID, expression string, maxGroups int) *inquireableValidationPolicy {
	policy, err := newInquireablePolicy(channelID, getPolicyBytes(expression))
	if err != nil {
		panic(err.Error())
	}
	policy.maxGroups = int32(maxGroups)
	return policy
}

func getPolicyBytes(expression string) []byte {
	sigPol, err := cauthdsl.FromString(expression)
	if err != nil {
		panic(err.Error())
	}
	policyBytes, err := proto.Marshal(sigPol)
	if err != nil {
		panic(err.Error())
	}
	return policyBytes
}

type mockPolicyRetriever struct {
	policyBytes   []byte
	err           error
	configUpdated ConfigUpdateListener
}

func (r *mockPolicyRetriever) GetPolicyBytes() ([]byte, bool, error) {
	return r.policyBytes, len(r.policyBytes) > 0, r.err
}

func (r *mockPolicyRetriever) AddListener(listener ConfigUpdateListener) {
	r.configUpdated = listener
}

func (r *mockPolicyRetriever) UpdatePolicyBytes(policyBytes []byte) {
	r.policyBytes = policyBytes
	if r.configUpdated != nil {
		r.configUpdated()
	}
}
