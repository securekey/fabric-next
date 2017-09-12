/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mscc

import (
	"fmt"
	"strings"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/core/chaincode/shim"
	"github.com/hyperledger/fabric/core/ledger/ledgermgmt"
	"github.com/hyperledger/fabric/core/peer"
	"github.com/hyperledger/fabric/core/scc/mscc/protos"
	"github.com/hyperledger/fabric/gossip/discovery"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

// TestInit tests Init method
func TestInit(t *testing.T) {
	e := new(MembershipSCC)
	stub := shim.NewMockStub("mscc", e)

	if res := stub.MockInit("txID", nil); res.Status != shim.OK {
		fmt.Println("Init failed", string(res.Message))
		t.FailNow()
	}

}

// TestInvokeInvalidFunction tests Invoke method with an invalid function name
func TestInvokeInvalidFunction(t *testing.T) {
	identity := newMockIdentity()
	sProp, identityDeserializer := newMockSignedProposal(identity)

	args := [][]byte{}
	stub := newMockStub(nil, identity, identityDeserializer)
	if res := stub.MockInvokeWithSignedProposal("txID", args, sProp); res.Status == shim.OK {
		t.Fatalf("mscc invoke expecting error for invalid number of args")
	}

	args = [][]byte{[]byte("invalid")}
	if res := stub.MockInvokeWithSignedProposal("txID", args, sProp); res.Status == shim.OK {
		t.Fatalf("mscc invoke expecting error for invalid function")
	}
}

// TestGetAllPeers tests Invoke with the "getAllPeers" function.
func TestGetAllPeers(t *testing.T) {
	localAddress := "host3:1000"
	viper.Set("peer.address", localAddress)

	// First test with no members (except for self)

	members := []discovery.NetworkMember{}
	msp1 := []byte("Org1MSP")

	identity := newMockIdentity()
	sProp, identityDeserializer := newMockSignedProposal(identity)
	stub := newMockStub(newMockGossipService(msp1, members), identity, identityDeserializer)

	args := [][]byte{[]byte(getAllPeersFunction)}
	res := stub.MockInvokeWithSignedProposal("txID", args, sProp)
	if res.Status != shim.OK {
		t.Fatalf("mscc invoke(getAllPeers) - unexpected status: %d, Message: %s", res.Status, res.Message)
	}

	if len(res.Payload) == 0 {
		t.Fatalf("mscc invoke(getAllPeers) - unexpected nil payload in response")
	}

	endpoints := &protos.PeerEndpoints{}
	if err := proto.Unmarshal(res.Payload, endpoints); err != nil {
		t.Fatalf("mscc invoke(getAllPeers) - error unmarshalling payload: %s", err)
	}

	expected := []*protos.PeerEndpoint{
		newEndpoint(localAddress, localAddress, msp1),
	}

	if err := checkEndpoints(expected, endpoints.Endpoints); err != nil {
		t.Fatalf("mscc invoke(getAllPeers) - %s", err)
	}

	// Second test with two members plus self

	members = []discovery.NetworkMember{
		discovery.NetworkMember{Endpoint: "host1:1000", InternalEndpoint: "internalhost1:1000"},
		discovery.NetworkMember{Endpoint: "host2:1000", InternalEndpoint: "internalhost2:1000"},
	}

	args = [][]byte{[]byte(getAllPeersFunction)}

	stub = newMockStub(newMockGossipService(msp1, members), identity, identityDeserializer)

	res = stub.MockInvokeWithSignedProposal("txID", args, sProp)
	if res.Status != shim.OK {
		t.Fatalf("mscc invoke(getAllPeers) - unexpected status: %d, Message: %s", res.Status, res.Message)
	}

	if len(res.Payload) == 0 {
		t.Fatalf("mscc invoke(getAllPeers) - unexpected nil payload in response")
	}

	endpoints = &protos.PeerEndpoints{}
	if err := proto.Unmarshal(res.Payload, endpoints); err != nil {
		t.Fatalf("mscc invoke(getAllPeers) - error unmarshalling payload: %s", err)
	}

	expected = []*protos.PeerEndpoint{
		newEndpoint(localAddress, localAddress, msp1),
		newEndpoint("host1:1000", "internalhost1:1000", msp1),
		newEndpoint("host2:1000", "internalhost2:1000", msp1),
	}

	if err := checkEndpoints(expected, endpoints.Endpoints); err != nil {
		t.Fatalf("mscc invoke(getAllPeers) - %s", err)
	}
}

func TestGetPeersOfChannel(t *testing.T) {
	channelID := "testchannel"
	localAddress := "host3:1000"
	viper.Set("peer.address", localAddress)

	members := []discovery.NetworkMember{
		discovery.NetworkMember{Endpoint: "host1:1000", InternalEndpoint: "internalhost1:1000"},
		discovery.NetworkMember{Endpoint: "host2:1000", InternalEndpoint: "internalhost2:1000"},
	}

	peer.MockInitialize()
	defer ledgermgmt.CleanupTestEnv()

	// Test on channel that peer hasn't joined
	msp1 := []byte("Org1MSP")

	identity := newMockIdentity()
	sProp, identityDeserializer := newMockSignedProposal(identity)

	stub := newMockStub(newMockGossipService(msp1, members), identity, identityDeserializer)

	args := [][]byte{[]byte(getPeersOfChannelFunction), nil}
	res := stub.MockInvokeWithSignedProposal("txID", args, sProp)
	if res.Status == shim.OK {
		t.Fatalf("mscc invoke(getPeersOfChannel) - Expecting error for nil channel ID")
	}

	args = [][]byte{[]byte(getPeersOfChannelFunction), []byte(channelID)}
	res = stub.MockInvokeWithSignedProposal("txID", args, sProp)
	if res.Status != shim.OK {
		t.Fatalf("mscc invoke(getPeersOfChannel) - unexpected status: %d, Message: %s", res.Status, res.Message)
	}

	if len(res.Payload) == 0 {
		t.Fatalf("mscc invoke(getPeersOfChannel) - unexpected nil payload in response")
	}

	endpoints := &protos.PeerEndpoints{}
	if err := proto.Unmarshal(res.Payload, endpoints); err != nil {
		t.Fatalf("mscc invoke(getPeersOfChannel) - error unmarshalling payload: %s", err)
	}

	expected := []*protos.PeerEndpoint{
		newEndpoint("host1:1000", "internalhost1:1000", msp1),
		newEndpoint("host2:1000", "internalhost2:1000", msp1),
	}

	if err := checkEndpoints(expected, endpoints.Endpoints); err != nil {
		t.Fatalf("mscc invoke(getPeersOfChannel) - %s", err)
	}

	// Join the peer to the channel
	if err := peer.MockCreateChain(channelID); err != nil {
		t.Fatalf("unexpected error when creating mock channel: %s", err)
	}

	args = [][]byte{[]byte(getPeersOfChannelFunction), []byte(channelID)}
	res = stub.MockInvokeWithSignedProposal("txID", args, sProp)
	if res.Status != shim.OK {
		t.Fatalf("mscc invoke(getPeersOfChannel) - unexpected status: %d, Message: %s", res.Status, res.Message)
	}

	if len(res.Payload) == 0 {
		t.Fatalf("mscc invoke(getPeersOfChannel) - unexpected nil payload in response")
	}

	endpoints = &protos.PeerEndpoints{}
	if err := proto.Unmarshal(res.Payload, endpoints); err != nil {
		t.Fatalf("mscc invoke(getPeersOfChannel) - error unmarshalling payload: %s", err)
	}

	expected = []*protos.PeerEndpoint{
		newEndpoint(localAddress, localAddress, msp1),
		newEndpoint("host1:1000", "internalhost1:1000", msp1),
		newEndpoint("host2:1000", "internalhost2:1000", msp1),
	}

	if err := checkEndpoints(expected, endpoints.Endpoints); err != nil {
		t.Fatalf("mscc invoke(getPeersOfChannel) - %s", err)
	}
}

// TestAccessControl tests access control
func TestAccessControl(t *testing.T) {
	sProp, identityDeserializer := newMockSignedProposal([]byte("invalididentity"))

	// getAllPeers
	stub := newMockStub(nil, newMockIdentity(), identityDeserializer)
	res := stub.MockInvokeWithSignedProposal("txID", [][]byte{[]byte(getAllPeersFunction), nil}, sProp)
	assert.Equal(t, int32(shim.ERROR), res.Status, "mscc invoke expected to fail with authorization error")
	assert.True(t, strings.HasPrefix(res.Message, "\"getAllPeers\" request failed authorization check"), "Unexpected error message: %s", res.Message)

	// getPeersOfChannel
	stub = newMockStub(nil, newMockIdentity(), identityDeserializer)
	res = stub.MockInvokeWithSignedProposal("txID", [][]byte{[]byte(getPeersOfChannelFunction), nil}, sProp)
	assert.Equal(t, int32(shim.ERROR), res.Status, "mscc invoke expected to fail with authorization error")
	assert.True(t, strings.HasPrefix(res.Message, "\"getPeersOfChannel\" request failed authorization check"), "Unexpected error message: %s", res.Message)
}

func checkEndpoints(expected []*protos.PeerEndpoint, actual []*protos.PeerEndpoint) error {
	if len(expected) != len(actual) {
		return fmt.Errorf("expecting %d endpoints but received %d", len(expected), len(actual))
	}

	fmt.Printf("Expected: %v, Actual: %v\n", expected, actual)
	for _, endpoint := range expected {
		if !contains(actual, endpoint) {
			return fmt.Errorf("unexpected endpoint %s", endpoint)
		}
	}

	return nil
}

func contains(endpoints []*protos.PeerEndpoint, member *protos.PeerEndpoint) bool {
	for _, endpoint := range endpoints {
		if endpoint.Endpoint == member.Endpoint && endpoint.InternalEndpoint == member.InternalEndpoint {
			return true
		}
	}
	return false
}

func newEndpoint(endpoint string, internalEndpoint string, mspID []byte) *protos.PeerEndpoint {
	return &protos.PeerEndpoint{
		Endpoint:         endpoint,
		InternalEndpoint: internalEndpoint,
		MSPid:            mspID,
	}
}
