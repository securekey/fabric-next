/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mscc

import (
	"github.com/hyperledger/fabric/core/chaincode/shim"
	"github.com/hyperledger/fabric/core/committer"
	"github.com/hyperledger/fabric/gossip/api"
	"github.com/hyperledger/fabric/gossip/comm"
	gcommon "github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/gossip/discovery"
	"github.com/hyperledger/fabric/gossip/service"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/gossip"
	proto "github.com/hyperledger/fabric/protos/gossip"
)

func newMockStub(gossip service.GossipService) *shim.MockStub {
	return shim.NewMockStub("MembershipSCC", &MembershipSCC{gossipServiceOverride: gossip})
}

type mockGossipService struct {
	mockGossip
}

func newMockGossipService(mspID []byte, members []discovery.NetworkMember) service.GossipService {
	return &mockGossipService{mockGossip: mockGossip{
		NetworkMembers: members,
		MSPid:          mspID,
	}}
}

func (s *mockGossipService) NewConfigEventer() service.ConfigProcessor {
	panic("not implemented")
}

func (s *mockGossipService) InitializeChannel(chainID string, committer committer.Committer, endpoints []string) {
	panic("not implemented")
}

func (s *mockGossipService) GetBlock(chainID string, index uint64) *common.Block {
	panic("not implemented")
}

func (s *mockGossipService) AddPayload(chainID string, payload *proto.Payload) error {
	panic("not implemented")
}

type mockGossip struct {
	NetworkMembers []discovery.NetworkMember
	MSPid          api.OrgIdentityType
}

func (s *mockGossip) Send(msg *gossip.GossipMessage, peers ...*comm.RemotePeer) {
	panic("not implemented")
}

func (s *mockGossip) Peers() []discovery.NetworkMember {
	return s.NetworkMembers
}

func (s *mockGossip) PeersOfChannel(gcommon.ChainID) []discovery.NetworkMember {
	return s.NetworkMembers
}

func (s *mockGossip) UpdateMetadata(metadata []byte) {
	panic("not implemented")
}

func (s *mockGossip) UpdateChannelMetadata(metadata []byte, chainID gcommon.ChainID) {
	panic("not implemented")
}

func (s *mockGossip) Gossip(msg *gossip.GossipMessage) {
	panic("not implemented")
}

func (s *mockGossip) Accept(acceptor gcommon.MessageAcceptor, passThrough bool) (<-chan *gossip.GossipMessage, <-chan gossip.ReceivedMessage) {
	panic("not implemented")
}

func (s *mockGossip) JoinChan(joinMsg api.JoinChannelMessage, chainID gcommon.ChainID) {
	panic("not implemented")
}

func (s *mockGossip) Stop() {
	panic("not implemented")
}

func (s *mockGossip) SuspectPeers(api.PeerSuspector) {
	panic("not implemented")
}

func (s *mockGossip) GetOrgOfPeer(PKIID gcommon.PKIidType) api.OrgIdentityType {
	return s.MSPid
}
