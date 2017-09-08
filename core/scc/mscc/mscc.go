/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mscc

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/core/chaincode/shim"
	"github.com/hyperledger/fabric/core/peer"
	"github.com/hyperledger/fabric/core/policy"
	"github.com/hyperledger/fabric/core/scc/mscc/protos"
	"github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/gossip/discovery"
	"github.com/hyperledger/fabric/gossip/service"
	mspmgmt "github.com/hyperledger/fabric/msp/mgmt"
	pb "github.com/hyperledger/fabric/protos/peer"
	logging "github.com/op/go-logging"
)

var logger = logging.MustGetLogger("mscc")

// Available function:
const (
	getAllPeersFunction       = "getAllPeers"
	getPeersOfChannelFunction = "getPeersOfChannel"
)

// MembershipSCC is the System Chaincode that provides information about peer membership
type MembershipSCC struct {
	gossipServiceOverride service.GossipService
	policyChecker         policy.PolicyChecker
}

// Init is called once when the chaincode started the first time
func (t *MembershipSCC) Init(stub shim.ChaincodeStubInterface) pb.Response {
	// Init policy checker for access control
	t.policyChecker = policy.NewPolicyChecker(
		peer.NewChannelPolicyManagerGetter(),
		mspmgmt.GetLocalMSP(),
		mspmgmt.NewLocalMSPPrincipalGetter(),
	)

	logger.Infof("Successfully initialized")
	return shim.Success(nil)
}

// Invoke is the main entry point for invocations
func (t *MembershipSCC) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	args := stub.GetArgs()
	if len(args) == 0 {
		return shim.Error(fmt.Sprintf("Function not provided. Expecting one of %s or %s", getAllPeersFunction, getPeersOfChannelFunction))
	}

	functionName := string(args[0])

	// Check ACL
	sp, err := stub.GetSignedProposal()
	if err != nil {
		return shim.Error(fmt.Sprintf("Failed getting signed proposal from stub: [%s]", err))
	}
	if err = t.policyChecker.CheckPolicyNoChannel(mspmgmt.Members, sp); err != nil {
		return shim.Error(fmt.Sprintf("\"%s\" request failed authorization check: [%s]", functionName, err))
	}

	switch functionName {
	case getAllPeersFunction:
		return t.getAllPeers(stub, args[1:])
	case getPeersOfChannelFunction:
		return t.getPeersOfChannel(stub, args[1:])
	default:
		return shim.Error(fmt.Sprintf("Invalid function: %s. Expecting one of %s or %s", functionName, getAllPeersFunction, getPeersOfChannelFunction))
	}
}

//getAllPeers retrieves all of the peers (excluding this one) that are currently alive
func (t *MembershipSCC) getAllPeers(stub shim.ChaincodeStubInterface, args [][]byte) pb.Response {
	payload, err := t.marshalEndpoints(t.gossip().Peers(), true)
	if err != nil {
		return shim.Error(err.Error())
	}
	return shim.Success(payload)
}

//getPeersOfChannel retrieves all of the peers (excluding this one) that are currently alive and joined to the given channel
func (t *MembershipSCC) getPeersOfChannel(stub shim.ChaincodeStubInterface, args [][]byte) pb.Response {
	if len(args) == 0 {
		return shim.Error("Expecting channel ID")
	}

	channelID := string(args[0])
	if channelID == "" {
		return shim.Error("Expecting channel ID")
	}

	localPeerJoined := false
	for _, ch := range peer.GetChannelsInfo() {
		if ch.ChannelId == channelID {
			localPeerJoined = true
			break
		}
	}

	payload, err := t.marshalEndpoints(t.gossip().PeersOfChannel(common.ChainID(channelID)), localPeerJoined)
	if err != nil {
		return shim.Error(err.Error())
	}

	logger.Debugf("Returning payload: %+v", payload)

	return shim.Success(payload)
}

func (t *MembershipSCC) marshalEndpoints(members []discovery.NetworkMember, includeLocalPeer bool) ([]byte, error) {
	gossip := t.gossip()

	peerEndpoints := &protos.PeerEndpoints{}
	for _, member := range members {
		peerEndpoints.Endpoints = append(peerEndpoints.Endpoints, &protos.PeerEndpoint{
			Endpoint:         member.Endpoint,
			InternalEndpoint: member.InternalEndpoint,
			MSPid:            gossip.GetOrgOfPeer(member.PKIid),
		})
	}

	if includeLocalPeer {
		// Add self since Gossip only contains other peers
		peerEndpoint, err := peer.GetPeerEndpoint()
		if err != nil {
			return nil, fmt.Errorf("Error reading peer endpoint: %s", err)
		}

		localMSPid, err := mspmgmt.GetLocalMSP().GetIdentifier()
		if err != nil {
			return nil, fmt.Errorf("Error getting local MSP Identifier: %s", err)
		}

		self := &protos.PeerEndpoint{
			Endpoint:         peerEndpoint.Address,
			InternalEndpoint: peerEndpoint.Address,
			MSPid:            []byte(localMSPid),
		}

		peerEndpoints.Endpoints = append(peerEndpoints.Endpoints, self)
	}

	payload, err := proto.Marshal(peerEndpoints)
	if err != nil {
		return nil, fmt.Errorf("error marshalling peer endpoints: %v", err)
	}
	return payload, nil
}

func (t *MembershipSCC) gossip() service.GossipService {
	if t.gossipServiceOverride != nil {
		return t.gossipServiceOverride
	}
	return service.GetGossipService()
}
