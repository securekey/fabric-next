/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package membership

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/core/scc/mscc/protos"
	"github.com/hyperledger/fabric/peer/common"
	pc "github.com/hyperledger/fabric/protos/common"
	pb "github.com/hyperledger/fabric/protos/peer"
	"github.com/hyperledger/fabric/protos/utils"
)

const (
	ok = 200

	membershipSCC         = "mscc"
	getAllPeersFunc       = "getAllPeers"
	getPeersOfChannelFunc = "getPeersOfChannel"
)

// MembershipClient is a client to the Membership SCC
type MembershipClient struct {
	endorserClient pb.EndorserClient
}

// NewClient returns a new client of the Membership SCC
func NewClient(conn *grpc.ClientConn) *MembershipClient {
	return &MembershipClient{endorserClient: pb.NewEndorserClient(conn)}
}

// GetAllPeers returns all peers of the target peer
func (c *MembershipClient) GetAllPeers() ([]*protos.PeerEndpoint, error) {
	return c.invoke(getAllPeersFunc, nil)
}

// GetPeersOfChannel returns all peers of the target peer for the given channel
func (c *MembershipClient) GetPeersOfChannel(channelID string) ([]*protos.PeerEndpoint, error) {
	return c.invoke(getPeersOfChannelFunc, [][]byte{[]byte(channelID)})
}

func (c *MembershipClient) invoke(function string, args [][]byte) ([]*protos.PeerEndpoint, error) {
	signer, err := common.GetDefaultSigner()
	if err != nil {
		return nil, fmt.Errorf("Error getting signer: %s", err)
	}

	creator, err := signer.Serialize()
	if err != nil {
		return nil, fmt.Errorf("Error serializing signer: %s", err)
	}

	funcAndArgs := [][]byte{[]byte(function)}
	funcAndArgs = append(funcAndArgs, args...)

	var prop *pb.Proposal
	if prop, _, err = utils.CreateProposalFromCIS(
		pc.HeaderType_ENDORSER_TRANSACTION, "",
		&pb.ChaincodeInvocationSpec{
			ChaincodeSpec: &pb.ChaincodeSpec{
				ChaincodeId: &pb.ChaincodeID{Name: membershipSCC},
				Input:       &pb.ChaincodeInput{Args: funcAndArgs},
			},
		},
		creator,
	); err != nil {
		return nil, fmt.Errorf("Error creating proposal: %s", err)
	}

	signedProp, err := utils.GetSignedProposal(prop, signer)
	if err != nil {
		return nil, fmt.Errorf("Error creating signed proposal: %s", err)
	}

	proposalResp, err := c.endorserClient.ProcessProposal(context.Background(), signedProp)
	if err != nil {
		return nil, fmt.Errorf("Error endorsing: %s", err)
	}

	if proposalResp.Response.Status != ok {
		return nil, fmt.Errorf("Error returned from endorser - Status: %d, Message: %s", proposalResp.Response.Status, proposalResp.Response.Message)
	}

	endpoints := &protos.PeerEndpoints{}
	if err := proto.Unmarshal(proposalResp.Response.Payload, endpoints); err != nil {
		return nil, fmt.Errorf("Error unmarshalling payload: %s", err)
	}

	return endpoints.Endpoints, nil
}
