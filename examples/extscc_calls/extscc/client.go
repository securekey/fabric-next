/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package extscc

import (
	"context"
	"fmt"

	"google.golang.org/grpc"

	"strconv"

	"github.com/hyperledger/fabric/peer/common"
	pc "github.com/hyperledger/fabric/protos/common"
	pb "github.com/hyperledger/fabric/protos/peer"
	"github.com/hyperledger/fabric/protos/utils"
	"github.com/op/go-logging"
)

// ExtSCCClient is a client used by extscc SCC
type ExtSCCClient struct {
	endorserClient pb.EndorserClient
}

const (
	ok = 200

	extSCC     = "extscccaller"
	extSCCBin  = "extscccaller2"
	callEXTSCC = "invoke"
)

var logger = logging.MustGetLogger("extscc-client")

// NewClient returns a new client of the Membership SCC
func NewClient(conn *grpc.ClientConn) *ExtSCCClient {
	return &ExtSCCClient{endorserClient: pb.NewEndorserClient(conn)}
}

// InvokeExtSCC calls extscccaller which will invoke another EXT SCC (extscc)
func (c *ExtSCCClient) InvokeExtScc(args [][]byte) error {
	return c.invoke(callEXTSCC, args)
}

func (c *ExtSCCClient) invoke(function string, args [][]byte) error {
	signer, err := common.GetDefaultSigner()
	if err != nil {
		return fmt.Errorf("Error getting signer: %s", err)
	}

	creator, err := signer.Serialize()
	if err != nil {
		return fmt.Errorf("Error serializing signer: %s", err)
	}

	channelId := ""

	if args[1] != nil && len(args[1]) > 0 {
		channelId = fmt.Sprintf("%s", args[1])
		logger.Infof("===============>setting channel to %s", channelId)
	}

	extSCCN := extSCC
	isBin, err := strconv.ParseBool(string(args[2]))
	//get bin EXT SCC if bin is set
	if err == nil && isBin {
		extSCCN = extSCCBin
	}

	funcAndArgs := [][]byte{[]byte(function)}
	if len(args) > 3 {
		funcAndArgs = append(funcAndArgs, args[:3]...)
	}

	logger.Infof("-------->Invoking %s with args:", extSCCN)
	for p := range funcAndArgs {
		logger.Infof("\t %s: %#v", p, fmt.Sprintf("%s", funcAndArgs[p]))
	}

	var prop *pb.Proposal
	if prop, _, err = utils.CreateProposalFromCIS(
		pc.HeaderType_ENDORSER_TRANSACTION, channelId,
		&pb.ChaincodeInvocationSpec{
			ChaincodeSpec: &pb.ChaincodeSpec{
				ChaincodeId: &pb.ChaincodeID{Name: extSCCN},
				Input:       &pb.ChaincodeInput{Args: funcAndArgs},
			},
		},
		creator,
	); err != nil {
		return fmt.Errorf("Error creating proposal: %s\n", err)
	}

	signedProp, err := utils.GetSignedProposal(prop, signer)
	if err != nil {
		return fmt.Errorf("Error creating signed proposal: %s\n", err)
	}

	proposalResp, err := c.endorserClient.ProcessProposal(context.Background(), signedProp)
	if err != nil {
		return fmt.Errorf("Error endorsing: %s\n", err)
	}

	if proposalResp.Response.Status != ok {
		return fmt.Errorf("Error returned from endorser - Status: %d, Message: %s\n", proposalResp.Response.Status, proposalResp.Response.Message)
	}

	if proposalResp.Response.Payload == nil {
		return fmt.Errorf("Payload received is empty\n")
	}

	logger.Infof("Successfully invoked %s. Payload recieved: %s", extSCC, proposalResp.Response.Payload)
	return nil
}
