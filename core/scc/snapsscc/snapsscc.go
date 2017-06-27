/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package snapsscc

import (
	"errors"
	"fmt"

	logging "github.com/op/go-logging"
	"github.com/spf13/viper"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/hyperledger/fabric/core/chaincode/shim"
	"github.com/hyperledger/fabric/core/scc/snapsscc/protos"
	pb "github.com/hyperledger/fabric/protos/peer"
	creds "google.golang.org/grpc/credentials"
)

var logger = logging.MustGetLogger("snapsscc")

// SnapsSCC is the Snaps System Chaincode that facilitates invoking snaps
type SnapsSCC struct {
}

// Init is called once when the chaincode is started for the first time
func (t *SnapsSCC) Init(stub shim.ChaincodeStubInterface) pb.Response {
	logger.Infof("Successfully initialized")
	return shim.Success(nil)
}

// Invoke is the main entry point for invocations
func (t *SnapsSCC) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	args := stub.GetArgs()
	if len(args) < 1 {
		return shim.Error("Function not provided.")
	}

	function := string(args[0])

	if function == "invokeSnap" {
		// Invoke snap
		return t.invokeSnap(stub, args[1:])
	}

	return shim.Error("Invalid invoke function name. Expecting \"invokeSnap\"")
}

// invokeSnap is a helper function for connecting to snaps dispatcher and invoking snaps
func (t *SnapsSCC) invokeSnap(stub shim.ChaincodeStubInterface, args [][]byte) pb.Response {
	if len(args) < 1 {
		return shim.Error("Snap name not provided.")
	}
	snapName := string(args[0])
	snapArgs := args[1:]

	conn, err := connectToSnapsDispatcher()
	if err != nil {
		errMsg := fmt.Sprintf("Failed to connect to snaps dispatcher. Error: %v", err)
		logger.Errorf(errMsg)
		return shim.Error(errMsg)
	}
	defer conn.Close()

	// Invoke snap using snaps client
	client := protos.NewSnapClient(conn)
	irequest := protos.Request{SnapName: snapName, Args: snapArgs}
	iresponse, err := client.Invoke(context.Background(), &irequest)
	if err != nil {
		errMsg := fmt.Sprintf("Failed to invoke snaps dispatcher. Error: %v", err)
		logger.Warning(errMsg)
		return shim.Error(errMsg)
	}

	// Check for error in response
	if iresponse.Error != "" {
		errMsg := fmt.Sprintf("Snaps invoke has returned an error: %s", iresponse.Error)
		logger.Warning(errMsg)
		return shim.Error(errMsg)
	}

	return shim.Success(iresponse.Payload[0])
}

// Get connection to snaps dispatcher
func connectToSnapsDispatcher() (*grpc.ClientConn, error) {

	// read snaps dispatcher port
	snapsInvokeAddress := viper.GetString("chaincode.system.config.snapsscc.snapsDispatcherAddress")
	if snapsInvokeAddress == "" {
		logger.Warningf("snapsDispatcherAddress was not set. Set property: 'chaincode.system.config.snapsscc.snapsDispatcherAddress'")
		return nil, errors.New("Error detecting snaps dispatcher address from property chaincode.system.config.snapsscc.snapsDispatcherAddress")
	}

	// grpc connection options
	var opts []grpc.DialOption
	if viper.GetBool("chaincode.system.config.snapsscc.tls.enabled") {
		sn := viper.GetString("chaincode.system.config.snapsscc.tls.serverhostoverride")
		peerTLSCertFile := viper.GetString("chaincode.system.config.snapsscc.tls.rootcert.file")
		creds, err := creds.NewClientTLSFromFile(peerTLSCertFile, sn)
		if err != nil {
			return nil, fmt.Errorf("Failed to create snaps dispatcher tls client from file: %s Error: %v", peerTLSCertFile, err)
		}
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		// TLS disabled
		opts = append(opts, grpc.WithInsecure())
	}

	logger.Infof("Dialing snaps dispatcher on: %s", snapsInvokeAddress)

	conn, err := grpc.Dial(snapsInvokeAddress, opts...)
	if err != nil {
		return nil, fmt.Errorf("Failed to dial snaps dispatcher at %s. Error: %v", snapsInvokeAddress, err)
	}

	return conn, err
}
