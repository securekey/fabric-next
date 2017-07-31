/*

 Copyright SecureKey Technologies Inc. All Rights Reserved.

 SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"fmt"

	"github.com/hyperledger/fabric/core/chaincode/shim"
	pb "github.com/hyperledger/fabric/protos/peer"
)

// SimpleChaincodeCaller example simple Chaincode implementation
type SimpleChaincodeCaller struct {
}

var logger = shim.NewLogger("extscccaller2")

// Init ...
func (t *SimpleChaincodeCaller) Init(stub shim.ChaincodeStubInterface) pb.Response {
	logger.Debugf("-----------------------------------------------")
	logger.Debugf("=========================> extscc Caller 2 Init")
	logger.Debugf("-----------------------------------------------")
	logger.Debugf("---------------------->>>>stub args are %s", stub.GetArgs())
	return shim.Success(nil)
}

// Invoke ...
func (t *SimpleChaincodeCaller) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	callArgs := [][]byte{[]byte("invoke")}
	callArgs = append(callArgs, []byte("Hello extscc from Caller 2"))
	logger.Debugf("***********************===================-------------------")
	logger.Debugf("*********************** extscc Caller 2 Invoke - About to invoke extscc3 with args: %s", callArgs)
	logger.Debugf("***********************===================-------------------")

	response := stub.InvokeChaincode("extscc3", callArgs, "")

	if response.Status != shim.OK {
		errStr := fmt.Sprintf("Failed to invoke chaincode %s from extscccaller_cc2 . Error: %s\n", "extscc3", string(response.Message))
		logger.Warning(errStr)
		return shim.Error(errStr)
	}
	return response
}

func main() {
	logger.Info("EXT SCC Caller 2 Main")
	err := shim.Start(new(SimpleChaincodeCaller))
	if err != nil {
		logger.Errorf("Error starting extscccaller2 chaincode: %s", err)
	}
}
