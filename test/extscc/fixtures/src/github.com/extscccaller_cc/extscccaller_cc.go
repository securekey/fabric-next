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

var logger = shim.NewLogger("extscccaller")

// Init ...
func (t *SimpleChaincodeCaller) Init(stub shim.ChaincodeStubInterface) pb.Response {
	logger.Debugf("-----------------------------------------------")
	logger.Debugf("=========================> extscc Caller Init")
	logger.Debugf("-----------------------------------------------")
	logger.Debugf("---------------------->>>>stub args are %s", stub.GetArgs())
	return shim.Success(nil)
}

// Invoke ...
func (t *SimpleChaincodeCaller) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	callArgs := [][]byte{[]byte("invoke")}
	callArgs = append(callArgs, []byte("Hello extscc from Caller"))
	logger.Debugf("***********************===================-------------------")
	logger.Debugf("*********************** extscc Caller Invoke - About to invoke extscc with args: %s", callArgs)
	logger.Debugf("***********************===================-------------------")

	response := stub.InvokeChaincode("extscc1", callArgs, "")

	if response.Status != shim.OK {
		errStr := fmt.Sprintf("Failed to invoke chaincode %s from extscccaller_cc . Error: %s", "extscc1", string(response.Message))
		logger.Warning(errStr)
		return shim.Error(errStr)
	}
	return response
}

func main() {
	logger.Info("EXT SCC Caller Main")
	err := shim.Start(new(SimpleChaincodeCaller))
	if err != nil {
		logger.Errorf("Error starting extscccaller chaincode: %s", err)
	}
}
