/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"fmt"

	"github.com/hyperledger/fabric/core/chaincode/shim"
	pb "github.com/hyperledger/fabric/protos/peer"
	"github.com/spf13/viper"
)

// SimpleChaincode example simple Chaincode implementation
type SimpleChaincode struct {
}

var logger = shim.NewLogger("extscc")

// Init ...
func (t *SimpleChaincode) Init(stub shim.ChaincodeStubInterface) pb.Response {
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		return shim.Error(fmt.Sprintf("Error reading configuration: %s", err))
	}
	v := viper.GetString("vipertest")
	if v != "Viper Works!" {
		return shim.Error("Error initializing viper")
	}
	logger.Info("-----------------------------------------------------------------")
	logger.Infof("=========================> extscc Init completed, %s", v)
	logger.Info("-----------------------------------------------------------------")
	return shim.Success(nil)
}

// Query ...
func (t *SimpleChaincode) Query(stub shim.ChaincodeStubInterface) pb.Response {
	return shim.Error("Unknown supported call")
}

// Invoke ...
// Transaction makes payment of X units from A to B
func (t *SimpleChaincode) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	// TODO - implement something for testing
	logger.Info("-----------------------------------------------------------------")
	logger.Infof("=========================> extscc Invoke completed, returning %s", []byte("Hello World from ExtScc"))
	logger.Info("-----------------------------------------------------------------")
	return shim.Success([]byte("Hello World from ExtScc"))
}

func main() {
	fmt.Println("EXT SCC Main")
	err := shim.Start(new(SimpleChaincode))
	if err != nil {
		fmt.Printf("Error starting Simple chaincode: %s", err)
	}
}
