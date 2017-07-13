/*
Copyright IBM Corp. 2016, 2017 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scc

import (
	//import system chain codes here

	"fmt"

	"github.com/hyperledger/fabric/core/scc/cscc"
	"github.com/hyperledger/fabric/core/scc/escc"
	"github.com/hyperledger/fabric/core/scc/lscc"
	"github.com/hyperledger/fabric/core/scc/mscc"
	"github.com/hyperledger/fabric/core/scc/qscc"
	"github.com/hyperledger/fabric/core/scc/snapsscc"
	"github.com/hyperledger/fabric/core/scc/vscc"
	pb "github.com/hyperledger/fabric/protos/peer"
	"github.com/spf13/viper"
)

//see systemchaincode_test.go for an example using "sample_syscc"
var systemChaincodes = []*SystemChaincode{
	{
		Enabled:           true,
		Name:              "cscc",
		Path:              "github.com/hyperledger/fabric/core/scc/cscc",
		InitArgs:          [][]byte{[]byte("")},
		Chaincode:         &cscc.PeerConfiger{},
		InvokableExternal: true, // cscc is invoked to join a channel
	},
	{
		Enabled:           true,
		Name:              "lscc",
		Path:              "github.com/hyperledger/fabric/core/scc/lscc",
		InitArgs:          [][]byte{[]byte("")},
		Chaincode:         &lscc.LifeCycleSysCC{},
		InvokableExternal: true, // lscc is invoked to deploy new chaincodes
		InvokableCC2CC:    true, // lscc can be invoked by other chaincodes
	},
	{
		Enabled:   true,
		Name:      "escc",
		Path:      "github.com/hyperledger/fabric/core/scc/escc",
		InitArgs:  [][]byte{[]byte("")},
		Chaincode: &escc.EndorserOneValidSignature{},
	},
	{
		Enabled:   true,
		Name:      "vscc",
		Path:      "github.com/hyperledger/fabric/core/scc/vscc",
		InitArgs:  [][]byte{[]byte("")},
		Chaincode: &vscc.ValidatorOneValidSignature{},
	},
	{
		Enabled:           true,
		Name:              "qscc",
		Path:              "github.com/hyperledger/fabric/core/chaincode/qscc",
		InitArgs:          [][]byte{[]byte("")},
		Chaincode:         &qscc.LedgerQuerier{},
		InvokableExternal: true, // qscc can be invoked to retrieve blocks
		InvokableCC2CC:    true, // qscc can be invoked to retrieve blocks also by a cc
	},
	{
		Enabled:           true,
		Name:              "snapsscc",
		Path:              "github.com/hyperledger/fabric/core/chaincode/snapsscc",
		InitArgs:          [][]byte{[]byte("")},
		Chaincode:         &snapsscc.SnapsSCC{},
		InvokableExternal: true, // snapsscc can be invoked externally
		InvokableCC2CC:    true, // snapsscc can be invoked by other chaincodes
	},
	{
		Enabled:           true,
		Name:              "mscc",
		Path:              "github.com/hyperledger/fabric/core/chaincode/mscc",
		InitArgs:          [][]byte{[]byte("")},
		Chaincode:         &mscc.MembershipSCC{},
		InvokableExternal: true, // mscc can be invoked externally
		InvokableCC2CC:    true, // mscc can be invoked by other chaincodes
	},
}

//RegisterSysCCs is the hook for system chaincodes where system chaincodes are registered with the fabric
//note the chaincode must still be deployed and launched like a user chaincode will be
func RegisterSysCCs() {
	err := loadExternalSysCCs()
	if err != nil {
		panic(fmt.Errorf("Error loading external system CCs: %v", err))
	}
	for _, sysCC := range systemChaincodes {
		RegisterSysCC(sysCC)
	}
}

//DeploySysCCs is the hook for system chaincodes where system chaincodes are registered with the fabric
//note the chaincode must still be deployed and launched like a user chaincode will be
func DeploySysCCs(chainID string) {
	for _, sysCC := range systemChaincodes {
		deploySysCC(chainID, sysCC)
	}
}

//DeDeploySysCCs is used in unit tests to stop and remove the system chaincodes before
//restarting them in the same process. This allows clean start of the system
//in the same process
func DeDeploySysCCs(chainID string) {
	for _, sysCC := range systemChaincodes {
		DeDeploySysCC(chainID, sysCC)
	}
}

//IsSysCC returns true if the name matches a system chaincode's
//system chaincode names are system, chain wide
func IsSysCC(name string) bool {
	for _, sysCC := range systemChaincodes {
		if sysCC.Name == name {
			return true
		}
	}
	return false
}

// IsSysCCAndNotInvokableExternal returns true if the chaincode
// is a system chaincode and *CANNOT* be invoked through
// a proposal to this peer
func IsSysCCAndNotInvokableExternal(name string) bool {
	for _, sysCC := range systemChaincodes {
		if sysCC.Name == name {
			return !sysCC.InvokableExternal
		}
	}
	return false
}

// IsSysCCAndNotInvokableCC2CC returns true if the chaincode
// is a system chaincode and *CANNOT* be invoked through
// a cc2cc invocation
func IsSysCCAndNotInvokableCC2CC(name string) bool {
	for _, sysCC := range systemChaincodes {
		if sysCC.Name == name {
			return !sysCC.InvokableCC2CC
		}
	}
	return false
}

// MockRegisterSysCCs is used only for testing
// This is needed to break import cycle
func MockRegisterSysCCs(mockSysCCs []*SystemChaincode) []*SystemChaincode {
	orig := systemChaincodes
	systemChaincodes = mockSysCCs
	RegisterSysCCs()
	return orig
}

// MockResetSysCCs restore orig system ccs - is used only for testing
func MockResetSysCCs(mockSysCCs []*SystemChaincode) {
	systemChaincodes = mockSysCCs
}

func loadExternalSysCCs() error {

	ccs := viper.GetStringMap("chaincode.systemext")

	sysccLogger.Infof("Loading %d external system chaincodes", len(ccs))

	for key := range ccs {
		escc := &SystemChaincode{
			Name:              key,
			Path:              viper.GetString(fmt.Sprintf("chaincode.systemext.%s.path", key)),
			ConfigPath:        viper.GetString(fmt.Sprintf("chaincode.systemext.%s.configPath", key)),
			Enabled:           viper.GetBool(fmt.Sprintf("chaincode.systemext.%s.enabled", key)),
			InvokableExternal: viper.GetBool(fmt.Sprintf("chaincode.systemext.%s.invokableExternal", key)),
			InvokableCC2CC:    viper.GetBool(fmt.Sprintf("chaincode.systemext.%s.invokableCC2CC", key)),
			InitArgs:          [][]byte{[]byte("")},
			Chaincode:         nil,
		}
		if escc.Path == "" {
			return fmt.Errorf("Error loading system chaincode %s: Path is not provided", key)
		}
		if escc.ConfigPath == "" {
			return fmt.Errorf("Error loading system chaincode %s: ConfigPath is not provided", key)
		}

		// Load chaincode type
		chaincodeTypeName := viper.GetString(fmt.Sprintf("chaincode.systemext.%s.chaincodeType", key))
		chaincodeType, ok := pb.ChaincodeSpec_Type_value[chaincodeTypeName]
		if !ok || chaincodeType == 0 {
			return fmt.Errorf("Error loading system chaincode %s: ChaincodeType is not provided", key)
		}
		escc.ChaincodeType = pb.ChaincodeSpec_Type(chaincodeType)

		// Load execution environment
		executionEnvironmentName := viper.GetString(fmt.Sprintf("chaincode.systemext.%s.executionEnvironment", key))
		executionEnvironment, ok := pb.ChaincodeDeploymentSpec_ExecutionEnvironment_value[executionEnvironmentName]
		if !ok || executionEnvironment == 0 {
			return fmt.Errorf("Error loading system chaincode %s: ExecutionEnvironment is not provided", key)
		}
		escc.ExecutionEnvironment = pb.ChaincodeDeploymentSpec_ExecutionEnvironment(executionEnvironment)

		if escc.ExecutionEnvironment == pb.ChaincodeDeploymentSpec_SYSTEM_EXT && escc.ChaincodeType != pb.ChaincodeSpec_GOLANG {
			// TODO: uncomment this line once the extcontroller properly handles this case
			// return fmt.Errorf("Error loading system chaincode %s: SYSTEM_EXT execution environment requires GOLANG chaincode type", key)
		}

		systemChaincodes = append(systemChaincodes, escc)
	}

	return nil
}
