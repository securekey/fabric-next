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
	err := loadRemoteSysCCs()
	if err != nil {
		panic(fmt.Errorf("Error loading remote system CCs: %v", err))
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

type SystemExtCCs struct {
	seccs map[string]SystemChaincode
}

func loadRemoteSysCCs() error {
	sysExtCCs := &SystemExtCCs{}
	err := viper.UnmarshalKey("chaincode.systemext", &sysExtCCs)

	if err != nil {
		return fmt.Errorf("Got error while loading External System CCs: %+v\n", err)
	}

	for key, escc := range sysExtCCs.seccs {
		// all External System CC must have External=true
		escc.External = true
		// set the name from the key
		escc.Name = key

		//for now harcoding InitArgs TODO remove when ready to read from config directly
		escc.InitArgs = [][]byte{[]byte("")}

		systemChaincodes = append(systemChaincodes, &escc)
	}

	return nil
}
