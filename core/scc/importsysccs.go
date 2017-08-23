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
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/hyperledger/fabric/core/aclmgmt"
	"github.com/hyperledger/fabric/core/common/ccprovider"
	pb "github.com/hyperledger/fabric/protos/peer"
	"github.com/spf13/viper"
	//import system chain codes here
	"github.com/hyperledger/fabric/core/scc/cscc"
	"github.com/hyperledger/fabric/core/scc/escc"
	"github.com/hyperledger/fabric/core/scc/lscc"
	"github.com/hyperledger/fabric/core/scc/mscc"
	"github.com/hyperledger/fabric/core/scc/qscc"
	"github.com/hyperledger/fabric/core/scc/rscc"
	"github.com/hyperledger/fabric/core/scc/vscc"
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
		Chainless:         false,
	},
	{
		Enabled:           true,
		Name:              "lscc",
		Path:              "github.com/hyperledger/fabric/core/scc/lscc",
		InitArgs:          [][]byte{[]byte("")},
		Chaincode:         &lscc.LifeCycleSysCC{},
		InvokableExternal: true, // lscc is invoked to deploy new chaincodes
		InvokableCC2CC:    true, // lscc can be invoked by other chaincodes
		Chainless:         false,
	},
	{
		Enabled:   true,
		Name:      "escc",
		Path:      "github.com/hyperledger/fabric/core/scc/escc",
		InitArgs:  [][]byte{[]byte("")},
		Chaincode: &escc.EndorserOneValidSignature{},
		Chainless: false,
	},
	{
		Enabled:   true,
		Name:      "vscc",
		Path:      "github.com/hyperledger/fabric/core/scc/vscc",
		InitArgs:  [][]byte{[]byte("")},
		Chaincode: &vscc.ValidatorOneValidSignature{},
		Chainless: false,
	},
	{
		Enabled:           true,
		Name:              "qscc",
		Path:              "github.com/hyperledger/fabric/core/chaincode/qscc",
		InitArgs:          [][]byte{[]byte("")},
		Chaincode:         &qscc.LedgerQuerier{},
		InvokableExternal: true, // qscc can be invoked to retrieve blocks
		InvokableCC2CC:    true, // qscc can be invoked to retrieve blocks also by a cc
		Chainless:         false,
	},
	{
		Enabled:           true,
		Name:              "rscc",
		Path:              "github.com/hyperledger/fabric/core/chaincode/rscc",
		InitArgs:          [][]byte{[]byte("")},
		Chaincode:         rscc.NewRscc(),
		InvokableExternal: true,  // rscc can be invoked to update policies
		InvokableCC2CC:    false, // rscc cannot be invoked from a cc
		Chainless:         false,
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
	extSccEnabled := viper.GetBool("chaincode.systemext.enabled")
	if !extSccEnabled {
		sysccLogger.Info("External System CCs feature is disabled.")
	} else {
		err := loadExternalSysCCs()
		if err != nil {
			panic(fmt.Errorf("Error loading external system CCs: %v", err))
		}
	}

	var aclProvider aclmgmt.ACLProvider
	for _, sysCC := range systemChaincodes {
		if reg, _ := registerSysCC(sysCC); reg {
			//rscc is registered, lets make it the aclProvider
			if sysCC.Name == "rscc" {
				aclProvider = sysCC.Chaincode.(aclmgmt.ACLProvider)
			}
		}
	}
	//a nil aclProvider will initialize defaultACLProvider
	//which will provide 1.0 ACL defaults
	aclmgmt.RegisterACLProvider(aclProvider)

}

//DeploySysCCs is the hook for system chaincodes where system chaincodes are registered with the fabric
//note the chaincode must still be deployed and launched like a user chaincode will be
func DeploySysCCs(chainID string) {
	for _, sysCC := range systemChaincodes {
		if chainID == "" || !sysCC.Chainless {
			deploySysCC(chainID, sysCC)
		}
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
func MockResetSysCCs(origSysCCs []*SystemChaincode) {
	systemChaincodes = origSysCCs
}

func loadExternalSysCCs() error {
	extSccCDSPath := viper.GetString("chaincode.systemext.cds.path")
	if extSccCDSPath == "" {
		sysccLogger.Info("extSccCDSPath is not specified, won't load any external system chaincodes")
		return nil
	}

	fi, err := os.Stat(extSccCDSPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("External SCC directory is missing: %s", extSccCDSPath)
		}
		return err
	}
	if !fi.IsDir() {
		return fmt.Errorf("extSccCDSPath is not a directory: %s", extSccCDSPath)
	}

	sysccLogger.Infof("Loading external SCCs from CDS path: %s", extSccCDSPath)
	err = filepath.Walk(extSccCDSPath, func(path string, f os.FileInfo, err error) error {
		if err == nil {
			if path == extSccCDSPath {
				return nil
			}
			if f.IsDir() {
				sysccLogger.Infof("Skipping loading SCC from directory: %s", path)
				return nil
			}
			sysccLogger.Infof("Loading external SCC from CDS path: %s", path)
			ccbytes, err := ioutil.ReadFile(path)
			if err != nil {
				return fmt.Errorf("Error reading from file %s: %v", path, err)
			}
			sysccLogger.Debugf("%d bytes read from file %s", len(ccbytes), path)
			ccpack := ccprovider.CDSPackage{}
			_, err = ccpack.InitFromBuffer(ccbytes)
			if err != nil {
				return err
			}
			cds := ccpack.GetDepSpec()
			ccName := cds.GetChaincodeSpec().GetChaincodeId().GetName()

			if !viper.IsSet(fmt.Sprintf("chaincode.systemext.%s.Enabled", ccName)) {
				sysccLogger.Infof("Found %s as Ext SCC CDS path, but it is not registered in the configs. Ignoring..", path)
				return nil
			}

			cds.ExecEnv = pb.ChaincodeDeploymentSpec_SYSTEM_EXT

			scc := &SystemChaincode{
				Name:              ccName,
				InitArgs:          [][]byte{[]byte("")},
				Chaincode:         nil,
				InvokableExternal: viper.GetBool(fmt.Sprintf("chaincode.systemext.%s.InvokableExternal", ccName)),
				InvokableCC2CC:    viper.GetBool(fmt.Sprintf("chaincode.systemext.%s.InvokableCC2CC", ccName)),
				Enabled:           viper.GetBool(fmt.Sprintf("chaincode.systemext.%s.Enabled", ccName)),
				ConfigPath:        viper.GetString(fmt.Sprintf("chaincode.systemext.%s.ConfigPath", ccName)),
				CDS:               cds,
				Chainless:         viper.GetBool(fmt.Sprintf("chaincode.systemext.%s.Chainless", ccName)),
			}
			systemChaincodes = append(systemChaincodes, scc)
			// Whitelist if it's enabled
			if scc.Enabled {
				chaincodes := viper.GetStringMapString("chaincode.system")
				chaincodes[ccName] = "enable"
				viper.Set("chaincode.system", chaincodes)
			}
		}
		return nil
	})
	return err
}
