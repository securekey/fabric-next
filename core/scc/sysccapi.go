/*
Copyright IBM Corp. 2016 All Rights Reserved.

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

	"golang.org/x/net/context"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/core/chaincode/shim"
	"github.com/hyperledger/fabric/core/common/ccprovider"
	"github.com/hyperledger/fabric/core/container/extcontroller"
	"github.com/hyperledger/fabric/core/container/inproccontroller"
	"github.com/hyperledger/fabric/core/peer"

	"github.com/spf13/viper"

	pb "github.com/hyperledger/fabric/protos/peer"
)

var sysccLogger = flogging.MustGetLogger("sccapi")

// SystemChaincode defines the metadata needed to initialize system chaincode
// when the fabric comes up. SystemChaincodes are installed by adding an
// entry in importsysccs.go
type SystemChaincode struct {
	//Unique name of the system chaincode
	Name string

	//InitArgs initialization arguments to startup the system chaincode
	InitArgs [][]byte

	// InvokableExternal keeps track of whether
	// this system chaincode can be invoked
	// through a proposal sent to this peer
	InvokableExternal bool

	// InvokableCC2CC keeps track of whether
	// this system chaincode can be invoked
	// by way of a chaincode-to-chaincode
	// invocation
	InvokableCC2CC bool

	// Enabled a convenient switch to enable/disable system chaincode without
	// having to remove its definition
	Enabled bool

	// Chaincode is the actual chaincode object
	// If not nil, this object will be used as the in-process SCC
	Chaincode shim.Chaincode

	// --------------------------------------------------------------------------
	// Properties below are used only if the Chaincode property is nil, that is,
	// if this is an out of process SCC
	// --------------------------------------------------------------------------

	// Chaincode type
	ChaincodeType pb.ChaincodeSpec_Type

	// Path to the system chaincode. The path content depends on the chaincode type.
	Path string

	// Path to the system chaincode configuration.
	// When the SCC is deployed to its own docker container, ConfigPath is mounted as a volume.
	// When the SCC is spawned as a process alongside the peer process, ConfigPath is
	// passed to the process as the current working directory.
	ConfigPath string

	// By default, an out of process chaincode is deployed to its own docker container.
	// A GOLANG chaincode can specify InPeerContainer=true, to be deployed as a process
	// alongside the peer process.
	InPeerContainer bool
}

func (syscc *SystemChaincode) isExternal() bool {
	return syscc.Chaincode == nil
}

// RegisterSysCC registers the given system chaincode with the peer
func RegisterSysCC(syscc *SystemChaincode) error {
	if !syscc.Enabled || !isWhitelisted(syscc) {
		sysccLogger.Info(fmt.Sprintf("system chaincode (%s,%s,%t) disabled", syscc.Name, syscc.Path, syscc.Enabled))
		return nil
	}

	if syscc.isExternal() {
		err := extcontroller.Register(syscc.Path, syscc.Chaincode)
		if err != nil {
			errStr := fmt.Sprintf("could not register (%s,%v): %s", syscc.Path, syscc, err)
			sysccLogger.Error(errStr)
			return fmt.Errorf(errStr)
		}
		sysccLogger.Infof("system chaincode %s(%s) registered", syscc.Name, syscc.Path)
		return nil
	}

	err := inproccontroller.Register(syscc.Path, syscc.Chaincode)
	if err != nil {
		//if the type is registered, the instance may not be... keep going
		if _, ok := err.(inproccontroller.SysCCRegisteredErr); !ok {
			errStr := fmt.Sprintf("could not register (%s,%v): %s", syscc.Path, syscc, err)
			sysccLogger.Error(errStr)
			return fmt.Errorf(errStr)
		}
	}

	sysccLogger.Infof("system chaincode %s(%s) registered", syscc.Name, syscc.Path)
	return err
}

// deploySysCC deploys the given system chaincode on a chain
func deploySysCC(chainID string, syscc *SystemChaincode) error {
	if !syscc.Enabled || !isWhitelisted(syscc) {
		sysccLogger.Info(fmt.Sprintf("system chaincode (%s,%s) disabled", syscc.Name, syscc.Path))
		return nil
	}

	var err error

	ccprov := ccprovider.GetChaincodeProvider()

	ctxt := context.Background()
	if chainID != "" {
		lgr := peer.GetLedger(chainID)
		if lgr == nil {
			panic(fmt.Sprintf("syschain %s start up failure - unexpected nil ledger for channel %s", syscc.Name, chainID))
		}

		_, err := ccprov.GetContext(lgr)
		if err != nil {
			return err
		}

		defer ccprov.ReleaseContext()
	}

	chaincodeID := &pb.ChaincodeID{Path: syscc.Path, Name: syscc.Name}
	spec := &pb.ChaincodeSpec{Type: pb.ChaincodeSpec_Type(pb.ChaincodeSpec_Type_value["GOLANG"]), ChaincodeId: chaincodeID, Input: &pb.ChaincodeInput{Args: syscc.InitArgs}}

	// First build and get the deployment spec
	chaincodeDeploymentSpec, err := buildSysCC(ctxt, spec, syscc.isExternal())

	if err != nil {
		sysccLogger.Error(fmt.Sprintf("Error deploying chaincode spec: %v\n\n error: %s", spec, err))
		return err
	}

	txid := util.GenerateUUID()

	version := util.GetSysCCVersion()

	cccid := ccprov.GetCCContext(chainID, chaincodeDeploymentSpec.ChaincodeSpec.ChaincodeId.Name, version, txid, true, nil, nil)

	_, _, err = ccprov.ExecuteWithErrorFilter(ctxt, cccid, chaincodeDeploymentSpec)

	sysccLogger.Infof("system chaincode %s/%s(%s) deployed", syscc.Name, chainID, syscc.Path)

	return err
}

// DeDeploySysCC stops the system chaincode and deregisters it from inproccontroller
func DeDeploySysCC(chainID string, syscc *SystemChaincode) error {
	chaincodeID := &pb.ChaincodeID{Path: syscc.Path, Name: syscc.Name}
	spec := &pb.ChaincodeSpec{Type: pb.ChaincodeSpec_Type(pb.ChaincodeSpec_Type_value["GOLANG"]), ChaincodeId: chaincodeID, Input: &pb.ChaincodeInput{Args: syscc.InitArgs}}

	ctx := context.Background()
	// First build and get the deployment spec
	chaincodeDeploymentSpec, err := buildSysCC(ctx, spec, syscc.isExternal())

	if err != nil {
		sysccLogger.Error(fmt.Sprintf("Error deploying chaincode spec: %v\n\n error: %s", spec, err))
		return err
	}

	ccprov := ccprovider.GetChaincodeProvider()

	version := util.GetSysCCVersion()

	cccid := ccprov.GetCCContext(chainID, syscc.Name, version, "", true, nil, nil)

	err = ccprov.Stop(ctx, cccid, chaincodeDeploymentSpec)

	return err
}

// buildLocal builds a given chaincode code
func buildSysCC(context context.Context, spec *pb.ChaincodeSpec, isExternal bool) (*pb.ChaincodeDeploymentSpec, error) {
	var codePackageBytes []byte
	execEnv := pb.ChaincodeDeploymentSpec_SYSTEM
	if isExternal {
		execEnv = pb.ChaincodeDeploymentSpec_SYSTEM_EXT
	}
	chaincodeDeploymentSpec := &pb.ChaincodeDeploymentSpec{ExecEnv: execEnv, ChaincodeSpec: spec, CodePackage: codePackageBytes}
	return chaincodeDeploymentSpec, nil
}

func isWhitelisted(syscc *SystemChaincode) bool {
	chaincodes := viper.GetStringMapString("chaincode.system")
	val, ok := chaincodes[syscc.Name]
	enabled := val == "enable" || val == "true" || val == "yes"

	return ok && enabled
}
