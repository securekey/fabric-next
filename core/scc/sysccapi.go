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

	//Path to the system chaincode; currently not used
	Path string

	//InitArgs initialization arguments to startup the system chaincode
	InitArgs [][]byte

	// Chaincode is the actual chaincode object
	Chaincode shim.Chaincode

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
	// having to remove entry from importsysccs.go
	Enabled bool

	// Chaincode deployment spec
	CDS *pb.ChaincodeDeploymentSpec

	// When the SCC is spawned as a process alongside the peer process, ConfigPath is
	// passed to the process as the current working directory.
	ConfigPath string

	// External system chaincodes can be configured to be chainless
	Chainless bool
}

// registerSysCC registers the given system chaincode with the peer
func registerSysCC(syscc *SystemChaincode) (bool, error) {
	if !syscc.Enabled || !isWhitelisted(syscc) {
		sysccLogger.Info(fmt.Sprintf("system chaincode (%s,%s,%t) disabled", syscc.Name, syscc.Path, syscc.Enabled))
		return false, nil
	}

	ok := true
	var err error
	var path string

	if syscc.CDS != nil && syscc.CDS.ExecEnv == pb.ChaincodeDeploymentSpec_SYSTEM_EXT {
		path = syscc.CDS.ChaincodeSpec.ChaincodeId.Path
		err = extcontroller.Register(path, nil, syscc.CDS.ChaincodeSpec.GetType(), syscc.ConfigPath)
		if err != nil {
			_, ok = err.(extcontroller.ExtSysCCRegisteredErr)
		}
	} else {
		path = syscc.Path
		err = inproccontroller.Register(path, syscc.Chaincode)
		if err != nil {
			_, ok = err.(inproccontroller.SysCCRegisteredErr)
		}
	}

	if !ok {
		errStr := fmt.Sprintf("could not register (%s,%v): %s", path, syscc, err)
		sysccLogger.Error(errStr)
		return false, fmt.Errorf(errStr)
	}

	sysccLogger.Infof("system chaincode %s(%s) registered", syscc.Name, path)
	return true, err
}

// deploySysCC deploys the given system chaincode on a chain
func deploySysCC(chainID string, syscc *SystemChaincode) error {
	if !syscc.Enabled || !isWhitelisted(syscc) {
		sysccLogger.Info(fmt.Sprintf("system chaincode (%s,%s) disabled", syscc.Name, syscc.Path))
		return nil
	}

	var err error

	ccprov := ccprovider.GetChaincodeProvider()

	txid := util.GenerateUUID()

	ctxt := context.Background()
	if chainID != "" {
		lgr := peer.GetLedger(chainID)
		if lgr == nil {
			panic(fmt.Sprintf("syschain %s start up failure - unexpected nil ledger for channel %s", syscc.Name, chainID))
		}

		//init can do GetState (and other Get's) even if Puts cannot be
		//be handled. Need ledger for this
		ctxt2, txsim, err := ccprov.GetContext(lgr, txid)
		if err != nil {
			return err
		}

		ctxt = ctxt2

		defer txsim.Done()
	}

	var cds *pb.ChaincodeDeploymentSpec
	var version string

	if syscc.CDS != nil && syscc.CDS.ExecEnv == pb.ChaincodeDeploymentSpec_SYSTEM_EXT {
		cds = syscc.CDS
		version = syscc.CDS.ChaincodeSpec.ChaincodeId.Version
	} else {
		chaincodeID := &pb.ChaincodeID{Path: syscc.Path, Name: syscc.Name}
		spec := &pb.ChaincodeSpec{Type: pb.ChaincodeSpec_Type(pb.ChaincodeSpec_Type_value["GOLANG"]), ChaincodeId: chaincodeID, Input: &pb.ChaincodeInput{Args: syscc.InitArgs}}
		cds, err = buildSysCC(ctxt, spec)

		if err != nil {
			sysccLogger.Error(fmt.Sprintf("Error deploying chaincode spec: %v\n\n error: %s", spec, err))
			return err
		}

		version = util.GetSysCCVersion()
	}

	cccid := ccprov.GetCCContext(chainID, cds.ChaincodeSpec.ChaincodeId.Name, version, txid, true, nil, nil)

	_, _, err = ccprov.ExecuteWithErrorFilter(ctxt, cccid, cds)

	sysccLogger.Infof("system chaincode %s/%s(%s) deployed", syscc.Name, chainID, syscc.Path)

	return err
}

// DeDeploySysCC stops the system chaincode and deregisters it from inproccontroller
func DeDeploySysCC(chainID string, syscc *SystemChaincode) error {
	var cds *pb.ChaincodeDeploymentSpec
	var version string
	var err error

	ctxt := context.Background()

	if syscc.CDS != nil && syscc.CDS.ExecEnv == pb.ChaincodeDeploymentSpec_SYSTEM_EXT {
		cds = syscc.CDS
		version = syscc.CDS.ChaincodeSpec.ChaincodeId.Version
	} else {
		chaincodeID := &pb.ChaincodeID{Path: syscc.Path, Name: syscc.Name}
		spec := &pb.ChaincodeSpec{Type: pb.ChaincodeSpec_Type(pb.ChaincodeSpec_Type_value["GOLANG"]), ChaincodeId: chaincodeID, Input: &pb.ChaincodeInput{Args: syscc.InitArgs}}
		cds, err = buildSysCC(ctxt, spec)

		if err != nil {
			sysccLogger.Error(fmt.Sprintf("Error deploying chaincode spec: %v\n\n error: %s", spec, err))
			return err
		}

		version = util.GetSysCCVersion()
	}

	ccprov := ccprovider.GetChaincodeProvider()

	cccid := ccprov.GetCCContext(chainID, syscc.Name, version, "", true, nil, nil)

	return ccprov.Stop(ctxt, cccid, cds)
}

// buildLocal builds a given chaincode code
func buildSysCC(context context.Context, spec *pb.ChaincodeSpec) (*pb.ChaincodeDeploymentSpec, error) {
	var codePackageBytes []byte
	chaincodeDeploymentSpec := &pb.ChaincodeDeploymentSpec{ExecEnv: pb.ChaincodeDeploymentSpec_SYSTEM, ChaincodeSpec: spec, CodePackage: codePackageBytes}
	return chaincodeDeploymentSpec, nil
}

func isWhitelisted(syscc *SystemChaincode) bool {
	chaincodes := viper.GetStringMapString("chaincode.system")
	val, ok := chaincodes[syscc.Name]
	enabled := val == "enable" || val == "true" || val == "yes"
	return ok && enabled
}
