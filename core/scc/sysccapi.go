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
	"github.com/hyperledger/fabric/core/container/dockercontroller"
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

	// The chaincode path.
	// Used only when ExecutionEnvironment == SYSTEM
	Path string

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
	// Used only when ExecutionEnvironment == SYSTEM
	Chaincode shim.Chaincode

	// Chaincode deployment spec
	CDS *pb.ChaincodeDeploymentSpec

	// Path to the system chaincode configuration.
	// Used onlu when ExecutionEnvironment != SYSTEM
	// When the SCC is deployed to docker, ConfigPath is added to the image.
	// When the SCC is spawned as a process alongside the peer process, ConfigPath is
	// passed to the process as the current working directory.
	ConfigPath string
}

// RegisterSysCC registers the given system chaincode with the peer
func RegisterSysCC(syscc *SystemChaincode) error {
	if !syscc.Enabled || !isWhitelisted(syscc) {
		sysccLogger.Info(fmt.Sprintf("system chaincode (%s,%t) disabled", syscc.Name, syscc.Enabled))
		return nil
	}

	ok := true
	var err error
	path := syscc.CDS.ChaincodeSpec.ChaincodeId.Path

	switch syscc.CDS.ExecEnv {
	case pb.ChaincodeDeploymentSpec_DOCKER:
		err = dockercontroller.Register(path, nil, syscc.CDS.ChaincodeSpec.GetType(), syscc.ConfigPath)
		if err != nil {
			_, ok = err.(dockercontroller.DockerSCCRegisteredErr)
		}
	case pb.ChaincodeDeploymentSpec_SYSTEM_EXT:
		err = extcontroller.Register(path, nil, syscc.CDS.ChaincodeSpec.GetType(), syscc.ConfigPath)
		if err != nil {
			_, ok = err.(extcontroller.ExtSysCCRegisteredErr)
		}
	default:
		err = inproccontroller.Register(path, syscc.Chaincode)
		if err != nil {
			_, ok = err.(inproccontroller.SysCCRegisteredErr)
		}
	}

	//if the type is registered, the instance may not be... keep going
	if !ok {
		errStr := fmt.Sprintf("could not register (%s,%v): %s", path, syscc, err)
		sysccLogger.Error(errStr)
		return fmt.Errorf(errStr)
	}
	sysccLogger.Infof("system chaincode %s(%s) registered", syscc.Name, path)
	return err
}

// deploySysCC deploys the given system chaincode on a chain
func deploySysCC(chainID string, syscc *SystemChaincode) error {
	if !syscc.Enabled || !isWhitelisted(syscc) {
		sysccLogger.Info(fmt.Sprintf("system chaincode (%s,%s) disabled", syscc.Name, syscc.CDS.ChaincodeSpec.ChaincodeId.Path))
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

	if err != nil {
		sysccLogger.Error(fmt.Sprintf("Error deploying chaincode spec: %v\n\n error: %s", syscc.CDS, err))
		return err
	}

	txid := util.GenerateUUID()

	version := syscc.CDS.ChaincodeSpec.ChaincodeId.Version

	cccid := ccprov.GetCCContext(chainID, syscc.CDS.ChaincodeSpec.ChaincodeId.Name, version, txid, true, nil, nil)

	_, _, err = ccprov.ExecuteWithErrorFilter(ctxt, cccid, syscc.CDS)

	sysccLogger.Infof("system chaincode %s/%s(%s) deployed", syscc.Name, chainID, syscc.CDS.ChaincodeSpec.ChaincodeId.Path)

	return err
}

// DeDeploySysCC stops the system chaincode and deregisters it from inproccontroller
func DeDeploySysCC(chainID string, syscc *SystemChaincode) error {

	ctx := context.Background()

	ccprov := ccprovider.GetChaincodeProvider()

	version := util.GetSysCCVersion()

	cccid := ccprov.GetCCContext(chainID, syscc.Name, version, "", true, nil, nil)

	err := ccprov.Stop(ctx, cccid, syscc.CDS)

	return err
}

func isWhitelisted(syscc *SystemChaincode) bool {
	chaincodes := viper.GetStringMapString("chaincode.system")
	val, ok := chaincodes[syscc.Name]
	enabled := val == "enable" || val == "true" || val == "yes"

	return ok && enabled
}
