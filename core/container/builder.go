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

package container

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"

	"github.com/hyperledger/fabric/core/config"
	"github.com/hyperledger/fabric/core/container/dockercontroller"
	"github.com/hyperledger/fabric/core/container/extcontroller"
	pb "github.com/hyperledger/fabric/protos/peer"
	"github.com/spf13/viper"
)

func getPeerTLSCert() ([]byte, error) {

	if viper.GetBool("peer.tls.enabled") == false {
		// no need for certificates if TLS is not enabled
		return nil, nil
	}
	var path string
	// first we check for the rootcert
	path = config.GetPath("peer.tls.rootcert.file")
	if path == "" {
		// check for tls cert
		path = config.GetPath("peer.tls.cert.file")
	}
	// this should not happen if the peer is running with TLS enabled
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}
	// FIXME: FAB-2037 - ensure we sanely resolve relative paths specified in the yaml
	return ioutil.ReadFile(path)
}

// Build generates a container build and provides a reader to deliver the build when
// a container is created
func Build(cds *pb.ChaincodeDeploymentSpec) (io.Reader, error) {
	cert, err := getPeerTLSCert()
	if err != nil {
		return nil, fmt.Errorf("Failed to read the TLS certificate: %s", err)
	}
	if cds.ExecEnv == pb.ChaincodeDeploymentSpec_DOCKER {
		return dockercontroller.GenerateDockerBuild(cds, cert)
	} else if cds.ExecEnv == pb.ChaincodeDeploymentSpec_SYSTEM_EXT {
		return extcontroller.GenerateExtBuild(cds, cert)
	} else {
		return nil, fmt.Errorf("Failed to generate platform-specific build: execution environment is not supported: %s", pb.ChaincodeDeploymentSpec_ExecutionEnvironment_name[int32(cds.ExecEnv)])
	}
}
