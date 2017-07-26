/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package container

import (
	"fmt"
	"io"

	"github.com/hyperledger/fabric/core/container/dockercontroller"
	"github.com/hyperledger/fabric/core/container/extcontroller"
	"github.com/hyperledger/fabric/core/container/util"
	pb "github.com/hyperledger/fabric/protos/peer"
)

// Build generates a container build and provides a reader to deliver the build when
// a container is created
func Build(cds *pb.ChaincodeDeploymentSpec) (io.Reader, error) {
	cert, err := util.GetPeerTLSCert()
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
