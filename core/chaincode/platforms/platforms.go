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

package platforms

import (
	"archive/tar"
	"fmt"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/chaincode/platforms/binary"
	"github.com/hyperledger/fabric/core/chaincode/platforms/car"
	"github.com/hyperledger/fabric/core/chaincode/platforms/golang"
	"github.com/hyperledger/fabric/core/chaincode/platforms/java"
	pb "github.com/hyperledger/fabric/protos/peer"
)

// Interface for validating the specification and and writing the package for
// the given platform
type Platform interface {
	ValidateSpec(spec *pb.ChaincodeSpec) error
	ValidateDeploymentSpec(spec *pb.ChaincodeDeploymentSpec) error
	GetDeploymentPayload(spec *pb.ChaincodeSpec) ([]byte, error)
	GenerateDockerfile(spec *pb.ChaincodeDeploymentSpec) (string, error)
	GenerateDockerBuild(spec *pb.ChaincodeDeploymentSpec, tw *tar.Writer) error
}

var logger = flogging.MustGetLogger("chaincode-platform")

// Find returns the platform interface for the given platform type
func Find(chaincodeType pb.ChaincodeSpec_Type) (Platform, error) {

	switch chaincodeType {
	case pb.ChaincodeSpec_GOLANG:
		return &golang.Platform{}, nil
	case pb.ChaincodeSpec_CAR:
		return &car.Platform{}, nil
	case pb.ChaincodeSpec_JAVA:
		return &java.Platform{}, nil
	case pb.ChaincodeSpec_BINARY:
		return &binary.Platform{}, nil
	default:
		return nil, fmt.Errorf("Unknown chaincodeType: %s", chaincodeType)
	}

}

func GetDeploymentPayload(spec *pb.ChaincodeSpec) ([]byte, error) {
	platform, err := Find(spec.Type)
	if err != nil {
		return nil, err
	}

	return platform.GetDeploymentPayload(spec)
}
