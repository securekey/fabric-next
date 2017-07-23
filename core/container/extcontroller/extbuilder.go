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

package extcontroller

import (
	"compress/gzip"
	"fmt"
	"io"
	"path/filepath"

	"os"
	"strings"

	"github.com/hyperledger/fabric/core/chaincode/platforms/util"
	"github.com/hyperledger/fabric/core/container/dockercontroller"

	pb "github.com/hyperledger/fabric/protos/peer"
)

// GenerateExtBuild takes a chaincode deployment spec, builds the chaincode
// source by using a Docker container, and then extracts the chaincode binary
// so that it can be used for execution within a process or Docker container
func GenerateExtBuild(cds *pb.ChaincodeDeploymentSpec, cert []byte) (io.Reader, error) {
	curDir, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("Error getting current directory: %v", err)
	}

	builder := func() (io.Reader, error) { return dockercontroller.GenerateDockerBuild(cds, cert) }

	reader, err := builder()
	if err != nil {
		return nil, fmt.Errorf("Error building chaincode in Docker container: %v", err)
	}

	gr, err := gzip.NewReader(reader)
	if err != nil {
		return nil, fmt.Errorf("Error opening gzip reader for code package: %v", err)
	}

	binPkgOutputPath := filepath.Join(curDir, "/binpackage.tar")

	err = util.ExtractFileFromTar(gr, "binpackage.tar", binPkgOutputPath)
	gr.Close()
	if err != nil {
		return nil, fmt.Errorf("Error extracting binpackage.tar from code package: %v", err)
	}

	binPkgTarFile, err := os.Open(binPkgOutputPath)
	if err != nil {
		return nil, fmt.Errorf("Error opening binpackage.tar: %v", err)
	}

	ccBinOutPath := filepath.Join(curDir, "/", cds.ChaincodeSpec.ChaincodeId.Name)

	err = util.ExtractFileFromTar(binPkgTarFile, "./chaincode", ccBinOutPath)
	binPkgTarFile.Close()
	if err != nil {
		return nil, fmt.Errorf("Error extracting chaincode binary from binpackage.tar: %v", err)
	}

	return strings.NewReader(ccBinOutPath), nil
}
