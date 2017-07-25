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

package dockercontroller

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"strings"

	"github.com/hyperledger/fabric/common/metadata"
	"github.com/hyperledger/fabric/core/chaincode/platforms"
	cutil "github.com/hyperledger/fabric/core/container/util"
	pb "github.com/hyperledger/fabric/protos/peer"
)

// InputFiles ...
type InputFiles map[string][]byte

// GenerateDockerBuild build a reader that will be used for creating a docker image
func GenerateDockerBuild(cds *pb.ChaincodeDeploymentSpec, cert []byte) (io.Reader, error) {
	inputFiles := make(InputFiles)

	// ----------------------------------------------------------------------------------------------------
	// Determine our platform driver from the spec
	// ----------------------------------------------------------------------------------------------------
	platform, err := platforms.Find(cds.ChaincodeSpec.Type)
	if err != nil {
		return nil, fmt.Errorf("Failed to determine platform type: %s", err)
	}

	// ----------------------------------------------------------------------------------------------------
	// Transfer the peer's TLS certificate to our list of input files, if applicable
	// ----------------------------------------------------------------------------------------------------
	// NOTE: We bake the peer TLS certificate in at the time we build the chaincode container if a cert is
	// found, regardless of whether TLS is enabled or not.  The main implication is that if the administrator
	// updates the peer cert, the chaincode containers will need to be invalidated and rebuilt.
	// We will manage enabling or disabling TLS at container run time via CORE_PEER_TLS_ENABLED
	inputFiles["peer.crt"] = cert

	// ----------------------------------------------------------------------------------------------------
	// Generate the Dockerfile specific to our context
	// ----------------------------------------------------------------------------------------------------
	dockerFile, err := generateDockerfile(platform, cds, cert != nil)
	if err != nil {
		return nil, fmt.Errorf("Failed to generate a Dockerfile: %s", err)
	}

	inputFiles["Dockerfile"] = dockerFile

	// ----------------------------------------------------------------------------------------------------
	// Finally, launch an asynchronous process to stream all of the above into a docker build context
	// ----------------------------------------------------------------------------------------------------
	input, output := io.Pipe()

	go func() {
		gw := gzip.NewWriter(output)
		tw := tar.NewWriter(gw)

		err := generateDockerBuild(platform, cds, inputFiles, tw)
		if err != nil {
			dockerLogger.Error(err)
		}

		tw.Close()
		gw.Close()
		output.CloseWithError(err)
	}()

	return input, nil
}

func generateDockerBuild(platform platforms.Platform, cds *pb.ChaincodeDeploymentSpec, inputFiles InputFiles, tw *tar.Writer) error {

	var err error

	// ----------------------------------------------------------------------------------------------------
	// First stream out our static inputFiles
	// ----------------------------------------------------------------------------------------------------
	for name, data := range inputFiles {
		err = cutil.WriteBytesToPackage(name, data, tw)
		if err != nil {
			return fmt.Errorf("Failed to inject \"%s\": %s", name, err)
		}
	}

	registeredSCC := sccTypeRegistry[cds.ChaincodeSpec.ChaincodeId.Path]
	if registeredSCC != nil {
		// This is a registered SCC, add configuration to the build
		if registeredSCC.configPath != "" {
			binpackage := bytes.NewBuffer(nil)
			cfgtw := tar.NewWriter(binpackage)
			defer cfgtw.Close()
			if err := cutil.WriteFolderToTarPackage(cfgtw, registeredSCC.configPath, "", "", nil, nil); err != nil {
				return err
			}
			if err := cutil.WriteBytesToPackage("config.tar", binpackage.Bytes(), tw); err != nil {
				return err
			}
		}
	}

	// ----------------------------------------------------------------------------------------------------
	// Now give the platform an opportunity to contribute its own context to the build
	// ----------------------------------------------------------------------------------------------------
	err = platform.GenerateDockerBuild(cds, tw)
	if err != nil {
		return fmt.Errorf("Failed to generate platform-specific docker build: %s", err)
	}

	return nil
}

func generateDockerfile(platform platforms.Platform, cds *pb.ChaincodeDeploymentSpec, tls bool) ([]byte, error) {

	var buf []string

	// ----------------------------------------------------------------------------------------------------
	// Let the platform define the base Dockerfile
	// ----------------------------------------------------------------------------------------------------
	base, err := platform.GenerateDockerfile(cds)
	if err != nil {
		return nil, fmt.Errorf("Failed to generate platform-specific Dockerfile: %s", err)
	}

	buf = append(buf, base)

	// ----------------------------------------------------------------------------------------------------
	// Add some handy labels
	// ----------------------------------------------------------------------------------------------------
	buf = append(buf, fmt.Sprintf("LABEL %s.chaincode.id.name=\"%s\" \\", metadata.BaseDockerLabel, cds.ChaincodeSpec.ChaincodeId.Name))
	buf = append(buf, fmt.Sprintf("      %s.chaincode.id.version=\"%s\" \\", metadata.BaseDockerLabel, cds.ChaincodeSpec.ChaincodeId.Version))
	buf = append(buf, fmt.Sprintf("      %s.chaincode.type=\"%s\" \\", metadata.BaseDockerLabel, cds.ChaincodeSpec.Type.String()))
	buf = append(buf, fmt.Sprintf("      %s.version=\"%s\" \\", metadata.BaseDockerLabel, metadata.Version))
	buf = append(buf, fmt.Sprintf("      %s.base.version=\"%s\"", metadata.BaseDockerLabel, metadata.BaseVersion))

	// ----------------------------------------------------------------------------------------------------
	// Then augment it with any general options
	// ----------------------------------------------------------------------------------------------------
	//append version so chaincode build version can be campared against peer build version
	buf = append(buf, fmt.Sprintf("ENV CORE_CHAINCODE_BUILDLEVEL=%s", metadata.Version))

	if tls {
		const guestTLSPath = "/etc/hyperledger/fabric/peer.crt"

		buf = append(buf, "ENV CORE_PEER_TLS_ROOTCERT_FILE="+guestTLSPath)
		buf = append(buf, "COPY peer.crt "+guestTLSPath)
	}

	registeredSCC := sccTypeRegistry[cds.ChaincodeSpec.ChaincodeId.Path]
	if registeredSCC != nil {
		// This is a registered SCC, copy the SCC configuration
		// This copy assumes that the chaincode working directory is '/'
		if registeredSCC.configPath != "" {
			buf = append(buf, "ADD config.tar /")
		}
	}

	// ----------------------------------------------------------------------------------------------------
	// Finalize it
	// ----------------------------------------------------------------------------------------------------
	contents := strings.Join(buf, "\n")
	dockerLogger.Debugf("\n%s", contents)

	return []byte(contents), nil
}
