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

package binary

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"sort"

	"bufio"

	"io"
	"time"

	"github.com/hyperledger/fabric/core/chaincode/platforms/golang"
	cutil "github.com/hyperledger/fabric/core/container/util"
	pb "github.com/hyperledger/fabric/protos/peer"
)

// Platform for chaincodes written in Go
type Platform struct {
}

// Returns whether the given file or directory exists or not
func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return true, err
}

func getGopath() (string, error) {
	env, err := golang.GetGoEnv()
	if err != nil {
		return "", err
	}
	// Only take the first element of GOPATH
	splitGoPath := filepath.SplitList(env["GOPATH"])
	if len(splitGoPath) == 0 {
		return "", fmt.Errorf("invalid GOPATH environment variable value:[%s]", env["GOPATH"])
	}
	return splitGoPath[0], nil
}

// ValidateSpec validates Go binary chaincodes
func (binaryPlatform *Platform) ValidateSpec(spec *pb.ChaincodeSpec) error {
	path, err := url.Parse(spec.ChaincodeId.Path)
	if err != nil || path == nil {
		return fmt.Errorf("invalid path: %s", err)
	}

	//we have no real good way of checking existence of remote urls except by downloading and testin
	//which we do later anyway. But we *can* - and *should* - test for existence of local paths.
	//Treat empty scheme as a local filesystem path
	if path.Scheme == "" {
		gopath, err := getGopath()
		if err != nil {
			return err
		}
		pathToCheck := filepath.Join(gopath, "src", spec.ChaincodeId.Path)
		exists, err := pathExists(pathToCheck)
		if err != nil {
			return fmt.Errorf("error validating chaincode path: %s", err)
		}
		if !exists {
			return fmt.Errorf("path to chaincode does not exist: %s", spec.ChaincodeId.Path)
		}
	}
	return nil
}

func (binaryPlatform *Platform) ValidateDeploymentSpec(cds *pb.ChaincodeDeploymentSpec) error {

	if cds.CodePackage == nil || len(cds.CodePackage) == 0 {
		// Nothing to validate if no CodePackage was included
		return nil
	}

	//We do not want to allow something like ./pkg/shady.a to be installed under
	// $GOPATH within the container.
	//
	// It should be noted that we cannot catch every threat with these techniques.  Therefore,
	// the container itself needs to be the last line of defense and be configured to be
	// resilient in enforcing constraints. However, we should still do our best to keep as much
	// garbage out of the system as possible.
	is := bytes.NewReader(cds.CodePackage)
	gr, err := gzip.NewReader(is)
	if err != nil {
		return fmt.Errorf("failure opening codepackage gzip stream: %s", err)
	}
	tr := tar.NewReader(gr)

	for {
		header, err := tr.Next()
		if err != nil {
			// We only get here if there are no more entries to scan
			break
		}

		// --------------------------------------------------------------------------------------
		// Check that file mode makes sense
		// --------------------------------------------------------------------------------------
		// Acceptable flags:
		//      ISREG      == 0100000
		//      rwxrwxrwx == 0777
		//
		// Anything else is suspect in this context and will be rejected
		// --------------------------------------------------------------------------------------
		if header.Mode&^0100777 != 0 {
			return fmt.Errorf("illegal file mode detected for file %s: %o", header.Name, header.Mode)
		}
	}

	return nil
}

// Generates a deployment payload for GOLANG as a series of src/$pkg entries in .tar.gz format
func (binaryPlatform *Platform) GetDeploymentPayload(spec *pb.ChaincodeSpec) ([]byte, error) {

	var err error

	// --------------------------------------------------------------------------------------
	// retrieve a CodeDescriptor from either HTTP or the filesystem
	// --------------------------------------------------------------------------------------
	code, err := getCode(spec)
	if err != nil {
		return nil, err
	}
	if code.Cleanup != nil {
		defer code.Cleanup()
	}

	// --------------------------------------------------------------------------------------
	// Find the source from our first-order code package ...
	// --------------------------------------------------------------------------------------
	fileMap, err := findSource(code.Gopath, code.Pkg)
	if err != nil {
		return nil, err
	}

	// --------------------------------------------------------------------------------------
	// Reprocess into a list for easier handling going forward
	// --------------------------------------------------------------------------------------
	files := make(Sources, 0)
	for _, file := range fileMap {
		files = append(files, file)
	}

	// --------------------------------------------------------------------------------------
	// Remap non-package dependencies to package/vendor
	// --------------------------------------------------------------------------------------
	//TODO: May need to be revisted when loading binaries with vendor dependencies
	//vendorDependencies(code.Pkg, files)

	// --------------------------------------------------------------------------------------
	// Sort on the filename so the tarball at least looks sane in terms of package grouping
	// --------------------------------------------------------------------------------------
	sort.Sort(files)

	// --------------------------------------------------------------------------------------
	// Write out our tar package
	// --------------------------------------------------------------------------------------
	payload := bytes.NewBuffer(nil)
	gw := gzip.NewWriter(payload)
	tw := tar.NewWriter(gw)

	for _, file := range files {

		fd, err := os.Open(file.Path)
		if err != nil {
			return nil, fmt.Errorf("Error writing tar : %s: %s", file.Path, err)
		}
		defer fd.Close()

		is := bufio.NewReader(fd)
		info, err := os.Stat(file.Path)
		if err != nil {
			return nil, fmt.Errorf("Error writing tar :%s: %s", file.Path, err)
		}
		header, err := tar.FileInfoHeader(info, file.Path)
		if err != nil {
			return nil, fmt.Errorf("Error writing tar :Error getting FileInfoHeader: %s", err)
		}

		oldname := header.Name
		var zeroTime time.Time
		header.AccessTime = zeroTime
		header.ModTime = zeroTime
		header.ChangeTime = zeroTime
		//TODO: Hardcoded name to chaincode...
		header.Name = "chaincode"
		header.Mode = 0100777

		if err = tw.WriteHeader(header); err != nil {
			return nil, fmt.Errorf("Error writing tar :Error write header for (path: %s, oldname:%s,newname:%s,sz:%d) : %s", file.Path, oldname, header.Name, header.Size, err)
		}
		if _, err := io.Copy(tw, is); err != nil {
			return nil, fmt.Errorf("Error writing tar :Error copy (path: %s, oldname:%s,newname:%s,sz:%d) : %s", file.Path, oldname, header.Name, header.Size, err)
		}

	}

	tw.Close()
	gw.Close()

	return payload.Bytes(), nil
}

func (binaryPlatform *Platform) GenerateDockerfile(cds *pb.ChaincodeDeploymentSpec) (string, error) {

	var buf []string

	buf = append(buf, "FROM "+cutil.GetDockerfileFromConfig("chaincode.binary.runtime"))
	buf = append(buf, "ADD binpackage.tar /usr/local/bin")

	dockerFileContents := strings.Join(buf, "\n")

	return dockerFileContents, nil
}

func (binaryPlatform *Platform) GenerateDockerBuild(cds *pb.ChaincodeDeploymentSpec, tw *tar.Writer) error {

	return cutil.WriteBytesToPackage("binpackage.tar", cds.CodePackage, tw)
}
