/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
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

	"encoding/hex"

	"io/ioutil"

	"crypto/md5"

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

	if isBinExtCCbuilt(curDir, cds.ChaincodeSpec.ChaincodeId.Name) {
		ccBinOutPath := filepath.Join(curDir, string(filepath.Separator), cds.ChaincodeSpec.ChaincodeId.Name)
		f, err := os.Open(ccBinOutPath)

		if err != nil {
			return nil, err
		}
		f.Close()

		return strings.NewReader(ccBinOutPath), nil
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

	binPkgOutputPath := filepath.Join(curDir, string(filepath.Separator)+"binpackage.tar")

	err = util.ExtractFileFromTar(gr, "binpackage.tar", binPkgOutputPath)
	gr.Close()
	if err != nil {
		return nil, fmt.Errorf("Error extracting binpackage.tar from code package: %v", err)
	}

	binPkgTarFile, err := os.Open(binPkgOutputPath)
	if err != nil {
		return nil, fmt.Errorf("Error opening binpackage.tar: %v", err)
	}

	ccBinOutPath := filepath.Join(curDir, string(filepath.Separator), cds.ChaincodeSpec.ChaincodeId.Name)

	err = util.ExtractFileFromTar(binPkgTarFile, "."+string(filepath.Separator)+"chaincode", ccBinOutPath)
	binPkgTarFile.Close()
	if err != nil {
		return nil, fmt.Errorf("Error extracting chaincode binary from binpackage.tar: %v", err)
	}

	generateExtCCHashFile(curDir, cds.ChaincodeSpec.ChaincodeId.Name, ccBinOutPath)

	return strings.NewReader(ccBinOutPath), nil
}
func generateExtCCHashFile(curDir string, ccName string, ccBinOutPath string) {
	d, err := ioutil.ReadFile(ccBinOutPath)
	if err != nil {
		panic("Bin file does not exist. Cannot generate hash file.")
	}

	name := computeHashFilenameForBin(ccName, d)
	outputHashFilePath := filepath.Join(curDir, string(filepath.Separator), name)

	//remove any existing hash prior to creating a new hash for this ext scc
	deleteAllHashFiles(curDir, ccName)

	if err != nil {
		panic("Failed to clean " + ccName + " hash files for binary build")
	}

	f, err := os.Create(outputHashFilePath)
	if err != nil {
		panic("Failed to generate hash file for binary build")
	}
	f.Close()
}
func isBinExtCCbuilt(curDir string, ccName string) bool {
	ccBinOutPath := filepath.Join(curDir, string(filepath.Separator), ccName)
	// if bin file doesn't exist, then don't bother computing the hash, return false
	if _, err := os.Stat(ccBinOutPath); os.IsNotExist(err) {
		return false
	}

	d, err := ioutil.ReadFile(ccBinOutPath)
	if err != nil {
		panic("Can't read Bin hash file. Either delete hash to force a recompile or verify the hash is a valid file. Bin file: " + ccBinOutPath)
	}
	name := computeHashFilenameForBin(ccName, d)
	hashFound := false
	if name != "" && len(name) > 0 {

		if _, err := os.Stat(curDir + string(filepath.Separator) + name); !os.IsNotExist(err) {
			hashFound = true
		}

	}
	return hashFound
}
func computeHashFilenameForBin(ccName string, data []byte) string {
	h := md5.New()
	h.Write(data)
	name := ccName + "-hash-" + hex.EncodeToString(h.Sum(nil))
	return name
}
func deleteAllHashFiles(curDir string, ccName string) {
	dirname := curDir + string(filepath.Separator)
	d, err := os.Open(dirname)
	if err != nil {
		panic("Unable to open directory: " + dirname)
	}
	defer d.Close()

	files, err := d.Readdir(-1)
	if err != nil {
		panic("Unable to read directory: " + dirname)
	}

	for _, file := range files {
		if file.Mode().IsRegular() {
			if strings.HasPrefix(file.Name(), ccName+"-hash-") {
				err = os.Remove(dirname + file.Name())
				if err != nil {
					panic("Unable to delete hash file: " + file.Name() + ". Error: " + fmt.Sprintf("%s", err))
				}
			}
		}
	}
}
