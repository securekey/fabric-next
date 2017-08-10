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
	"github.com/hyperledger/fabric/core/config"
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
	buildDir := filepath.Join(config.GetPath("peer.fileSystemPath"), "chaincodes")

	if isBinExtCCbuilt(buildDir, cds.ChaincodeSpec.ChaincodeId.Name) {
		ccBinOutPath := filepath.Join(buildDir, cds.ChaincodeSpec.ChaincodeId.Name)
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

	binPkgOutputPath := filepath.Join(buildDir, "binpackage.tar")

	err = util.ExtractFileFromTar(gr, "binpackage.tar", binPkgOutputPath)
	gr.Close()
	if err != nil {
		return nil, fmt.Errorf("Error extracting binpackage.tar from code package: %v", err)
	}

	binPkgTarFile, err := os.Open(binPkgOutputPath)
	if err != nil {
		return nil, fmt.Errorf("Error opening binpackage.tar: %v", err)
	}

	ccBinOutPath := filepath.Join(buildDir, cds.ChaincodeSpec.ChaincodeId.Name)

	err = util.ExtractFileFromTar(binPkgTarFile, "./chaincode", ccBinOutPath)
	binPkgTarFile.Close()
	if err != nil {
		return nil, fmt.Errorf("Error extracting chaincode binary from binpackage.tar: %v", err)
	}

	err = generateExtCCHashFile(buildDir, cds.ChaincodeSpec.ChaincodeId.Name, ccBinOutPath)
	if err != nil {
		return nil, fmt.Errorf("Error generating chaincode binary hash: %v", err)
	}

	return strings.NewReader(ccBinOutPath), nil
}
func generateExtCCHashFile(buildDir string, ccName string, ccBinOutPath string) error {
	d, err := ioutil.ReadFile(ccBinOutPath)
	if err != nil {
		extLogger.Warning("Bin file does not exist. Cannot generate hash file. Err: %s", err)
		return err
	}

	name := computeHashFilenameForBin(ccName, d)
	outputHashFilePath := filepath.Join(buildDir, "hash", name)

	//remove any existing hash prior to creating a new hash for this ext scc
	err = deleteAllHashFiles(buildDir, ccName)
	if err != nil {
		extLogger.Warningf("Failed to clean "+ccName+" hash files for binary build. Err: %s", err)
		return err
	}

	var f *os.File = nil
	defer f.Close()
	f, err = os.Create(outputHashFilePath)
	if err != nil {
		extLogger.Warningf("Failed to generate hash file for binary build. Err: %s", err)
		return err
	}

	return nil
}
func isBinExtCCbuilt(buildDir string, ccName string) bool {
	ccBinOutPath := filepath.Join(buildDir, ccName)
	// if bin file doesn't exist, then don't bother computing the hash, return false
	if _, err := os.Stat(ccBinOutPath); os.IsNotExist(err) {
		return false
	}

	hashFound := false
	d, err := ioutil.ReadFile(ccBinOutPath)
	if err != nil {
		extLogger.Warningf("Can't read Bin hash file. Either delete hash from to force a recompile or verify the hash is a valid file. Bin file: %s", ccBinOutPath)
	}
	name := computeHashFilenameForBin(ccName, d)
	if name != "" && len(name) > 0 {

		if _, err := os.Stat(filepath.Join(buildDir, "hash", name)); !os.IsNotExist(err) {
			hashFound = true
		}

	}
	return hashFound
}
func computeHashFilenameForBin(ccName string, data []byte) string {
	h := md5.New()
	h.Write(data)
	name := ccName + hex.EncodeToString(h.Sum(nil))
	return name
}
func deleteAllHashFiles(buildDir string, ccName string) error {
	dirname := filepath.Join(buildDir, "hash")
	d, err := os.Open(dirname)
	if err != nil {
		// try to create the hash directory if it doesn't exist
		err = os.MkdirAll(dirname, os.ModePerm)
		return err
	}
	defer d.Close()

	files, err := d.Readdir(-1)
	if err != nil {
		return err
	}

	for _, file := range files {
		if file.Mode().IsRegular() {
			if strings.HasPrefix(file.Name(), ccName) {
				err = os.Remove(filepath.Join(dirname, file.Name()))
				if err != nil {
					return fmt.Errorf("Unable to delete hash file: %s. Error: %s", file.Name(), err)
				}
			}
		}
	}
	return nil
}
