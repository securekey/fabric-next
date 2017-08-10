/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package extcontroller

import (
	"testing"

	"io/ioutil"
	"os"
	"time"

	"github.com/stretchr/testify/assert"

	"path/filepath"

	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/core/chaincode/platforms"
	pb "github.com/hyperledger/fabric/protos/peer"
	"github.com/spf13/viper"
)

func TestGenerateBinHashAndCheckIsBinExtCCBuilt(t *testing.T) {
	testCCName := "extscc_cc"
	pathPrefix := filepath.Join("testbin", "chaincodes", string(filepath.Separator))
	ccBinOutPath := filepath.Join(pathPrefix, testCCName)
	err := generateExtCCHashFile(pathPrefix, testCCName, ccBinOutPath)
	testerr(t, err, true)

	d, err := ioutil.ReadFile(ccBinOutPath)
	testerr(t, err, true)

	testCCPath := filepath.Join(pathPrefix, "hash", computeHashFilenameForBin(testCCName, d))
	t.Logf("About to open %s", testCCPath)
	f, err := os.Open(testCCPath)
	testerr(t, err, true)

	if !isBinExtCCbuilt(pathPrefix, testCCName) {
		t.Fatalf("Test extscc_cc binary file missing)")
	}
	if f != nil {
		t.Logf("New File Hash created successfuly. %#v", f)
		//os.Remove(testCCPath)
	} else {
		t.Fatalf("New CC Hash failed to be created.  %#v", f)
	}

	err = deleteAllHashFiles(pathPrefix, testCCName)
	testerr(t, err, true)
}

func TestComputeHashCodes(t *testing.T) {
	testCCName := "extscc_cc"
	pathPrefix := filepath.Join("testbin", "chaincodes", string(filepath.Separator))
	ccBinOutPath := filepath.Join(pathPrefix, testCCName)
	err := generateExtCCHashFile(pathPrefix, testCCName, ccBinOutPath)
	testerr(t, err, true)
	d, err := ioutil.ReadFile(ccBinOutPath)
	testerr(t, err, true)

	start := time.Now()
	n := computeHashFilenameForBin(testCCName, d)
	elapsed := time.Since(start)
	t.Logf("Time elapsed for hashing name %s: %s", n, elapsed)
}

func TestGenerateExtBuild(t *testing.T) {
	viper.Set("peer.fileSystemPath", "testbin/")
	chaincodePath := "github.com/hyperledger/fabric/examples/chaincode/go/chaincode_example01"
	spec := &pb.ChaincodeSpec{Type: pb.ChaincodeSpec_GOLANG,
		ChaincodeId: &pb.ChaincodeID{Name: "extscc_cc", Path: chaincodePath},
		Input:       &pb.ChaincodeInput{Args: util.ToChaincodeArgs("f")}}
	codePackage, err := platforms.GetDeploymentPayload(spec)

	if err != nil {
		t.Fatal()
	}
	cds := &pb.ChaincodeDeploymentSpec{ChaincodeSpec: spec, CodePackage: codePackage}

	_, err = GenerateExtBuild(cds, nil)
	testerr(t, err, true)

	// to see the generated hash for the scc binary, comment out the below 2 lines
	// hash is found under testbin/chaincodes/hash
	err = deleteAllHashFiles(filepath.Join(viper.GetString("peer.fileSystemPath"), "chaincodes"), "extscc_cc")
	testerr(t, err, true)
}

func testerr(t *testing.T, err error, succ bool) {
	if succ {
		assert.NoError(t, err, "Expected success but got error")
	} else {
		assert.Error(t, err, "Expected failure but succeeded")
	}
}
