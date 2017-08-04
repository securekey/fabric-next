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

	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/core/chaincode/platforms"
	pb "github.com/hyperledger/fabric/protos/peer"
)

func TestGenerateBinHashAndCheckIsBinExtCCBuilt(t *testing.T) {
	testCCName := "extscc_cc"
	pathPrefix := "./testbin/"
	ccBinOutPath := pathPrefix + testCCName
	generateExtCCHashFile(pathPrefix, testCCName, ccBinOutPath)
	d, err := ioutil.ReadFile(ccBinOutPath)
	testerr(t, err, true)

	testCCPath := pathPrefix + computeHashFilenameForBin(testCCName, d)
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

	deleteAllHashFiles(pathPrefix, testCCName)

}

func TestComputeHashCodes(t *testing.T) {
	testCCName := "extscc_cc"
	pathPrefix := "./testbin/"
	ccBinOutPath := pathPrefix + testCCName
	generateExtCCHashFile(pathPrefix, testCCName, ccBinOutPath)
	d, err := ioutil.ReadFile(ccBinOutPath)
	testerr(t, err, true)

	start := time.Now()
	computeHashFilenameForBin(testCCName, d)
	elapsed := time.Since(start)
	t.Logf("Time elapsed for regular hash: %s", elapsed)
}

func TestGenerateExtBuild(t *testing.T) {
	//todo add bin tar file in the same folder as this file for this test to work..
	chaincodePath := "github.com/hyperledger/fabric/examples/chaincode/go/chaincode_example01"
	spec := &pb.ChaincodeSpec{Type: pb.ChaincodeSpec_GOLANG,
		ChaincodeId: &pb.ChaincodeID{Name: "testbin/extscc_cc", Path: chaincodePath},
		Input:       &pb.ChaincodeInput{Args: util.ToChaincodeArgs("f")}}
	codePackage, err := platforms.GetDeploymentPayload(spec)

	if err != nil {
		t.Fatal()
	}
	cds := &pb.ChaincodeDeploymentSpec{ChaincodeSpec: spec, CodePackage: codePackage}
	_, err = GenerateExtBuild(cds, nil)
	testerr(t, err, true)
}

func testerr(t *testing.T, err error, succ bool) {
	if succ {
		assert.NoError(t, err, "Expected success but got error")
	} else {
		assert.Error(t, err, "Expected failure but succeeded")
	}
}
