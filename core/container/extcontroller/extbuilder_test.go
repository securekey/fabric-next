/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package extcontroller

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

func testerr(t *testing.T, err error, succ bool) {
	if succ {
		assert.NoError(t, err, "Expected success but got error")
	} else {
		assert.Error(t, err, "Expected failure but succeeded")
	}
}
