/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package extcontroller

import (
	"fmt"
	"path/filepath"

	"os"
	"strings"

	"encoding/hex"

	"io/ioutil"

	"crypto/md5"
)

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
