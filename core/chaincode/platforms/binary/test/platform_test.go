/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package test

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hyperledger/fabric/core/chaincode/platforms/binary"
	"github.com/hyperledger/fabric/core/config"
	pb "github.com/hyperledger/fabric/protos/peer"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func testerr(err error, succ bool) error {
	if succ && err != nil {
		return fmt.Errorf("Expected success but got error %s", err)
	} else if !succ && err == nil {
		return fmt.Errorf("Expected failure but succeeded")
	}
	return nil
}

func writeBytesToPackage(name string, payload []byte, mode int64, tw *tar.Writer) error {
	//Make headers identical by using zero time
	var zeroTime time.Time
	tw.WriteHeader(&tar.Header{Name: name, Size: int64(len(payload)), ModTime: zeroTime, AccessTime: zeroTime, ChangeTime: zeroTime, Mode: mode})
	tw.Write(payload)

	return nil
}

func generateFakeCDS(ccname, path, file string, mode int64) (*pb.ChaincodeDeploymentSpec, error) {
	codePackage := bytes.NewBuffer(nil)
	gw := gzip.NewWriter(codePackage)
	tw := tar.NewWriter(gw)

	payload := make([]byte, 25, 25)
	err := writeBytesToPackage(file, payload, mode, tw)
	if err != nil {
		return nil, err
	}

	tw.Close()
	gw.Close()

	cds := &pb.ChaincodeDeploymentSpec{
		ChaincodeSpec: &pb.ChaincodeSpec{
			Type: pb.ChaincodeSpec_BINARY,
			ChaincodeId: &pb.ChaincodeID{
				Name: ccname,
				Path: path,
			},
		},
		CodePackage: codePackage.Bytes(),
	}

	return cds, nil
}

type spec struct {
	CCName          string
	Path, File      string
	Mode            int64
	SuccessExpected bool
	RealGen         bool
}

func TestValidateCDS(t *testing.T) {
	platform := &binary.Platform{}

	specs := make([]spec, 0)
	//Should fail with file name validation error
	specs = append(specs, spec{CCName: "NoCode", Path: "path/to/nowhere", File: "/some/path", Mode: 0100400, SuccessExpected: false})
	//Should fail with  header mode validation error
	specs = append(specs, spec{CCName: "NoCode", Path: "path/to/somewhere", File: "chaincode", Mode: int64(100400), SuccessExpected: false})
	//Should be successful
	specs = append(specs, spec{CCName: "NoCode", Path: "path/to/somewhere", File: "chaincode", Mode: 0100555, SuccessExpected: true})

	for _, s := range specs {
		cds, err := generateFakeCDS(s.CCName, s.Path, s.File, s.Mode)
		err = platform.ValidateDeploymentSpec(cds)

		if s.SuccessExpected == true && err != nil {
			t.Errorf("Unexpected failure: %s", err)
		}
		if s.SuccessExpected == false && err == nil {
			t.Log("Expected validation failure")
			t.Fail()
		}
	}
}

func Test_DeploymentPayload(t *testing.T) {
	platform := &binary.Platform{}
	spec := &pb.ChaincodeSpec{
		ChaincodeId: &pb.ChaincodeID{
			Path: "example_cc",
		},
	}

	payload, err := platform.GetDeploymentPayload(spec)
	assert.NoError(t, err)

	t.Logf("payload size: %d", len(payload))

	is := bytes.NewReader(payload)
	gr, err := gzip.NewReader(is)
	if err == nil {
		tr := tar.NewReader(gr)

		for {
			header, err := tr.Next()
			if err != nil {
				// We only get here if there are no more entries to scan
				break
			}

			t.Logf("%s (%d)", header.Name, header.Size)
		}
	}
}

func TestGetDeploymentPayload(t *testing.T) {
	platform := &binary.Platform{}

	var tests = []struct {
		spec *pb.ChaincodeSpec
		succ bool
	}{
		{spec: &pb.ChaincodeSpec{ChaincodeId: &pb.ChaincodeID{Name: "Test Chaincode", Path: "example_cc"}}, succ: true},
		{spec: &pb.ChaincodeSpec{ChaincodeId: &pb.ChaincodeID{Name: "Test Chaincode", Path: "github.com/hyperledger/fabric/test/chaincodes/BadImport"}}, succ: false},
	}

	for _, tst := range tests {
		_, err := platform.GetDeploymentPayload(tst.spec)
		if err = testerr(err, tst.succ); err != nil {
			t.Errorf("Error validating chaincode spec: %s, %s", tst.spec.ChaincodeId.Path, err)
		}
	}
}

//TestGenerateDockerBuild goes through the functions needed to do docker build
func TestGenerateDockerBuild(t *testing.T) {
	platform := &binary.Platform{}

	specs := make([]spec, 0)
	specs = append(specs, spec{CCName: "NoCode", Path: "path/to/nowhere", File: "chaincode", Mode: 0100400, SuccessExpected: true})

	var err error
	for _, tst := range specs {
		inputbuf := bytes.NewBuffer(nil)
		tw := tar.NewWriter(inputbuf)

		var cds *pb.ChaincodeDeploymentSpec
		if tst.RealGen {
			cds = &pb.ChaincodeDeploymentSpec{
				ChaincodeSpec: &pb.ChaincodeSpec{
					ChaincodeId: &pb.ChaincodeID{
						Name:    tst.CCName,
						Path:    tst.Path,
						Version: "0",
					},
				},
			}
			cds.CodePackage, err = platform.GetDeploymentPayload(cds.ChaincodeSpec)
			if err = testerr(err, tst.SuccessExpected); err != nil {
				t.Errorf("test failed in GetDeploymentPayload: %s, %s", cds.ChaincodeSpec.ChaincodeId.Path, err)
			}
		} else {
			cds, err = generateFakeCDS(tst.CCName, tst.Path, tst.File, tst.Mode)
		}

		if _, err = platform.GenerateDockerfile(cds); err != nil {
			t.Errorf("could not generate docker file for a valid spec: %s, %s", cds.ChaincodeSpec.ChaincodeId.Path, err)
		}
		err = platform.GenerateDockerBuild(cds, tw)

		if err = testerr(err, tst.SuccessExpected); err != nil {
			t.Errorf("Error validating chaincode spec: %s, %s", cds.ChaincodeSpec.ChaincodeId.Path, err)
		}
	}
}

func TestMain(m *testing.M) {
	viper.SetConfigName("core")
	viper.SetEnvPrefix("CORE")
	config.AddDevConfigPath(nil)
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err != nil {
		fmt.Printf("could not read config %s\n", err)
		os.Exit(-1)
	}
	os.Exit(m.Run())
}
