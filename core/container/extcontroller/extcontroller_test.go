/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package extcontroller

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/hyperledger/fabric/core/chaincode/shim"
	//"github.com/hyperledger/fabric/core/container/api"
	"github.com/hyperledger/fabric/core/container/ccintf"
	pb "github.com/hyperledger/fabric/protos/peer"
	"github.com/stretchr/testify/assert"
)

type testCC struct {
}

func (t testCC) Init(stub shim.ChaincodeStubInterface) pb.Response {
	return shim.Success(nil)
}

func (t testCC) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	return shim.Success(nil)
}

func TestRegister(t *testing.T) {
	err := Register("somePath", nil, pb.ChaincodeSpec_UNDEFINED, "")
	assert.Nil(t, err)

	// already registered so should be err
	err = Register("somePath", nil, pb.ChaincodeSpec_UNDEFINED, "")
	assert.EqualError(t, err, "somePath already registered")
}

func TestGetInstance(t *testing.T) {
	extvm := ExtVM{id: "1"}
	ipcTemplate := &extContainer{}

	// instance gets created first time
	_, err := extvm.getInstance(nil, ipcTemplate, "testGetInstance", nil, nil)
	assert.Nil(t, err)

	// should be no error when getting the same instance again
	_, err = extvm.getInstance(nil, ipcTemplate, "testGetInstance", nil, nil)
	assert.Nil(t, err)
}

func TestDeploy(t *testing.T) {
	ccid := ccintf.CCID{
		ChaincodeSpec: &pb.ChaincodeSpec{ChaincodeId: &pb.ChaincodeID{Version: "1", Path: "testDeploy"}},
	}

	extvm := ExtVM{id: "1"}

	// not registered
	err := extvm.Deploy(nil, ccid, nil, nil, nil)
	assert.EqualError(t, err, "testDeploy not registered")

	// no chaincode instance
	err = Register("testDeploy", nil, pb.ChaincodeSpec_UNDEFINED, "")
	assert.Nil(t, err)
	err = extvm.Deploy(nil, ccid, nil, nil, nil)
	assert.EqualError(t, err, "testDeploy system chaincode does not contain chaincode instance")

	// happy path
	ccid2 := ccintf.CCID{
		ChaincodeSpec: &pb.ChaincodeSpec{ChaincodeId: &pb.ChaincodeID{Version: "1", Path: "testDeployHP"}},
	}

	err = Register("testDeployHP", testCC{}, pb.ChaincodeSpec_UNDEFINED, "")
	assert.Nil(t, err)
	err = extvm.Deploy(nil, ccid2, nil, nil, nil)
	assert.Nil(t, err)
}

func TestStart(t *testing.T) {
	ccid := ccintf.CCID{
		ChaincodeSpec: &pb.ChaincodeSpec{ChaincodeId: &pb.ChaincodeID{Version: "1", Path: "testStart"}},
	}

	extvm := ExtVM{id: "1"}

	// not registered
	err := extvm.Start(nil, ccid, nil, nil, nil, nil)
	assert.EqualError(t, err, "testStart not registered")

	// not Binary type
	err = Register("testStart", nil, pb.ChaincodeSpec_UNDEFINED, "")
	assert.Nil(t, err)
	err = extvm.Start(nil, ccid, nil, nil, nil, nil)
	assert.EqualError(t, err, "Error starting chaincode : Only BINARY chaincodes are supported")

	//
	ccidBin := ccintf.CCID{
		ChaincodeSpec: &pb.ChaincodeSpec{ChaincodeId: &pb.ChaincodeID{Version: "1", Path: "testStartBin", Name: "testStartBin"}, Type: pb.ChaincodeSpec_BINARY},
	}
	err = Register("testStartBin", testCC{}, pb.ChaincodeSpec_BINARY, "")
	assert.Nil(t, err)
	err = extvm.Deploy(nil, ccidBin, nil, nil, nil)
	assert.Nil(t, err)

	builder := func() (io.Reader, error) { return strings.NewReader("testing"), nil }

	// TODO: make extcontroller panic when bin path is not found
	err = extvm.Start(nil, ccidBin, nil, nil, builder, nil)
	assert.Nil(t, err)
	//assert.Panics(t, func() { extvm.Start(nil, ccidBin, nil, nil, builder, nil) })
}

func TestExecCmd(t *testing.T) {
	ec := extContainer{}
	err := ec.execCmd("notExist", []string{"testEnv"})
	assert.EqualError(t, err, "Error performing exec command on ext scc binary exec: \"notExist\": executable file not found in $PATH")
}

func TestStop(t *testing.T) {
	ccid := ccintf.CCID{
		ChaincodeSpec: &pb.ChaincodeSpec{ChaincodeId: &pb.ChaincodeID{Version: "1", Path: "testStop"}},
	}

	extvm := ExtVM{id: "1"}

	// not registered
	err := extvm.Stop(nil, ccid, 0, false, false)
	assert.EqualError(t, err, "testStop not registered")

	err = Register("testStop", nil, pb.ChaincodeSpec_UNDEFINED, "")
	assert.Nil(t, err)
	err = extvm.Stop(nil, ccid, 0, false, false)
	assert.EqualError(t, err, " not running")
}

func TestGetVMName(t *testing.T) {
	ccid := ccintf.CCID{
		ChaincodeSpec: &pb.ChaincodeSpec{ChaincodeId: &pb.ChaincodeID{Version: "1", Path: "testPath"}},
	}
	extvm := ExtVM{id: "1"}
	name, err := extvm.GetVMName(ccid, testFormatFunc)
	assert.Nil(t, err)
	assert.Equal(t, "TESTformat", name)
}

func testFormatFunc(name string) (string, error) {
	return "TESTformat", nil
}

func TestDestroy(t *testing.T) {
	extvm := ExtVM{id: "1"}
	assert.Nil(t, extvm.Destroy(nil, ccintf.CCID{}, false, false))
}

func TestGetPeerEnv(t *testing.T) {
	os.Setenv("CORE_TEST", "v1")
	os.Setenv("TEST", "v2")

	envs := getPeerEnv()

	if len(envs) < 1 {
		extLogger.Info(envs)
		t.Fatal("Expected at least one variable")
	}

	found := false

	for _, v := range envs {
		if !strings.HasPrefix(v, "CORE_") {
			t.Fatalf("Found an env var without CORE_ prefix: %s", v)
		}

		if v == "CORE_TEST=v1" {
			found = true
		}
	}

	if !found {
		t.Fatalf("Did not find \"CORE_TEST=v1\" in env vars")
	}
}
