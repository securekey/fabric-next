/*
Copyright IBM Corp. 2017 All Rights Reserved.

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

package scc

import (
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/hyperledger/fabric/core/common/ccprovider"
	"github.com/hyperledger/fabric/core/ledger/ledgermgmt"
	ccprovider2 "github.com/hyperledger/fabric/core/mocks/ccprovider"
	"github.com/hyperledger/fabric/core/peer"
	pb "github.com/hyperledger/fabric/protos/peer"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func init() {
	viper.Set("chaincode.system", map[string]string{"lscc": "enable", "a": "enable", "extscc1": "enable"})

	MockExternalSysCCs(false)

	ccprovider.RegisterChaincodeProviderFactory(&ccprovider2.MockCcProviderFactory{})
	RegisterSysCCs()
}

func MockExternalSysCCs(enabled bool) {

	//mock External SysCCs
	sysExtCCs := SystemChaincode{}

	viper.Set("chaincode.systemext", map[string]interface{}{"extscc": sysExtCCs})
	sysccLogger.Errorf("Setting %s for chaincode.systemext.enabled", enabled)
	viper.Set("chaincode.systemext.enabled", enabled)
	// make sure this path is available and has a valid CDS file (extscc1.golang):
	// github.com/hyperledger/fabric/test/extscc/fixtures/config/extsysccs/extscc1.golang
	// todo need to investigate why unit tests can't ready from a relative path
	viper.Set("chaincode.systemext.cds.path", "../../test/extscc/fixtures/config/extsysccs")

	// below entries are needed as the above line doesn't simulate exact real yaml entries
	viper.Set("chaincode.systemext.extscc1.configPath", "github.com/hyperledger/fabric/core/chaincode/extscc1/config")
	viper.Set("chaincode.systemext.extscc1.invokableexternal", true)
	viper.Set("chaincode.systemext.extscc1.invokablecc2cc", true)
	viper.Set("chaincode.systemext.extscc1.enabled", enabled)

	viper.Set("chaincode.systemext.extscc1.initArgs", map[string]string{
		"arg1key": "arg1value",
		"arg2key": "arg2value",
		"arg3key": "arg3value",
	})
}

func TestDeploy(t *testing.T) {
	DeploySysCCs("")
	f := func() {
		DeploySysCCs("a")
	}
	assert.Panics(t, f)
	ledgermgmt.InitializeTestEnv()
	defer ledgermgmt.CleanupTestEnv()
	err := peer.MockCreateChain("a")
	fmt.Println(err)
	deploySysCC("a", &SystemChaincode{
		Enabled: true,
		Name:    "lscc",
		CDS: &pb.ChaincodeDeploymentSpec{
			ExecEnv: pb.ChaincodeDeploymentSpec_SYSTEM,
			ChaincodeSpec: &pb.ChaincodeSpec{
				Type: pb.ChaincodeSpec_GOLANG,
				ChaincodeId: &pb.ChaincodeID{
					Name: "lscc",
					Path: "",
				},
				Input: &pb.ChaincodeInput{
					Args: [][]byte{[]byte("")},
				},
			},
		},
	})
}

func TestDeDeploySysCC(t *testing.T) {
	DeDeploySysCCs("")
	f := func() {
		DeDeploySysCCs("a")
	}
	assert.NotPanics(t, f)
}

func TestSCCProvider(t *testing.T) {
	assert.NotNil(t, (&sccProviderFactory{}).NewSystemChaincodeProvider())
}

func TestIsSysCC(t *testing.T) {
	assert.True(t, IsSysCC("lscc"))
	assert.False(t, IsSysCC("noSCC"))
	assert.True(t, (&sccProviderImpl{}).IsSysCC("lscc"))
	assert.False(t, (&sccProviderImpl{}).IsSysCC("noSCC"))
}

func TestIsSysCCAndNotInvokableCC2CC(t *testing.T) {
	assert.False(t, IsSysCCAndNotInvokableCC2CC("lscc"))
	assert.True(t, IsSysCC("cscc"))
	assert.True(t, IsSysCCAndNotInvokableCC2CC("cscc"))
	assert.True(t, (&sccProviderImpl{}).IsSysCC("cscc"))
	assert.False(t, (&sccProviderImpl{}).IsSysCCAndNotInvokableCC2CC("lscc"))
	assert.True(t, (&sccProviderImpl{}).IsSysCCAndNotInvokableCC2CC("cscc"))
}

func TestIsSysCCAndNotInvokableExternal(t *testing.T) {
	assert.False(t, IsSysCCAndNotInvokableExternal("cscc"))
	assert.True(t, IsSysCC("cscc"))
	assert.True(t, IsSysCCAndNotInvokableExternal("vscc"))
	assert.False(t, (&sccProviderImpl{}).IsSysCCAndNotInvokableExternal("cscc"))
	assert.True(t, (&sccProviderImpl{}).IsSysCC("cscc"))
	assert.True(t, (&sccProviderImpl{}).IsSysCCAndNotInvokableExternal("vscc"))
}

func TestSccProviderImpl_GetQueryExecutorForLedger(t *testing.T) {
	qe, err := (&sccProviderImpl{}).GetQueryExecutorForLedger("")
	assert.Nil(t, qe)
	assert.Error(t, err)
}

func TestMockRegisterAndResetSysCCs(t *testing.T) {
	orig := MockRegisterSysCCs([]*SystemChaincode{})
	assert.NotEmpty(t, orig)
	MockResetSysCCs(orig)
	assert.Equal(t, len(orig), len(systemChaincodes))
}

func TestRegisterSysCC(t *testing.T) {
	cds := &pb.ChaincodeDeploymentSpec{
		ExecEnv: pb.ChaincodeDeploymentSpec_SYSTEM,
		ChaincodeSpec: &pb.ChaincodeSpec{
			Type: pb.ChaincodeSpec_GOLANG,
			ChaincodeId: &pb.ChaincodeID{
				Name: "lscc",
				Path: "",
			},
			Input: &pb.ChaincodeInput{
				Args: [][]byte{[]byte("")},
			},
		},
	}

	err := RegisterSysCC(&SystemChaincode{
		Name:    "lscc",
		Path:    "path",
		Enabled: true,
		CDS:     cds,
	})
	assert.NoError(t, err)
	err = RegisterSysCC(&SystemChaincode{
		Name:    "lscc",
		Path:    "path",
		Enabled: true,
		CDS:     cds,
	})
	assert.Error(t, err)
	assert.Contains(t, "path already registered", err)
}

func TestRegisterDuplicateExtSysCC(t *testing.T) {
	MockExternalSysCCs(true)
	RegisterSysCCs()
	cds, err := mockextscc1CDS()

	assert.Nil(t, err, "mocking extscc1 CDS should not return an error")

	ex := &SystemChaincode{
		Name:              "extscc1", // <-- name should trigger duplicate error
		Path:              "github.com/hyperledger/fabric/core/chaincode/extscc1",
		Enabled:           true,
		InvokableExternal: true,
		InvokableCC2CC:    false,
		ConfigPath:        "github.com/hyperledger/fabric/core/chaincode/extscc1/config",
		CDS:               cds,
	}
	err = RegisterSysCC(ex)

	assert.Error(t, err)
}

func mockextscc1CDS() (*pb.ChaincodeDeploymentSpec, error) {
	// make sure this path is available and has a valid CDS file (extscc1.golang):
	// github.com/hyperledger/fabric/test/extscc/fixtures/config/extsysccs/extscc1.golang
	// todo need to investigate why unit tests can't read from a relative path
	path := "../../test/extscc/fixtures/config/extsysccs/extscc1.golang"

	ccbytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("Error reading from file %s: %v", path, err)
	}

	ccpack := ccprovider.CDSPackage{}

	_, err = ccpack.InitFromBuffer(ccbytes)
	if err != nil {
		return nil, err
	}
	cds := ccpack.GetDepSpec()

	return cds, nil
}
