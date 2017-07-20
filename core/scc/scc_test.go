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
	"testing"

	"github.com/hyperledger/fabric/core/common/ccprovider"
	"github.com/hyperledger/fabric/core/ledger/ledgermgmt"
	ccprovider2 "github.com/hyperledger/fabric/core/mocks/ccprovider"
	"github.com/hyperledger/fabric/core/peer"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func init() {
	viper.Set("chaincode.system", map[string]string{"lscc": "enable", "a": "enable", "extscc": "enable"})

	MockExternalSysCCs(false)

	ccprovider.RegisterChaincodeProviderFactory(&ccprovider2.MockCcProviderFactory{})
	RegisterSysCCs()
}

func MockExternalSysCCs(enabled bool) {

	//mock External SysCCs
	sysExtCCs := SystemChaincode{}

	viper.Set("chaincode.systemext", map[string]interface{}{"extscc": sysExtCCs})

	// below entries are needed as the above line doesn't simulate exact real yaml entries
	viper.Set("chaincode.systemext.extscc.path", "github.com/hyperledger/fabric/core/chaincode/extscc")
	viper.Set("chaincode.systemext.extscc.configPath", "github.com/hyperledger/fabric/core/chaincode/extscc/config")
	viper.Set("chaincode.systemext.extscc.chaincodeType", "GOLANG")
	viper.Set("chaincode.systemext.extscc.executionEnvironment", "SYSTEM_EXT")
	viper.Set("chaincode.systemext.extscc.enabled", true)
	viper.Set("chaincode.systemext.extscc.invokableExternal", true)
	viper.Set("chaincode.systemext.extscc.invokableCC2CC", true)

	viper.Set("chaincode.systemext.extscc.initArgs", map[string]string{
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

func TestSysCCInitArgs(t *testing.T) {
	initArgs := SysCCInitArgs("extscc")
	assert.Equal(t, 3, len(initArgs))
	assert.Equal(t, "arg1key=arg1value", string(initArgs[0]))
	assert.Equal(t, "arg2key=arg2value", string(initArgs[1]))
	assert.Equal(t, "arg3key=arg3value", string(initArgs[2]))
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
	err := RegisterSysCC(&SystemChaincode{
		Name:    "lscc",
		Path:    "path",
		Enabled: true,
	})
	assert.NoError(t, err)
	err = RegisterSysCC(&SystemChaincode{
		Name:    "lscc",
		Path:    "path",
		Enabled: true,
	})
	assert.Error(t, err)
	assert.Contains(t, "path already registered", err)
}

func TestRegisterDuplicateExtSysCC(t *testing.T) {
	MockExternalSysCCs(true)
	RegisterSysCCs()

	ex := &SystemChaincode{
		Name:              "extscc",
		Path:              "github.com/hyperledger/fabric/core/chaincode/extscc",
		Enabled:           true,
		InvokableExternal: true,
		InvokableCC2CC:    false,
		ConfigPath:        "github.com/hyperledger/fabric/core/chaincode/extscc/config",
	}
	err := RegisterSysCC(ex)

	assert.Error(t, err)
}
