/*
Copyright IBM Corp. 2016 All Rights Reserved.

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

package extcontroller

import (
	"fmt"
	"io"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/chaincode/shim"
	container "github.com/hyperledger/fabric/core/container/api"
	"github.com/hyperledger/fabric/core/container/ccintf"

	"golang.org/x/net/context"
)

type extContainer struct {
	chaincode shim.Chaincode
	running   bool
	args      []string
	env       []string
}

var (
	extLogger    = flogging.MustGetLogger("extcontroller")
	typeRegistry = make(map[string]*extContainer)
	instRegistry = make(map[string]*extContainer)
)

// errors

//ExtSysCCRegisteredErr registered error
type ExtSysCCRegisteredErr string

func (s ExtSysCCRegisteredErr) Error() string {
	return fmt.Sprintf("%s already registered", string(s))
}

//Register registers remote system chaincode with given path. The deploy should be called to initialize
func Register(path string, cc shim.Chaincode) error {
	tmp := typeRegistry[path]
	if tmp != nil {
		return ExtSysCCRegisteredErr(path)
	}

	typeRegistry[path] = &extContainer{chaincode: cc}
	return nil
}

//ExtVM is a vm. It is identified by a executable name
type ExtVM struct {
	id string
}

func (vm *ExtVM) getInstance(ctxt context.Context, ipctemplate *extContainer, instName string, args []string, env []string) (*extContainer, error) {
	ipc := instRegistry[instName]
	if ipc != nil {
		extLogger.Warningf("chaincode instance exists for %s", instName)
		return ipc, nil
	}
	ipc = &extContainer{args: args, env: env, chaincode: ipctemplate.chaincode}
	instRegistry[instName] = ipc
	extLogger.Debugf("chaincode instance created for %s", instName)
	return ipc, nil
}

//Deploy verifies chaincode is registered and creates an instance for it. Currently only one instance can be created
func (vm *ExtVM) Deploy(ctxt context.Context, ccid ccintf.CCID, args []string, env []string, reader io.Reader) error {
	path := ccid.ChaincodeSpec.ChaincodeId.Path

	ipctemplate := typeRegistry[path]
	if ipctemplate == nil {
		return fmt.Errorf(fmt.Sprintf("%s not registered", path))
	}

	if ipctemplate.chaincode == nil {
		return fmt.Errorf(fmt.Sprintf("%s system chaincode does not contain chaincode instance", path))
	}

	instName, _ := vm.GetVMName(ccid)
	_, err := vm.getInstance(ctxt, ipctemplate, instName, args, env)

	//FUTURE ... here is where we might check code for safety
	extLogger.Debugf("registered : %s", path)

	return err
}

//Start starts a previously registered system codechain
func (vm *ExtVM) Start(ctxt context.Context, ccid ccintf.CCID, args []string, env []string, builder container.BuildSpecFactory, prelaunchFunc container.PrelaunchFunc) error {
	path := ccid.ChaincodeSpec.ChaincodeId.Path

	ipctemplate := typeRegistry[path]

	if ipctemplate == nil {
		return fmt.Errorf(fmt.Sprintf("%s not registered", path))
	}

	instName, _ := vm.GetVMName(ccid)

	rc, err := vm.getInstance(ctxt, ipctemplate, instName, args, env)

	if err != nil {
		return fmt.Errorf(fmt.Sprintf("could not create instance for %s", instName))
	}

	if rc.running {
		return fmt.Errorf(fmt.Sprintf("chaincode running %s", path))
	}

	//TODO VALIDITY CHECKS ?

	if prelaunchFunc != nil {
		if err = prelaunchFunc(); err != nil {
			return err
		}
	}

	rc.running = true

	return nil
}

//Stop stops a system codechain
func (vm *ExtVM) Stop(ctxt context.Context, ccid ccintf.CCID, timeout uint, dontkill bool, dontremove bool) error {
	path := ccid.ChaincodeSpec.ChaincodeId.Path

	ipctemplate := typeRegistry[path]
	if ipctemplate == nil {
		return fmt.Errorf("%s not registered", path)
	}

	instName, _ := vm.GetVMName(ccid)

	ipc := instRegistry[instName]

	if ipc == nil {
		return fmt.Errorf("%s not found", instName)
	}

	if !ipc.running {
		return fmt.Errorf("%s not running", instName)
	}

	delete(instRegistry, instName)
	//TODO stop
	return nil
}

//Destroy destroys an image
func (vm *ExtVM) Destroy(ctxt context.Context, ccid ccintf.CCID, force bool, noprune bool) error {
	//not implemented
	return nil
}

//GetVMName ignores the peer and network name as it just needs to be unique in process
func (vm *ExtVM) GetVMName(ccid ccintf.CCID) (string, error) {
	return ccid.GetName(), nil
}
