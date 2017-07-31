/*

 Copyright SecureKey Technologies Inc. All Rights Reserved.

 SPDX-License-Identifier: Apache-2.0


Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"fmt"

	"github.com/hyperledger/fabric/core/chaincode/shim"
	pb "github.com/hyperledger/fabric/protos/peer"
)

// SimpleCahincodeCaller example simple Chaincode implementation
type SimpleCahincodeCaller struct {
}

var logger = shim.NewLogger("extscccaller")

// Init ...
func (t *SimpleCahincodeCaller) Init(stub shim.ChaincodeStubInterface) pb.Response {
	logger.Info("-----------------------------------------------")
	logger.Info("=========================> extscc Caller Init")
	logger.Info("-----------------------------------------------")
	logger.Infof("---------------------->>>>stub args are %s", stub.GetArgs())
	return shim.Success(nil)
	//return shim.Error("============ Testing if INIT is called by the peer ==================")
}

// Query ...
func (t *SimpleCahincodeCaller) Query(stub shim.ChaincodeStubInterface) pb.Response {
	return shim.Error("Unknown supported call")
}

// Invoke ...
func (t *SimpleCahincodeCaller) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	callArgs := [][]byte{[]byte("invoke")}
	callArgs = append(callArgs, []byte("Hello extscc from Caller"))
	logger.Infof("***********************===================-------------------")
	logger.Infof("*********************** extscc Caller Invoke - About to invoke extscc with args: %s", callArgs)
	logger.Infof("***********************===================-------------------")

	response := stub.InvokeChaincode("extscc1", callArgs, "")

	if response.Status != shim.OK {
		errStr := fmt.Sprintf("Failed to invoke chaincode %s from extscccaller_cc . Error: %s", "extscc1", string(response.Message))
		logger.Warning(errStr)
		return shim.Error(errStr)
	}
	return response
	//return shim.Success([]byte("Hello World from ExtScccaller"))
}

func main() {
	fmt.Println("EXT SCC Caller Main")
	err := shim.Start(new(SimpleCahincodeCaller))
	if err != nil {
		logger.Errorf("Error starting extscccaller chaincode: %s", err)
	}
}
