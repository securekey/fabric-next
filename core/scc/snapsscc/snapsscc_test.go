/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package snapsscc

import (
	"fmt"
	"testing"

	"github.com/hyperledger/fabric/core/chaincode/shim"
	"github.com/spf13/viper"
)

// TestInit tests Init method
func TestInit(t *testing.T) {
	e := new(SnapsSCC)
	stub := shim.NewMockStub("snapsscc", e)

	if res := stub.MockInit("txID", nil); res.Status != shim.OK {
		fmt.Println("Init failed", string(res.Message))
		t.FailNow()
	}

}

// TestInvokeInvalidFunction tests Invoke method with an invalid function name
func TestInvokeInvalidFunction(t *testing.T) {
	stub := newMockStub()

	args := [][]byte{}
	if res := stub.MockInvoke("txID", args); res.Status == shim.OK {
		t.Fatalf("snapsscc invoke expecting error for invalid number of args")
	}

	args = [][]byte{[]byte("invalid")}
	if res := stub.MockInvoke("txID", args); res.Status == shim.OK {
		t.Fatalf("snapsscc invoke expecting error for invalid function")
	}
}

// TestInvokeInvalidSnap tests Invoke method without snap name
func TestInvokeInvalidSnap(t *testing.T) {
	stub := newMockStub()

	args := [][]byte{[]byte("invokeSnap")}
	if res := stub.MockInvoke("txID", args); res.Status == shim.OK {
		t.Fatalf("snapsscc invoke expecting error for not providing snap name")
	}
}

// TestAddressNotSet tests Invoke method without setting snaps dispatcher address
func TestAddressNotSet(t *testing.T) {
	stub := newMockStub()

	args := [][]byte{[]byte("invokeSnap"), []byte("mySnap")}
	if res := stub.MockInvoke("txID", args); res.Status == shim.OK {
		t.Fatalf("snapsscc invoke expecting error for not providing snaps dispatcher address")
	}
}

// TestValidAddress tests Invoke method with valid address (snaps dispatcher not running)
func TestValidAddress(t *testing.T) {
	stub := newMockStub()
	viper.Reset()

	viper.Set("chaincode.system.config.snapsscc.snapsDispatcherAddress", "0.0.0.0:8087")

	args := [][]byte{[]byte("invokeSnap"), []byte("mySnap")}
	if res := stub.MockInvoke("txID", args); res.Status == shim.OK {
		t.Fatalf("snapsscc invoke expecting error because snaps dispatcher is not available (connection refused)")
	}
}

// TestTLSDisabled tests Invoke method with TLS disabled
func TestTLSDisabled(t *testing.T) {
	stub := newMockStub()
	viper.Reset()

	viper.Set("chaincode.system.config.snapsscc.snapsDispatcherAddress", "0.0.0.0:8087")

	viper.Set("chaincode.system.config.snapsscc.tls.enabled", false)

	args := [][]byte{[]byte("invokeSnap"), []byte("mySnap")}
	if res := stub.MockInvoke("txID", args); res.Status == shim.OK {
		t.Fatalf("snapsscc invoke expecting error because snaps dispatcher is not available (connection refused)")
	}
}

// TestTLSEnabledNoCredentials tests TLS connection without setting TLS crt
func TestTLSEnabledNoCredentials(t *testing.T) {
	stub := newMockStub()
	viper.Reset()

	viper.Set("chaincode.system.config.snapsscc.snapsDispatcherAddress", "0.0.0.0:8087")

	viper.Set("chaincode.system.config.snapsscc.tls.enabled", true)

	args := [][]byte{[]byte("invokeSnap"), []byte("mySnap")}
	if res := stub.MockInvoke("txID", args); res.Status == shim.OK {
		t.Fatalf("snapsscc invoke expecting error for not providing root cert info")
	}
}

// TestTLSEnabledInvalidCredentials tests Invoke method with invalid TLS cert
func TestTLSEnabledInvalidCredentials(t *testing.T) {
	stub := newMockStub()
	viper.Reset()

	viper.Set("chaincode.system.config.snapsscc.snapsDispatcherAddress", "0.0.0.0:8087")

	viper.Set("chaincode.system.config.snapsscc.tls.enabled", true)
	viper.Set("chaincode.system.config.snapsscc.tls.rootcert.file", "tls/cert.pem")

	args := [][]byte{[]byte("invokeSnap"), []byte("mySnap")}
	if res := stub.MockInvoke("txID", args); res.Status == shim.OK {
		t.Fatalf("snapsscc invoke expecting error for providing invalid root cert")
	}
}

func newMockStub() *shim.MockStub {
	cc := new(SnapsSCC)
	return shim.NewMockStub("snapsscc", cc)
}
