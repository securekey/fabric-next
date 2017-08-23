/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package extcontroller

import (
	"os"
	"testing"
)

func TestGetPeerEnv(t *testing.T) {
	os.Setenv("CORE_TEST", "v1")
	os.Setenv("TEST", "v2")

	envs := getPeerEnv()

	if len(envs) != 1 {
		t.Fatal("Expected only one variable")
	}
	if envs[0] != "CORE_TEST=v1" {
		t.Fatalf("Found unexpected env var. Expected \"CORE_TEST=v1\". Got: %s",
			envs[0])
	}
}
