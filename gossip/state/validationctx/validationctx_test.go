/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package validationctx

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidationCtx(t *testing.T) {
	provider := NewProvider()
	require.NotNil(t, provider)

	blockNum := uint64(1000)
	ctx := provider.Create(blockNum)
	assert.NotNil(t, ctx)

	go provider.Cancel(blockNum)

	select {
	case <-ctx.Done():
		t.Log("Context is done")
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Timed out waiting for cancel")
	}
}
