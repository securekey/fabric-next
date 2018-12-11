/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package validationctx

import (
	"sync"

	"golang.org/x/net/context"
)

// Provider is a validation context provider
type Provider struct {
	mutex         sync.Mutex
	cancelByBlock map[uint64]context.CancelFunc
}

// NewProvider returns a new validation context provider
func NewProvider() *Provider {
	return &Provider{
		cancelByBlock: make(map[uint64]context.CancelFunc),
	}
}

// Create creates a new context for the given block number
func (v *Provider) Create(blockNum uint64) context.Context {
	ctx, cancel := context.WithCancel(context.Background())

	v.mutex.Lock()
	defer v.mutex.Unlock()

	v.cancelByBlock[blockNum] = cancel

	return ctx
}

// Cancel cancels any outstanding validation for the given block number
func (v *Provider) Cancel(blockNum uint64) {
	v.mutex.Lock()
	defer v.mutex.Unlock()
	cancel, ok := v.cancelByBlock[blockNum]
	if ok {
		delete(v.cancelByBlock, blockNum)
		cancel()
	}
}
