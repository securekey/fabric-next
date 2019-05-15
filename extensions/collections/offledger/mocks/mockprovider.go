/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mocks

import (
	"context"

	"github.com/hyperledger/fabric/extensions/collections/api/offledger"
	storeapi "github.com/hyperledger/fabric/extensions/collections/api/store"
)

// Provider is a mock off-ledger data data provider
type Provider struct {
}

// RetrieverForChannel returns the retriever for the given channel
func (p *Provider) RetrieverForChannel(channel string) offledger.Retriever {
	return &retriever{}
}

type retriever struct {
}

// GetData gets data for the given key
func (m *retriever) GetData(ctxt context.Context, key *storeapi.Key) (*storeapi.ExpiringValue, error) {
	return &storeapi.ExpiringValue{Value: []byte(key.Key)}, nil
}

// GetDataMultipleKeys gets data for multiple keys
func (m *retriever) GetDataMultipleKeys(ctxt context.Context, key *storeapi.MultiKey) (storeapi.ExpiringValues, error) {
	values := make(storeapi.ExpiringValues, len(key.Keys))
	for i, k := range key.Keys {
		values[i] = &storeapi.ExpiringValue{Value: []byte(k)}
	}
	return values, nil
}
