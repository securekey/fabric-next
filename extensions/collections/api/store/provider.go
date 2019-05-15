/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package store

import (
	"context"
	"time"

	"github.com/hyperledger/fabric/extensions/common"
	cb "github.com/hyperledger/fabric/protos/common"
	proto "github.com/hyperledger/fabric/protos/transientstore"
)

// ExpiringValue holds the value and expiration time.
type ExpiringValue struct {
	Value  []byte
	Expiry time.Time
}

// ExpiringValues is an array of ExpiringValue
type ExpiringValues []*ExpiringValue

// Store manages the storage of private data collections.
type Store interface {
	// Persist stores the private write set of a transaction.
	Persist(txid string, privateSimulationResultsWithConfig *proto.TxPvtReadWriteSetWithConfigInfo) error

	// GetTransientData gets the value for the given transient data item
	GetTransientData(key *Key) (*ExpiringValue, error)

	// GetTransientDataMultipleKeys gets the values for the multiple transient data items in a single call
	GetTransientDataMultipleKeys(key *MultiKey) (ExpiringValues, error)

	// GetData gets the value for the given item
	GetData(key *Key) (*ExpiringValue, error)

	// GetDataMultipleKeys gets the values for the multiple items in a single call
	GetDataMultipleKeys(key *MultiKey) (ExpiringValues, error)

	// PutData stores the key/value.
	PutData(config *cb.StaticCollectionConfig, key *Key, value *ExpiringValue) error

	// Close closes the store
	Close()
}

// Retriever retrieves private data
type Retriever interface {
	// GetTransientData gets the value for the given transient data item
	GetTransientData(ctxt context.Context, key *Key) (*ExpiringValue, error)

	// GetTransientDataMultipleKeys gets the values for the multiple transient data items in a single call
	GetTransientDataMultipleKeys(ctxt context.Context, key *MultiKey) (ExpiringValues, error)

	// GetData gets the value for the given data item
	GetData(ctxt context.Context, key *Key) (*ExpiringValue, error)

	// GetDataMultipleKeys gets the values for the multiple data items in a single call
	GetDataMultipleKeys(ctxt context.Context, key *MultiKey) (ExpiringValues, error)
}

// Provider provides transient data retrievers
type Provider interface {
	RetrieverForChannel(channel string) Retriever
}

// Values returns the ExpiringValues as Values
func (ev ExpiringValues) Values() common.Values {
	vals := make(common.Values, len(ev))
	for i, v := range ev {
		vals[i] = v
	}
	return vals
}

// AsExpiringValues converts Values into ExpiringValues
func AsExpiringValues(cv common.Values) ExpiringValues {
	vals := make(ExpiringValues, len(cv))
	for i, v := range cv {
		if common.IsNil(v) {
			vals[i] = nil
		} else {
			vals[i] = v.(*ExpiringValue)
		}
	}
	return vals
}
