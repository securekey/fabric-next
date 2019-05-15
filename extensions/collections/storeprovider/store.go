/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package storeprovider

import (
	"github.com/hyperledger/fabric/extensions/collections/api/offledger"
	storeapi "github.com/hyperledger/fabric/extensions/collections/api/store"
	"github.com/hyperledger/fabric/extensions/collections/api/transientdata"
	cb "github.com/hyperledger/fabric/protos/common"
	pb "github.com/hyperledger/fabric/protos/transientstore"
	"github.com/pkg/errors"
)

type store struct {
	channelID          string
	transientDataStore transientdata.Store
	offLedgerStore     offledger.Store
}

func newDelegatingStore(channelID string, transientDataStore transientdata.Store, offLedgerStore offledger.Store) *store {
	return &store{
		channelID:          channelID,
		transientDataStore: transientDataStore,
		offLedgerStore:     offLedgerStore,
	}
}

// Persist persists all transient data within the private data simulation results
func (d *store) Persist(txID string, privateSimulationResultsWithConfig *pb.TxPvtReadWriteSetWithConfigInfo) error {
	if err := d.transientDataStore.Persist(txID, privateSimulationResultsWithConfig); err != nil {
		return errors.WithMessage(err, "error persisting transient data")
	}

	// Off-ledger data should only be persisted on committers
	if err := d.offLedgerStore.Persist(txID, privateSimulationResultsWithConfig); err != nil {
		return errors.WithMessage(err, "error persisting off-ledger data")
	}

	return nil
}

// GetTransientData returns the transient data for the given key
func (d *store) GetTransientData(key *storeapi.Key) (*storeapi.ExpiringValue, error) {
	return d.transientDataStore.GetTransientData(key)
}

// GetTransientData returns the transient data for the given keys
func (d *store) GetTransientDataMultipleKeys(key *storeapi.MultiKey) (storeapi.ExpiringValues, error) {
	return d.transientDataStore.GetTransientDataMultipleKeys(key)
}

// GetData gets the value for the given key
func (d *store) GetData(key *storeapi.Key) (*storeapi.ExpiringValue, error) {
	return d.offLedgerStore.GetData(key)
}

// PutData stores the key/value.
func (d *store) PutData(config *cb.StaticCollectionConfig, key *storeapi.Key, value *storeapi.ExpiringValue) error {
	return d.offLedgerStore.PutData(config, key, value)
}

// GetDataMultipleKeys gets the values for multiple keys in a single call
func (d *store) GetDataMultipleKeys(key *storeapi.MultiKey) (storeapi.ExpiringValues, error) {
	return d.offLedgerStore.GetDataMultipleKeys(key)
}

// Close closes all of the stores store
func (d *store) Close() {
	d.transientDataStore.Close()
	d.offLedgerStore.Close()
}
