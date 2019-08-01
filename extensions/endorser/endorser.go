/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package endorser

import (
	"github.com/hyperledger/fabric/extensions/endorser/api"
	"github.com/hyperledger/fabric/protos/ledger/rwset"
	extendorser "github.com/trustbloc/fabric-peer-ext/pkg/endorser"
)

// CollRWSetFilter filters out all off-ledger (including transient data) read-write sets from the simulation results
// so that they won't be included in the block.
type CollRWSetFilter interface {
	Filter(channelID string, pubSimulationResults *rwset.TxReadWriteSet) (*rwset.TxReadWriteSet, error)
}

// NewCollRWSetFilter returns a new collection RW set filter
func NewCollRWSetFilter(api.QueryExecutorProviderFactory, api.BlockPublisherProvider) CollRWSetFilter {
	return extendorser.NewCollRWSetFilter()
}
