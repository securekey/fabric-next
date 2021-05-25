/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package endorser

import (
	"testing"

	"github.com/hyperledger/fabric/extensions/endorser/api"
	xgossipapi "github.com/hyperledger/fabric/extensions/gossip/api"
	"github.com/hyperledger/fabric/protos/ledger/rwset"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	channelID = "testchannel"
)

func TestFilterPubSimulationResults(t *testing.T) {
	f := NewCollRWSetFilter(&mockQueryExecutorProviderFactory{}, &mockBlockPublisherProvider{})
	require.NotNil(t, f)

	pubSimulationResults := &rwset.TxReadWriteSet{}
	p, err := f.Filter(channelID, pubSimulationResults)
	assert.NoError(t, err)
	assert.Equal(t, pubSimulationResults, p)
}

type mockQueryExecutorProviderFactory struct {
}

func (m *mockQueryExecutorProviderFactory) GetQueryExecutorProvider(channelID string) api.QueryExecutorProvider {
	return nil
}

type mockBlockPublisherProvider struct {
}

func (m *mockBlockPublisherProvider) ForChannel(channelID string) xgossipapi.BlockPublisher {
	return nil
}
