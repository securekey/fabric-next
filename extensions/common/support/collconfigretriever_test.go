/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package support

import (
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/core/common/privdata"
	gossipapi "github.com/hyperledger/fabric/extensions/gossip/api"
	"github.com/hyperledger/fabric/extensions/mocks"
	"github.com/hyperledger/fabric/protos/common"
	cb "github.com/hyperledger/fabric/protos/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	coll3 = "collection3"
)

func TestConfigRetriever(t *testing.T) {
	const channelID = "testchannel"
	const lscc = "lscc"

	nsBuilder := mocks.NewNamespaceBuilder(ns1)
	nsBuilder.Collection(coll1).StaticConfig("OR ('Org1.member','Org2.member')", 3, 3, 100)
	nsBuilder.Collection(coll2).TransientConfig("OR ('Org1.member','Org2.member')", 3, 3, "1m")

	configPkgBytes, err := proto.Marshal(nsBuilder.BuildCollectionConfig())
	require.NoError(t, err)

	state := make(map[string]map[string][]byte)
	state[lscc] = make(map[string][]byte)
	state[lscc][privdata.BuildCollectionKVSKey(ns1)] = configPkgBytes

	blockPublisher := &mockBlockPublisher{}

	r := NewCollectionConfigRetriever(channelID, &mocks.Ledger{
		QueryExecutor: mocks.NewQueryExecutor(state),
	}, blockPublisher)
	require.NotNil(t, r)

	t.Run("Policy", func(t *testing.T) {
		policy, err := r.Policy(ns1, coll2)
		require.NoError(t, err)
		require.NotNil(t, policy)
		assert.Equal(t, 2, len(policy.MemberOrgs()))

		policy2, err := r.Policy(ns1, coll2)
		require.NoError(t, err)
		assert.Truef(t, policy == policy2, "expecting policy to be retrieved from cache")

		policy, err = r.Policy(ns1, coll1)
		require.NoError(t, err)
		require.NotNil(t, policy)
		assert.Equal(t, 2, len(policy.MemberOrgs()))
	})

	t.Run("Config", func(t *testing.T) {
		config, err := r.Config(ns1, coll2)
		require.NoError(t, err)
		assert.Equal(t, coll2, config.Name)
		assert.Equal(t, int32(3), config.RequiredPeerCount)
		assert.Equal(t, common.CollectionType_COL_TRANSIENT, config.Type)
		assert.Equal(t, "1m", config.TimeToLive)

		config2, err := r.Config(ns1, coll2)
		require.NoError(t, err)
		assert.Truef(t, config == config2, "expecting config to be retrieved from cache")
	})

	t.Run("Config not found -> error", func(t *testing.T) {
		config, err := r.Config(ns1, coll3)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "configuration not found")
		assert.Nil(t, config)
	})

	t.Run("Chaincode updated", func(t *testing.T) {
		nsBuilder := mocks.NewNamespaceBuilder(ns1)
		nsBuilder.Collection(coll1).StaticConfig("OR ('Org1.member','Org2.member','Org3.member')", 3, 3, 100)
		nsBuilder.Collection(coll2).TransientConfig("OR ('Org1.member','Org2.member','Org3.member')", 4, 3, "10m")

		configPkgBytes, err := proto.Marshal(nsBuilder.BuildCollectionConfig())
		require.NoError(t, err)

		state[lscc][privdata.BuildCollectionKVSKey(ns1)] = configPkgBytes

		err = blockPublisher.handleUpgrade(1001, "tx1", ns1)
		assert.NoError(t, err)

		// Make sure the new config is loaded
		config, err := r.Config(ns1, coll2)
		require.NoError(t, err)
		assert.Equal(t, coll2, config.Name)
		assert.Equal(t, int32(4), config.RequiredPeerCount)
		assert.Equal(t, common.CollectionType_COL_TRANSIENT, config.Type)
		assert.Equal(t, "10m", config.TimeToLive)

		policy, err := r.Policy(ns1, coll2)
		require.NoError(t, err)
		require.NotNil(t, policy)
		assert.Equal(t, 3, len(policy.MemberOrgs()))

		policy, err = r.Policy(ns1, coll1)
		require.NoError(t, err)
		require.NotNil(t, policy)
		assert.Equal(t, 3, len(policy.MemberOrgs()))
	})
}

func TestConfigRetrieverError(t *testing.T) {
	const channelID = "testchannel"

	state := make(map[string]map[string][]byte)

	blockPublisher := &mockBlockPublisher{}

	expectedErr := fmt.Errorf("injected error")
	r := NewCollectionConfigRetriever(channelID, &mocks.Ledger{
		QueryExecutor: mocks.NewQueryExecutor(state).WithError(expectedErr),
	}, blockPublisher)
	require.NotNil(t, r)

	t.Run("Policy", func(t *testing.T) {
		policy, err := r.Policy(ns1, coll2)
		require.Error(t, err)
		assert.Contains(t, err.Error(), expectedErr.Error())
		assert.Nil(t, policy)
	})

	t.Run("Config", func(t *testing.T) {
		config, err := r.Config(ns1, coll2)
		require.Error(t, err)
		assert.Contains(t, err.Error(), expectedErr.Error())
		assert.Nil(t, config)
	})
}

type mockBlockPublisher struct {
	handleUpgrade gossipapi.ChaincodeUpgradeHandler
}

func (m *mockBlockPublisher) AddCCUpgradeHandler(handler gossipapi.ChaincodeUpgradeHandler) {
	m.handleUpgrade = handler
}

func (m *mockBlockPublisher) AddConfigUpdateHandler(handler gossipapi.ConfigUpdateHandler) {
	panic("not implemented")
}

func (m *mockBlockPublisher) AddWriteHandler(handler gossipapi.WriteHandler) {
	panic("not implemented")
}

func (m *mockBlockPublisher) AddReadHandler(handler gossipapi.ReadHandler) {
	panic("not implemented")
}

func (m *mockBlockPublisher) AddCCEventHandler(handler gossipapi.ChaincodeEventHandler) {
	panic("not implemented")
}

func (m *mockBlockPublisher) Publish(block *cb.Block) {
}
