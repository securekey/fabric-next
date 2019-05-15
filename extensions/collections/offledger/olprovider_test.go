/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package offledger

import (
	"context"
	"testing"
	"time"

	offledgerapi "github.com/hyperledger/fabric/extensions/collections/api/offledger"
	storeapi "github.com/hyperledger/fabric/extensions/collections/api/store"
	supportapi "github.com/hyperledger/fabric/extensions/collections/api/support"
	spmocks "github.com/hyperledger/fabric/extensions/collections/storeprovider/mocks"
	kcommon "github.com/hyperledger/fabric/extensions/common"
	"github.com/hyperledger/fabric/extensions/common/requestmgr"
	"github.com/hyperledger/fabric/extensions/gossip/blockpublisher"
	"github.com/hyperledger/fabric/extensions/mocks"
	ledgerconfig "github.com/hyperledger/fabric/extensions/roles"
	gcommon "github.com/hyperledger/fabric/gossip/common"
	cb "github.com/hyperledger/fabric/protos/common"
	gproto "github.com/hyperledger/fabric/protos/gossip"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	channelID = "testchannel"
	ns1       = "chaincode1"
	coll1     = "collection1"
)

var (
	org1MSPID      = "Org1MSP"
	p1Org1Endpoint = "p1.org1.com"
	p1Org1PKIID    = gcommon.PKIidType("pkiid_P1O1")
	p2Org1Endpoint = "p2.org1.com"
	p2Org1PKIID    = gcommon.PKIidType("pkiid_P2O1")
	p3Org1Endpoint = "p3.org1.com"
	p3Org1PKIID    = gcommon.PKIidType("pkiid_P3O1")

	org2MSPID      = "Org2MSP"
	p1Org2Endpoint = "p1.org2.com"
	p1Org2PKIID    = gcommon.PKIidType("pkiid_P1O2")
	p2Org2Endpoint = "p2.org2.com"
	p2Org2PKIID    = gcommon.PKIidType("pkiid_P2O2")
	p3Org2Endpoint = "p3.org2.com"
	p3Org2PKIID    = gcommon.PKIidType("pkiid_P3O2")

	org3MSPID      = "Org3MSP"
	p1Org3Endpoint = "p1.org3.com"
	p1Org3PKIID    = gcommon.PKIidType("pkiid_P1O3")
	p2Org3Endpoint = "p2.org3.com"
	p2Org3PKIID    = gcommon.PKIidType("pkiid_P2O3")
	p3Org3Endpoint = "p3.org3.com"
	p3Org3PKIID    = gcommon.PKIidType("pkiid_P3O3")

	committerRole = string(ledgerconfig.CommitterRole)
	endorserRole  = string(ledgerconfig.EndorserRole)

	respTimeout = 100 * time.Millisecond

	value1 = &storeapi.ExpiringValue{Value: []byte("value1")}
	value2 = &storeapi.ExpiringValue{Value: []byte("value2")}
	value3 = &storeapi.ExpiringValue{Value: []byte("value3")}
	value4 = &storeapi.ExpiringValue{Value: []byte("value4")}

	key1 = kcommon.GetCASKey(value1.Value)
	key2 = kcommon.GetCASKey(value2.Value)
	key3 = kcommon.GetCASKey(value3.Value)
	key4 = kcommon.GetCASKey(value4.Value)

	txID = "tx1"
)

func TestProvider(t *testing.T) {
	support := mocks.NewMockSupport().
		CollectionPolicy(&mocks.MockAccessPolicy{
			MaxPeerCount: 2,
			Orgs:         []string{org1MSPID, org2MSPID, org3MSPID},
		}).
		CollectionConfig(&cb.StaticCollectionConfig{
			Type: cb.CollectionType_COL_OFFLEDGER,
			Name: coll1,
		})

	getLocalMSPID = func() (string, error) { return org1MSPID, nil }

	gossip := mocks.NewMockGossipAdapter()
	gossip.Self(org1MSPID, mocks.NewMember(p1Org1Endpoint, p1Org1PKIID)).
		Member(org1MSPID, mocks.NewMember(p2Org1Endpoint, p2Org1PKIID, committerRole)).
		Member(org1MSPID, mocks.NewMember(p3Org1Endpoint, p3Org1PKIID, committerRole)).
		Member(org2MSPID, mocks.NewMember(p1Org2Endpoint, p1Org2PKIID, endorserRole)).
		Member(org2MSPID, mocks.NewMember(p2Org2Endpoint, p2Org2PKIID, committerRole)).
		Member(org2MSPID, mocks.NewMember(p3Org2Endpoint, p3Org2PKIID, endorserRole)).
		Member(org3MSPID, mocks.NewMember(p1Org3Endpoint, p1Org3PKIID, endorserRole)).
		Member(org3MSPID, mocks.NewMember(p2Org3Endpoint, p2Org3PKIID, committerRole)).
		Member(org3MSPID, mocks.NewMember(p3Org3Endpoint, p3Org3PKIID, endorserRole))

	localStore := spmocks.NewStore().
		Data(storeapi.NewKey(txID, ns1, coll1, key1), value1)

	storeProvider := func(channelID string) offledgerapi.Store { return localStore }
	gossipProvider := func() supportapi.GossipAdapter { return gossip }

	p := NewProvider(storeProvider, support, gossipProvider)

	retriever := p.RetrieverForChannel(channelID)
	require.NotNil(t, retriever)

	t.Run("GetData - From local peer", func(t *testing.T) {
		ctx, _ := context.WithTimeout(context.Background(), respTimeout)
		value, err := retriever.GetData(ctx, storeapi.NewKey(txID, ns1, coll1, key1))
		require.NoError(t, err)
		require.NotNil(t, value)
		require.Equal(t, value1, value)
	})

	t.Run("GetData - From remote peer", func(t *testing.T) {
		gossip.MessageHandler(
			newMockGossipMsgHandler(channelID).
				Value(key2, value2).
				Handle)

		ctx, _ := context.WithTimeout(context.Background(), respTimeout)
		value, err := retriever.GetData(ctx, storeapi.NewKey(txID, ns1, coll1, key2))
		require.NoError(t, err)
		require.NotNil(t, value)
		require.Equal(t, value2, value)
	})

	t.Run("GetData - No response from remote peer", func(t *testing.T) {
		gossip.MessageHandler(func(msg *gproto.GossipMessage) {})

		ctx, _ := context.WithTimeout(context.Background(), respTimeout)
		value, err := retriever.GetData(ctx, storeapi.NewKey(txID, ns1, coll1, key4))
		require.NoError(t, err)
		assert.Nil(t, value)
	})

	t.Run("GetData - Cancel request from remote peer", func(t *testing.T) {
		gossip.MessageHandler(func(msg *gproto.GossipMessage) {})

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()

		value, err := retriever.GetData(ctx, storeapi.NewKey(txID, ns1, coll1, key4))
		require.NoError(t, err)
		assert.Nil(t, value)
	})

	t.Run("GetDataMultipleKeys -> success", func(t *testing.T) {
		gossip.MessageHandler(
			newMockGossipMsgHandler(channelID).
				Value(key2, value2).
				Value(key3, value3).
				Handle)

		ctx, _ := context.WithTimeout(context.Background(), respTimeout)
		values, err := retriever.GetDataMultipleKeys(ctx, storeapi.NewMultiKey(txID, ns1, coll1, key1, key2, key3))
		require.NoError(t, err)
		require.Equal(t, 3, len(values))
		assert.Equal(t, value1, values[0])
		assert.Equal(t, value2, values[1])
		assert.Equal(t, value3, values[2])
	})

	t.Run("GetDataMultipleKeys - Key not found -> success", func(t *testing.T) {
		gossip.MessageHandler(
			newMockGossipMsgHandler(channelID).
				Value(key1, value1).
				Value(key2, value2).
				Value(key3, value3).
				Handle)

		ctx, _ := context.WithTimeout(context.Background(), respTimeout)
		values, err := retriever.GetDataMultipleKeys(ctx, storeapi.NewMultiKey(txID, ns1, coll1, "xxx", key2, key3))
		require.NoError(t, err)
		require.Equal(t, 3, len(values))
		assert.Nil(t, values[0])
		assert.Equal(t, value2, values[1])
		assert.Equal(t, value3, values[2])
	})

	t.Run("GetDataMultipleKeys - Timeout -> fail", func(t *testing.T) {
		gossip.MessageHandler(
			newMockGossipMsgHandler(channelID).
				Value(key1, value1).
				Value(key2, value2).
				Value(key3, value3).
				Handle)

		ctx, _ := context.WithTimeout(context.Background(), time.Microsecond)
		values, err := retriever.GetDataMultipleKeys(ctx, storeapi.NewMultiKey(txID, ns1, coll1, key1, key2, key3))
		assert.NoError(t, err)
		assert.Empty(t, values.Values().IsEmpty())
	})

	t.Run("Persist Missing -> success", func(t *testing.T) {
		gossip.MessageHandler(
			newMockGossipMsgHandler(channelID).Handle)

		// Should not exist in local store
		ctx, _ := context.WithTimeout(context.Background(), respTimeout)
		value, err := retriever.GetData(ctx, storeapi.NewKey(txID, ns1, coll1, key4))
		require.NoError(t, err)
		require.Nil(t, value)

		gossip.MessageHandler(
			newMockGossipMsgHandler(channelID).
				Value(key4, value4).
				Handle)

		// Should retrieve from other peer and then persist to local store
		value, err = retriever.GetData(ctx, storeapi.NewKey(txID, ns1, coll1, key4))
		require.NoError(t, err)
		require.NotNil(t, value)

		gossip.MessageHandler(
			newMockGossipMsgHandler(channelID).Handle)

		// Should retrieve from local store
		value, err = retriever.GetData(ctx, storeapi.NewKey(txID, ns1, coll1, key4))
		require.NoError(t, err)
		require.NotNil(t, value)
	})
}

func TestProvider_AccessDenied(t *testing.T) {
	support := mocks.NewMockSupport().
		CollectionPolicy(&mocks.MockAccessPolicy{
			MaxPeerCount: 2,
			Orgs:         []string{org1MSPID, org2MSPID},
		})
	support.Publisher = blockpublisher.New(channelID)

	getLocalMSPID = func() (string, error) { return org3MSPID, nil }

	gossip := mocks.NewMockGossipAdapter()
	gossip.Self(org3MSPID, mocks.NewMember(p1Org3Endpoint, p1Org3PKIID)).
		Member(org1MSPID, mocks.NewMember(p2Org1Endpoint, p2Org1PKIID, committerRole)).
		Member(org1MSPID, mocks.NewMember(p3Org1Endpoint, p3Org1PKIID, committerRole)).
		Member(org2MSPID, mocks.NewMember(p1Org2Endpoint, p1Org2PKIID, endorserRole)).
		Member(org2MSPID, mocks.NewMember(p2Org2Endpoint, p2Org2PKIID, committerRole)).
		Member(org2MSPID, mocks.NewMember(p3Org2Endpoint, p3Org2PKIID, endorserRole))

	localStore := spmocks.NewStore().
		Data(storeapi.NewKey(txID, ns1, coll1, key1), value1)

	storeProvider := func(channelID string) offledgerapi.Store { return localStore }
	gossipProvider := func() supportapi.GossipAdapter { return gossip }

	p := NewProvider(storeProvider, support, gossipProvider)

	retriever := p.RetrieverForChannel(channelID)
	require.NotNil(t, retriever)

	gossip.MessageHandler(
		newMockGossipMsgHandler(channelID).
			Value(key2, value2).
			Handle)

	t.Run("GetData - From remote peer -> nil", func(t *testing.T) {
		ctx, _ := context.WithTimeout(context.Background(), respTimeout)
		value, err := retriever.GetData(ctx, storeapi.NewKey(txID, ns1, coll1, key2))
		assert.NoError(t, err)
		assert.Nil(t, value)
	})

	t.Run("GetDataMultipleKeys - From remote peer -> nil", func(t *testing.T) {
		ctx, _ := context.WithTimeout(context.Background(), respTimeout)
		values, err := retriever.GetDataMultipleKeys(ctx, storeapi.NewMultiKey(txID, ns1, coll1, "xxx", key2, key3))
		assert.NoError(t, err)
		assert.Nil(t, values)
	})

	t.Run("Missing from local", func(t *testing.T) {
		gossip.MessageHandler(
			newMockGossipMsgHandler(channelID).
				Value(key4, value4).
				Handle)

		// Shouldn't be authorized to retrieve from other peer
		ctx, _ := context.WithTimeout(context.Background(), respTimeout)
		value, err := retriever.GetData(ctx, storeapi.NewKey(txID, ns1, coll1, key4))
		require.NoError(t, err)
		require.Nil(t, value)
	})
}

type mockGossipMsgHandler struct {
	channelID string
	values    map[string]*storeapi.ExpiringValue
}

func newMockGossipMsgHandler(channelID string) *mockGossipMsgHandler {
	return &mockGossipMsgHandler{
		channelID: channelID,
		values:    make(map[string]*storeapi.ExpiringValue),
	}
}

func (m *mockGossipMsgHandler) Value(key string, value *storeapi.ExpiringValue) *mockGossipMsgHandler {
	m.values[key] = value
	return m
}

func (m *mockGossipMsgHandler) Handle(msg *gproto.GossipMessage) {
	req := msg.GetCollDataReq()

	res := &requestmgr.Response{
		Endpoint:  "p1.org1",
		MSPID:     "org1",
		Identity:  []byte("p1.org1"),
		Signature: []byte("sig"),
	}

	for _, d := range req.Digests {
		e := &requestmgr.Element{
			Namespace:  d.Namespace,
			Collection: d.Collection,
			Key:        d.Key,
		}

		v := m.values[d.Key]
		if v != nil {
			e.Value = v.Value
			e.Expiry = v.Expiry
		}

		res.Data = append(res.Data, e)
	}

	requestmgr.Get(m.channelID).Respond(req.Nonce, res)
}
