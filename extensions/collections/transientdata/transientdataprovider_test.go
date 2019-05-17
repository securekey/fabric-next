/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package transientdata

import (
	"context"
	"testing"
	"time"

	storeapi "github.com/hyperledger/fabric/extensions/collections/api/store"
	supportapi "github.com/hyperledger/fabric/extensions/collections/api/support"
	tdataapi "github.com/hyperledger/fabric/extensions/collections/api/transientdata"
	spmocks "github.com/hyperledger/fabric/extensions/collections/storeprovider/mocks"
	"github.com/hyperledger/fabric/extensions/common/requestmgr"
	"github.com/hyperledger/fabric/extensions/mocks"
	ledgerconfig "github.com/hyperledger/fabric/extensions/roles"
	gcommon "github.com/hyperledger/fabric/gossip/common"
	gproto "github.com/hyperledger/fabric/protos/gossip"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	channelID = "testchannel"
	ns1       = "chaincode1"
	coll1     = "collection1"
	key1      = "key1"
	key2      = "key2"
	key3      = "key3"
	txID      = "tx1"
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

	validatorRole = string(ledgerconfig.ValidatorRole)
	endorserRole  = string(ledgerconfig.EndorserRole)

	respTimeout = 100 * time.Millisecond
)

func TestTransientDataProvider(t *testing.T) {
	value1 := &storeapi.ExpiringValue{Value: []byte("value1")}
	value2 := &storeapi.ExpiringValue{Value: []byte("value2")}
	value3 := &storeapi.ExpiringValue{Value: []byte("value3")}

	support := mocks.NewMockSupport().
		CollectionPolicy(&mocks.MockAccessPolicy{
			MaxPeerCount: 2,
			Orgs:         []string{org1MSPID, org2MSPID, org3MSPID},
		})

	getLocalMSPID = func() (string, error) { return org1MSPID, nil }

	gossip := mocks.NewMockGossipAdapter()
	gossip.Self(org1MSPID, mocks.NewMember(p1Org1Endpoint, p1Org1PKIID)).
		Member(org1MSPID, mocks.NewMember(p2Org1Endpoint, p2Org1PKIID, validatorRole)).
		Member(org1MSPID, mocks.NewMember(p3Org1Endpoint, p3Org1PKIID, validatorRole)).
		Member(org2MSPID, mocks.NewMember(p1Org2Endpoint, p1Org2PKIID, endorserRole)).
		Member(org2MSPID, mocks.NewMember(p2Org2Endpoint, p2Org2PKIID, validatorRole)).
		Member(org2MSPID, mocks.NewMember(p3Org2Endpoint, p3Org2PKIID, endorserRole)).
		Member(org3MSPID, mocks.NewMember(p1Org3Endpoint, p1Org3PKIID, endorserRole)).
		Member(org3MSPID, mocks.NewMember(p2Org3Endpoint, p2Org3PKIID, validatorRole)).
		Member(org3MSPID, mocks.NewMember(p3Org3Endpoint, p3Org3PKIID, endorserRole)).IdentityInfo()

	localStore := spmocks.NewTransientDataStore().
		Data(storeapi.NewKey(txID, ns1, coll1, key1), value1)

	p := &Provider{
		support:         support,
		storeForChannel: func(channelID string) tdataapi.Store { return localStore },
		gossipAdapter:   func() supportapi.GossipAdapter { return gossip },
	}

	retriever := p.RetrieverForChannel(channelID)
	require.NotNil(t, retriever)

	t.Run("GetTransientData - From local peer", func(t *testing.T) {
		ctx, _ := context.WithTimeout(context.Background(), respTimeout)
		value, err := retriever.GetTransientData(ctx, storeapi.NewKey(txID, ns1, coll1, key1))
		require.NoError(t, err)
		require.NotNil(t, value)
		require.Equal(t, value1, value)
	})

	t.Run("GetTransientData - From remote peer", func(t *testing.T) {
		gossip.MessageHandler(
			newMockGossipMsgHandler(channelID).
				Value(key2, value2).
				Handle)

		ctx, _ := context.WithTimeout(context.Background(), respTimeout)
		value, err := retriever.GetTransientData(ctx, storeapi.NewKey(txID, ns1, coll1, key2))
		require.NoError(t, err)
		require.NotNil(t, value)
		require.Equal(t, value2, value)
	})

	t.Run("GetTransientData - No response from remote peer", func(t *testing.T) {
		gossip.MessageHandler(func(msg *gproto.GossipMessage) {})

		ctx, _ := context.WithTimeout(context.Background(), respTimeout)
		value, err := retriever.GetTransientData(ctx, storeapi.NewKey(txID, ns1, coll1, key2))
		require.NoError(t, err)
		assert.Nil(t, value)
	})

	t.Run("GetTransientData - Cancel request from remote peer", func(t *testing.T) {
		gossip.MessageHandler(func(msg *gproto.GossipMessage) {})

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()

		value, err := retriever.GetTransientData(ctx, storeapi.NewKey(txID, ns1, coll1, key2))
		require.NoError(t, err)
		assert.Nil(t, value)
	})

	t.Run("GetTransientDataMultipleKeys - 1 key -> success", func(t *testing.T) {
		gossip.MessageHandler(
			newMockGossipMsgHandler(channelID).
				Value(key1, value2).
				Handle)

		ctx, _ := context.WithTimeout(context.Background(), respTimeout)
		values, err := retriever.GetTransientDataMultipleKeys(ctx, storeapi.NewMultiKey(txID, ns1, coll1, key1))
		require.NoError(t, err)
		require.Equal(t, 1, len(values))
		assert.Equal(t, value1, values[0])
	})

	t.Run("GetTransientDataMultipleKeys - 3 keys -> success", func(t *testing.T) {
		gossip.MessageHandler(
			newMockGossipMsgHandler(channelID).
				Value(key1, value1).
				Value(key2, value2).
				Value(key3, value3).
				Handle)

		ctx, _ := context.WithTimeout(context.Background(), respTimeout)
		values, err := retriever.GetTransientDataMultipleKeys(ctx, storeapi.NewMultiKey(txID, ns1, coll1, key1, key2, key3))
		require.NoError(t, err)
		require.Equal(t, 3, len(values))
		assert.Equal(t, value1, values[0])
		assert.Equal(t, value2, values[1])
		assert.Equal(t, value3, values[2])
	})

	t.Run("GetTransientDataMultipleKeys - Key not found -> success", func(t *testing.T) {
		gossip.MessageHandler(
			newMockGossipMsgHandler(channelID).
				Value(key1, value1).
				Value(key2, value2).
				Value(key3, value3).
				Handle)

		ctx, _ := context.WithTimeout(context.Background(), respTimeout)
		values, err := retriever.GetTransientDataMultipleKeys(ctx, storeapi.NewMultiKey(txID, ns1, coll1, "xxx", key2, key3))
		require.NoError(t, err)
		require.Equal(t, 3, len(values))
		assert.Nil(t, values[0])
		assert.Equal(t, value2, values[1])
		assert.Equal(t, value3, values[2])
	})

	t.Run("GetTransientDataMultipleKeys - Timeout -> fail", func(t *testing.T) {
		handler := newMockGossipMsgHandler(channelID).
			Value(key1, value1).
			Value(key2, value2).
			Value(key3, value3)
		gossip.MessageHandler(
			func(msg *gproto.GossipMessage) {
				time.Sleep(10 * time.Millisecond)
				handler.Handle(msg)
			},
		)

		ctx, _ := context.WithTimeout(context.Background(), time.Microsecond)
		values, err := retriever.GetTransientDataMultipleKeys(ctx, storeapi.NewMultiKey(txID, ns1, coll1, key2, key3))
		require.NoError(t, err)
		assert.True(t, values.Values().IsEmpty())
	})
}

func TestTransientDataProvider_AccessDenied(t *testing.T) {
	value1 := &storeapi.ExpiringValue{Value: []byte("value1")}
	value2 := &storeapi.ExpiringValue{Value: []byte("value2")}

	getLocalMSPID = func() (string, error) { return org3MSPID, nil }

	gossip := mocks.NewMockGossipAdapter()
	gossip.Self(org3MSPID, mocks.NewMember(p1Org3Endpoint, p1Org3PKIID)).
		Member(org1MSPID, mocks.NewMember(p2Org1Endpoint, p2Org1PKIID, validatorRole)).
		Member(org1MSPID, mocks.NewMember(p3Org1Endpoint, p3Org1PKIID, validatorRole)).
		Member(org2MSPID, mocks.NewMember(p1Org2Endpoint, p1Org2PKIID, endorserRole)).
		Member(org2MSPID, mocks.NewMember(p2Org2Endpoint, p2Org2PKIID, validatorRole)).
		Member(org2MSPID, mocks.NewMember(p3Org2Endpoint, p3Org2PKIID, endorserRole))

	gossip.MessageHandler(
		newMockGossipMsgHandler(channelID).
			Value(key2, value2).
			Handle)

	localStore := spmocks.NewTransientDataStore().
		Data(storeapi.NewKey(txID, ns1, coll1, key1), value1)

	support := mocks.NewMockSupport().
		CollectionPolicy(&mocks.MockAccessPolicy{
			MaxPeerCount: 2,
			Orgs:         []string{org1MSPID, org2MSPID},
		})

	p := &Provider{
		support:         support,
		storeForChannel: func(channelID string) tdataapi.Store { return localStore },
		gossipAdapter:   func() supportapi.GossipAdapter { return gossip },
	}

	retriever := p.RetrieverForChannel(channelID)
	require.NotNil(t, retriever)

	t.Run("GetTransientData - From remote peer -> nil (not authorized)", func(t *testing.T) {
		ctx, _ := context.WithTimeout(context.Background(), respTimeout)
		value, err := retriever.GetTransientData(ctx, storeapi.NewKey(txID, ns1, coll1, key2))
		assert.NoError(t, err)
		assert.Nil(t, value)
	})

	t.Run("GetTransientDataMultipleKeys - From remote peer -> nil (not authorized)", func(t *testing.T) {
		ctx, _ := context.WithTimeout(context.Background(), respTimeout)
		values, err := retriever.GetTransientDataMultipleKeys(ctx, storeapi.NewMultiKey(txID, ns1, coll1, key2))
		assert.NoError(t, err)
		require.Equal(t, 1, len(values))
		assert.Nil(t, values[0])
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
