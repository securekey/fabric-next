/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package dispatcher

import (
	"context"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/core/common/privdata"
	"github.com/hyperledger/fabric/extensions/collections/api/store"
	"github.com/hyperledger/fabric/extensions/common"
	"github.com/hyperledger/fabric/extensions/common/requestmgr"
	gmocks "github.com/hyperledger/fabric/extensions/gossip/mocks"
	"github.com/hyperledger/fabric/extensions/mocks"
	ledgerconfig "github.com/hyperledger/fabric/extensions/roles"
	gcommon "github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/gossip/discovery"
	"github.com/hyperledger/fabric/gossip/protoext"
	gproto "github.com/hyperledger/fabric/protos/gossip"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	org1MSPID      = "Org1MSP"
	p1Org1Endpoint = "p1.org1.com"
	p2Org1Endpoint = "p2.org1.com"
	p3Org1Endpoint = "p3.org1.com"

	org2MSPID      = "Org2MSP"
	p1Org2Endpoint = "p1.org2.com"
	p2Org2Endpoint = "p2.org2.com"
	p3Org2Endpoint = "p3.org2.com"

	org3MSPID      = "Org3MSP"
	p1Org3Endpoint = "p1.org3.com"
	p2Org3Endpoint = "p2.org3.com"
	p3Org3Endpoint = "p3.org3.com"
)

var (
	p1Org1PKIID = gcommon.PKIidType("pkiid_P1O1")
	p2Org1PKIID = gcommon.PKIidType("pkiid_P2O1")
	p3Org1PKIID = gcommon.PKIidType("pkiid_P3O1")

	p1Org2PKIID = gcommon.PKIidType("pkiid_P1O2")
	p2Org2PKIID = gcommon.PKIidType("pkiid_P2O2")
	p3Org2PKIID = gcommon.PKIidType("pkiid_P3O2")

	p1Org3PKIID = gcommon.PKIidType("pkiid_P1O3")
	p2Org3PKIID = gcommon.PKIidType("pkiid_P2O3")
	p3Org3PKIID = gcommon.PKIidType("pkiid_P3O3")

	endorserRole  = string(ledgerconfig.EndorserRole)
	committerRole = string(ledgerconfig.CommitterRole)
)

func TestDispatchUnhandled(t *testing.T) {
	const channelID = "testchannel"

	dispatcher := New(
		channelID,
		&mocks.DataStore{},
		mocks.NewMockGossipAdapter(),
		&mocks.Ledger{QueryExecutor: mocks.NewQueryExecutor(nil)},
		gmocks.NewBlockPublisher(),
	)

	var response *gproto.GossipMessage
	msg := &mockReceivedMessage{
		message: newDataMsg(channelID),
		respondTo: func(msg *gproto.GossipMessage) {
			response = msg
		},
	}
	assert.False(t, dispatcher.Dispatch(msg))
	require.Nil(t, response)
}

func TestDispatchDataRequest(t *testing.T) {
	const channelID = "testchannel"
	const lscc = "lscc"
	const ns1 = "ns1"
	const ns2 = "ns2"
	const coll1 = "coll1"
	const coll2 = "coll2"

	key1 := store.NewKey("txID1", ns1, coll1, "key1")
	key2 := store.NewKey("txID1", ns2, coll2, "key2")
	key3 := store.NewKey("txID1", ns1, coll2, "key3")
	key4 := store.NewKey("txID1", ns2, coll1, "key4")

	value1 := &store.ExpiringValue{Value: []byte("value1")}
	value2 := &store.ExpiringValue{Value: []byte("value2")}
	value3 := &store.ExpiringValue{Value: []byte("value3")}
	value4 := &store.ExpiringValue{Value: []byte("value4")}

	nsBuilder1 := mocks.NewNamespaceBuilder(ns1)
	nsBuilder1.Collection(coll1).TransientConfig("OR ('Org1MSP.member','Org2MSP.member')", 3, 3, "1m")
	nsBuilder1.Collection(coll2).DCASConfig("OR ('Org1MSP.member','Org2MSP.member')", 3, 3, "1m")

	nsBuilder2 := mocks.NewNamespaceBuilder(ns2)
	nsBuilder2.Collection(coll2).TransientConfig("OR ('Org1MSP.member','Org2MSP.member','Org3MSP.member')", 3, 3, "1m")
	nsBuilder2.Collection(coll1).DCASConfig("OR ('Org1MSP.member','Org2MSP.member','Org3MSP.member')", 3, 3, "1m")

	configPkgBytes1, err := proto.Marshal(nsBuilder1.BuildCollectionConfig())
	require.NoError(t, err)
	configPkgBytes2, err := proto.Marshal(nsBuilder2.BuildCollectionConfig())
	require.NoError(t, err)

	state := make(map[string]map[string][]byte)
	state[lscc] = make(map[string][]byte)
	state[lscc][privdata.BuildCollectionKVSKey(ns1)] = configPkgBytes1
	state[lscc][privdata.BuildCollectionKVSKey(ns2)] = configPkgBytes2

	gossipAdapter := mocks.NewMockGossipAdapter()
	gossipAdapter.Self(org1MSPID, mocks.NewMember(p1Org1Endpoint, p1Org1PKIID)).
		Member(org1MSPID, mocks.NewMember(p2Org1Endpoint, p2Org1PKIID, committerRole)).
		Member(org1MSPID, mocks.NewMember(p3Org1Endpoint, p3Org1PKIID, committerRole)).
		Member(org2MSPID, mocks.NewMember(p1Org2Endpoint, p1Org2PKIID, endorserRole)).
		Member(org2MSPID, mocks.NewMember(p2Org2Endpoint, p2Org2PKIID, committerRole)).
		Member(org2MSPID, mocks.NewMember(p3Org2Endpoint, p3Org2PKIID, endorserRole)).
		Member(org3MSPID, mocks.NewMember(p1Org3Endpoint, p1Org3PKIID, endorserRole)).
		Member(org3MSPID, mocks.NewMember(p2Org3Endpoint, p2Org3PKIID, committerRole)).
		Member(org3MSPID, mocks.NewMember(p3Org3Endpoint, p3Org3PKIID, endorserRole))

	dispatcher := New(
		channelID,
		mocks.NewDataStore().TransientData(key1, value1).TransientData(key2, value2).Data(key3, value3).Data(key4, value4),
		gossipAdapter,
		&mocks.Ledger{QueryExecutor: mocks.NewQueryExecutor(state)},
		gmocks.NewBlockPublisher(),
	)
	require.NotNil(t, dispatcher)

	t.Run("Endorser -> success", func(t *testing.T) {
		reqID1 := uint64(1000)

		var response *gproto.GossipMessage
		msg := &mockReceivedMessage{
			message: newCollDataReqMsg(channelID, reqID1, key1, key2, key3, key4),
			respondTo: func(msg *gproto.GossipMessage) {
				response = msg
			},
			member: mocks.NewMember(p1Org2Endpoint, p1Org2PKIID, endorserRole),
		}
		assert.True(t, dispatcher.Dispatch(msg))
		require.NotNil(t, response)
		assert.Equal(t, []byte(channelID), response.Channel)
		assert.Equal(t, gproto.GossipMessage_CHAN_ONLY, response.Tag)

		res := response.GetCollDataRes()
		require.NotNil(t, res)
		assert.Equal(t, reqID1, res.Nonce)
		require.Equal(t, 4, len(res.Elements))

		element := res.Elements[0]
		require.NotNil(t, element.Digest)
		assert.Equal(t, key1.Namespace, element.Digest.Namespace)
		assert.Equal(t, key1.Collection, element.Digest.Collection)
		assert.Equal(t, key1.Key, element.Digest.Key)
		assert.Equal(t, value1.Value, element.Value)

		element = res.Elements[1]
		require.NotNil(t, element.Digest)
		assert.Equal(t, key2.Namespace, element.Digest.Namespace)
		assert.Equal(t, key2.Collection, element.Digest.Collection)
		assert.Equal(t, key2.Key, element.Digest.Key)
		assert.Equal(t, value2.Value, element.Value)

		element = res.Elements[2]
		require.NotNil(t, element.Digest)
		assert.Equal(t, key3.Namespace, element.Digest.Namespace)
		assert.Equal(t, key3.Collection, element.Digest.Collection)
		assert.Equal(t, key3.Key, element.Digest.Key)
		assert.Equal(t, value3.Value, element.Value)

		element = res.Elements[3]
		require.NotNil(t, element.Digest)
		assert.Equal(t, key4.Namespace, element.Digest.Namespace)
		assert.Equal(t, key4.Collection, element.Digest.Collection)
		assert.Equal(t, key4.Key, element.Digest.Key)
		assert.Equal(t, value4.Value, element.Value)
	})

	t.Run("Non-Endorser -> no response", func(t *testing.T) {
		f := isEndorser
		defer func() { isEndorser = f }()
		isEndorser = func() bool { return false }

		reqID2 := uint64(1001)

		var response *gproto.GossipMessage
		msg := &mockReceivedMessage{
			message: newCollDataReqMsg(channelID, reqID2, key1, key2),
			respondTo: func(msg *gproto.GossipMessage) {
				response = msg
			},
		}
		assert.True(t, dispatcher.Dispatch(msg))
		require.Nil(t, response)
	})

	t.Run("Access Denied -> nil response", func(t *testing.T) {
		reqID2 := uint64(1001)

		var response *gproto.GossipMessage
		msg := &mockReceivedMessage{
			message: newCollDataReqMsg(channelID, reqID2, key1, key2, key3, key4),
			respondTo: func(msg *gproto.GossipMessage) {
				response = msg
			},
			member: mocks.NewMember(p1Org3Endpoint, p1Org3PKIID, endorserRole), // An Org3 member is requesting data he doesn't have access to
		}
		assert.True(t, dispatcher.Dispatch(msg))
		require.NotNil(t, response)
		require.NotNil(t, response.GetCollDataRes())
		require.Equal(t, 4, len(response.GetCollDataRes().Elements))

		// Org3 doesn't have access to ns1:collection1
		require.NotNil(t, response.GetCollDataRes().Elements[0])
		assert.Nil(t, response.GetCollDataRes().Elements[0].Value)

		// Org3 has access to ns2:collection2
		require.NotNil(t, response.GetCollDataRes().Elements[1])
		assert.NotNil(t, response.GetCollDataRes().Elements[1].Value)

		// Org3 doesn't have access to ns1:collection2
		require.NotNil(t, response.GetCollDataRes().Elements[2])
		assert.Nil(t, response.GetCollDataRes().Elements[2].Value)

		// Org3 has access to ns2:collection1
		require.NotNil(t, response.GetCollDataRes().Elements[3])
		assert.NotNil(t, response.GetCollDataRes().Elements[3].Value)
	})
}

func TestDispatchDataResponse(t *testing.T) {
	const channelID = "testchannel"
	key1 := store.NewKey("txID1", "ns1", "coll1", "key1")
	key2 := store.NewKey("txID1", "ns2", "coll2", "key2")

	value1 := &store.ExpiringValue{Value: []byte("value1")}
	value2 := &store.ExpiringValue{Value: []byte("value2")}

	p1Org1 := mocks.NewMember(p1Org1Endpoint, p1Org1PKIID)
	p1Org2 := mocks.NewMember(p1Org2Endpoint, p1Org2PKIID, endorserRole)

	gossip := mocks.NewMockGossipAdapter().
		Self(org1MSPID, p1Org1).
		Member(org2MSPID, p1Org2)

	dispatcher := New(
		channelID,
		mocks.NewDataStore().TransientData(key1, value1).TransientData(key2, value2),
		gossip,
		&mocks.Ledger{QueryExecutor: mocks.NewQueryExecutor(nil)},
		gmocks.NewBlockPublisher(),
	)
	require.NotNil(t, dispatcher)

	reqMgr := requestmgr.Get(channelID)
	require.NotNil(t, reqMgr)

	t.Run("Endorser -> success", func(t *testing.T) {
		req := reqMgr.NewRequest()

		msg := &mockReceivedMessage{
			message: newTransientDataResMsg(channelID, req.ID(), newKeyValue(key1, value1), newKeyValue(key2, value2)),
			member:  p1Org2,
		}

		go func() {
			if !dispatcher.Dispatch(msg) {
				t.Fatal("Message not handled")
			}
		}()
		ctxt, _ := context.WithTimeout(context.Background(), 50*time.Millisecond)

		res, err := req.GetResponse(ctxt)
		assert.NoError(t, err)
		require.NotNil(t, res)

		require.Equal(t, 2, len(res.Data))

		element := res.Data[0]
		assert.Equal(t, key1.Namespace, element.Namespace)
		assert.Equal(t, key1.Collection, element.Collection)
		assert.Equal(t, key1.Key, element.Key)
		assert.Equal(t, value1.Value, element.Value)

		element = res.Data[1]
		assert.Equal(t, key2.Namespace, element.Namespace)
		assert.Equal(t, key2.Collection, element.Collection)
		assert.Equal(t, key2.Key, element.Key)
		assert.Equal(t, value2.Value, element.Value)
	})

	t.Run("Non-Endorser -> no response", func(t *testing.T) {
		f := isEndorser
		defer func() { isEndorser = f }()
		isEndorser = func() bool { return false }

		req := reqMgr.NewRequest()
		ctxt, _ := context.WithTimeout(context.Background(), 50*time.Millisecond)

		res, err := req.GetResponse(ctxt)
		assert.EqualError(t, err, "context deadline exceeded")
		assert.Nil(t, res)
	})
}

func newCollDataReqMsg(channelID string, reqID uint64, keys ...*store.Key) *protoext.SignedGossipMessage {
	var digests []*gproto.CollDataDigest
	for _, key := range keys {
		digests = append(digests, &gproto.CollDataDigest{
			Namespace:      key.Namespace,
			Collection:     key.Collection,
			Key:            key.Key,
			EndorsedAtTxID: key.EndorsedAtTxID,
		})
	}

	msg, _ := protoext.NoopSign(&gproto.GossipMessage{
		Tag:     gproto.GossipMessage_CHAN_ONLY,
		Channel: []byte(channelID),
		Content: &gproto.GossipMessage_CollDataReq{
			CollDataReq: &gproto.RemoteCollDataRequest{
				Nonce:   reqID,
				Digests: digests,
			},
		},
	})
	return msg
}

type keyVal struct {
	*store.Key
	*store.ExpiringValue
}

func newKeyValue(key *store.Key, value *store.ExpiringValue) *keyVal {
	return &keyVal{
		Key:           key,
		ExpiringValue: value,
	}
}

func newTransientDataResMsg(channelID string, reqID uint64, keyVals ...*keyVal) *protoext.SignedGossipMessage {
	var elements []*gproto.CollDataElement
	for _, kv := range keyVals {
		elements = append(elements, &gproto.CollDataElement{
			Digest: &gproto.CollDataDigest{
				Namespace:      kv.Namespace,
				Collection:     kv.Collection,
				Key:            kv.Key.Key,
				EndorsedAtTxID: kv.EndorsedAtTxID,
			},
			Value:      kv.Value,
			ExpiryTime: common.ToTimestamp(kv.Expiry),
		})
	}

	msg, _ := protoext.NoopSign(&gproto.GossipMessage{
		Tag:     gproto.GossipMessage_CHAN_ONLY,
		Channel: []byte(channelID),
		Content: &gproto.GossipMessage_CollDataRes{
			CollDataRes: &gproto.RemoteCollDataResponse{
				Nonce:    reqID,
				Elements: elements,
			},
		},
	})
	return msg
}

func newDataMsg(channelID string) *protoext.SignedGossipMessage {
	msg, _ := protoext.NoopSign(&gproto.GossipMessage{
		Tag:     gproto.GossipMessage_CHAN_ONLY,
		Channel: []byte(channelID),
		Content: &gproto.GossipMessage_DataMsg{},
	})
	return msg
}

type mockReceivedMessage struct {
	message   *protoext.SignedGossipMessage
	respondTo func(msg *gproto.GossipMessage)
	member    discovery.NetworkMember
}

func (m *mockReceivedMessage) Respond(msg *gproto.GossipMessage) {
	if m.respondTo != nil {
		m.respondTo(msg)
	}
}

func (m *mockReceivedMessage) GetGossipMessage() *protoext.SignedGossipMessage {
	return m.message
}

func (m *mockReceivedMessage) GetSourceEnvelope() *gproto.Envelope {
	return nil
}

func (m *mockReceivedMessage) GetConnectionInfo() *protoext.ConnectionInfo {
	return &protoext.ConnectionInfo{
		ID:       m.member.PKIid,
		Endpoint: m.member.Endpoint,
	}
}

func (m *mockReceivedMessage) Ack(err error) {
}
