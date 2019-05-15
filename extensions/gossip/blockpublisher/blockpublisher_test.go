/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package blockpublisher

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/extensions/mocks"
	cb "github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/ledger/rwset/kvrwset"
	pb "github.com/hyperledger/fabric/protos/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	channelID = "testchannel"

	txID1 = "tx1"
	txID2 = "tx2"
	txID3 = "tx3"

	ccID1 = "cc1"
	ccID2 = "cc2"

	coll1 = "collection1"
	coll2 = "collection2"

	key1 = "key1"
	key2 = "key2"
	key3 = "key3"

	ccEvent1 = "ccevent1"

	hName1 = "handler1"
	hName2 = "handler2"
	hName3 = "handler3"
)

func TestPublisher_Get(t *testing.T) {
	p := New("mychannel")
	require.NotNil(t, p)
	p.Close()
}

func TestPublisher_Close(t *testing.T) {
	p := New(channelID)
	require.NotNil(t, p)

	p.Close()

	assert.NotPanics(t, func() {
		p.Close()
	}, "Expecting Close to not panic when called multiple times")
}

func TestPublisher_PublishEndorsementEvents(t *testing.T) {
	var (
		value1 = []byte("value1")
		value2 = []byte("value2")
		value3 = []byte("value3")

		v1 = &kvrwset.Version{
			BlockNum: 1000,
			TxNum:    0,
		}
		v2 = &kvrwset.Version{
			BlockNum: 1001,
			TxNum:    1,
		}
	)

	p := New(channelID)
	require.NotNil(t, p)
	defer p.Close()

	handler1 := newMockBlockHandler(hName1)
	p.AddReadHandler(handler1.HandleRead)
	p.AddWriteHandler(handler1.HandleWrite)

	handler2 := newMockBlockHandler(hName2)
	p.AddReadHandler(handler2.HandleRead)
	p.AddCCEventHandler(handler2.HandleChaincodeEvent)

	handler3 := newMockBlockHandler(hName3)
	p.AddCCUpgradeHandler(handler3.HandleChaincodeUpgradeEvent)

	b := mocks.NewBlockBuilder(channelID, 1100)

	tb1 := b.Transaction(txID1, pb.TxValidationCode_VALID)
	tb1.ChaincodeAction(ccID1).
		Write(key1, value1).
		Read(key1, v1).
		ChaincodeEvent(ccEvent1, []byte("ccpayload"))
	tb1.ChaincodeAction(ccID2).
		Write(key2, value2).
		Read(key2, v2)

	tb2 := b.Transaction(txID2, pb.TxValidationCode_VALID)
	cc2_1 := tb2.ChaincodeAction(ccID1).
		Write(key2, value2)
	cc2_1.Collection(coll1).
		Write(key1, value2)
	cc2_1.Collection(coll2).
		Delete(key1)

	// This transaction should not be published
	tb3 := b.Transaction(txID3, pb.TxValidationCode_MVCC_READ_CONFLICT)
	tb3.ChaincodeAction(ccID1).
		Write(key3, value3).
		ChaincodeEvent(ccEvent1, []byte("ccpayload"))

	lceBytes, err := proto.Marshal(&pb.LifecycleEvent{ChaincodeName: ccID2})
	require.NoError(t, err)
	require.NotNil(t, lceBytes)

	b.Transaction(txID1, pb.TxValidationCode_VALID).
		ChaincodeAction(lsccID).
		ChaincodeEvent(upgradeEvent, lceBytes)
	tb4 := b.Transaction(txID2, pb.TxValidationCode_VALID)
	tb4.ChaincodeAction(lsccID).
		ChaincodeEvent(ccEvent1, nil)

	p.Publish(b.Build())

	time.Sleep(time.Second)

	assert.Equal(t, 2, handler1.NumReads())
	assert.Equal(t, 5, handler1.NumWrites())
	assert.Equal(t, 0, handler1.NumCCEvents())
	assert.Equal(t, 0, handler1.NumCCUpgradeEvents())

	assert.Equal(t, 2, handler2.NumReads())
	assert.Equal(t, 0, handler2.NumWrites())
	assert.Equal(t, 3, handler2.NumCCEvents())
	assert.Equal(t, 0, handler2.NumCCUpgradeEvents())

	assert.Equal(t, 0, handler3.NumReads())
	assert.Equal(t, 0, handler3.NumWrites())
	assert.Equal(t, 0, handler3.NumCCEvents())
	assert.Equal(t, 1, handler3.NumCCUpgradeEvents())
}

func TestPublisher_PublishConfigUpdateEvents(t *testing.T) {
	p := New(channelID)
	require.NotNil(t, p)
	defer p.Close()

	handler := newMockBlockHandler(hName1)
	p.AddConfigUpdateHandler(handler.HandleConfigUpdate)

	b := mocks.NewBlockBuilder(channelID, 1100)
	b.ConfigUpdate()

	p.Publish(b.Build())

	time.Sleep(time.Second)

	assert.Equal(t, 1, handler.NumConfigUpdates())
}

func TestPublisher_Error(t *testing.T) {
	var (
		value1 = []byte("value1")
		value2 = []byte("value2")

		v1 = &kvrwset.Version{
			BlockNum: 1000,
			TxNum:    3,
		}
		v2 = &kvrwset.Version{
			BlockNum: 1001,
			TxNum:    5,
		}
	)

	p := New(channelID)
	require.NotNil(t, p)
	defer p.Close()

	expectedErr := fmt.Errorf("injected error")

	handler1 := newMockBlockHandler(hName1).WithError(expectedErr)
	p.AddReadHandler(handler1.HandleRead)
	p.AddWriteHandler(handler1.HandleWrite)
	p.AddCCEventHandler(handler1.HandleChaincodeEvent)
	p.AddCCUpgradeHandler(handler1.HandleChaincodeUpgradeEvent)

	b := mocks.NewBlockBuilder(channelID, 1100)

	tb1 := b.Transaction(txID1, pb.TxValidationCode_VALID)
	tb1.ChaincodeAction(ccID1).
		Write(key1, value1).
		Read(key1, v1).
		ChaincodeEvent(ccEvent1, []byte("ccpayload"))
	tb1.ChaincodeAction(ccID2).
		Write(key2, value2).
		Read(key2, v2)

	tb2 := b.Transaction(txID2, pb.TxValidationCode_VALID)
	cc2_1 := tb2.ChaincodeAction(ccID1).
		Write(key2, value2)
	cc2_1.Collection(coll1).
		Write(key1, value2)
	cc2_1.Collection(coll2).
		Delete(key1)

	lceBytes, err := proto.Marshal(&pb.LifecycleEvent{ChaincodeName: ccID2})
	require.NoError(t, err)
	require.NotNil(t, lceBytes)

	b.Transaction(txID1, pb.TxValidationCode_VALID).
		ChaincodeAction(lsccID).
		ChaincodeEvent(upgradeEvent, lceBytes)
	tb4 := b.Transaction(txID2, pb.TxValidationCode_VALID)
	tb4.ChaincodeAction(lsccID).
		ChaincodeEvent(ccEvent1, nil)

	p.Publish(b.Build())

	time.Sleep(time.Second)

	assert.Equal(t, 2, handler1.NumReads())
	assert.Equal(t, 5, handler1.NumWrites())
	assert.Equal(t, 3, handler1.NumCCEvents())
	assert.Equal(t, 1, handler1.NumCCUpgradeEvents())
}

type mockBlockHandler struct {
	name               string
	numReads           int32
	numWrites          int32
	numCCEvents        int32
	numCCUpgradeEvents int32
	numConfigUpdates   int32
	err                error
}

func newMockBlockHandler(name string) *mockBlockHandler {
	return &mockBlockHandler{
		name: name,
	}
}

func (m *mockBlockHandler) WithError(err error) *mockBlockHandler {
	m.err = err
	return m
}

func (m *mockBlockHandler) Name() string {
	return m.name
}

func (m *mockBlockHandler) NumReads() int {
	return int(atomic.LoadInt32(&m.numReads))
}

func (m *mockBlockHandler) NumWrites() int {
	return int(atomic.LoadInt32(&m.numWrites))
}

func (m *mockBlockHandler) NumCCEvents() int {
	return int(atomic.LoadInt32(&m.numCCEvents))
}

func (m *mockBlockHandler) NumCCUpgradeEvents() int {
	return int(atomic.LoadInt32(&m.numCCUpgradeEvents))
}

func (m *mockBlockHandler) NumConfigUpdates() int {
	return int(atomic.LoadInt32(&m.numConfigUpdates))
}

func (m *mockBlockHandler) HandleRead(blockNum uint64, txID string, namespace string, kvRead *kvrwset.KVRead) error {
	atomic.AddInt32(&m.numReads, 1)
	fmt.Printf("[%s] Got read in block [%d] at Tx [%s] for [%s]: %s\n", m.name, blockNum, txID, namespace, kvRead)
	return m.err
}

func (m *mockBlockHandler) HandleWrite(blockNum uint64, txID string, namespace string, kvWrite *kvrwset.KVWrite) error {
	atomic.AddInt32(&m.numWrites, 1)
	fmt.Printf("[%s] Got write in block [%d] at Tx [%s] for [%s]: %s\n", m.name, blockNum, txID, namespace, kvWrite)
	return m.err
}

func (m *mockBlockHandler) HandleChaincodeEvent(blockNum uint64, txID string, event *pb.ChaincodeEvent) error {
	atomic.AddInt32(&m.numCCEvents, 1)
	fmt.Printf("[%s] Got CC event in block [%d] at Tx [%s]: %s\n", m.name, blockNum, txID, event)
	return m.err
}

func (m *mockBlockHandler) HandleChaincodeUpgradeEvent(blockNum uint64, txID string, chaincodeName string) error {
	atomic.AddInt32(&m.numCCUpgradeEvents, 1)
	fmt.Printf("[%s] Got CC upgrade event in block [%d] at Tx [%s] for CC [%s]\n", m.name, blockNum, txID, chaincodeName)
	return m.err
}

func (m *mockBlockHandler) HandleConfigUpdate(blockNum uint64, configUpdate *cb.ConfigUpdate) error {
	atomic.AddInt32(&m.numConfigUpdates, 1)
	fmt.Printf("[%s] Got config update in block [%d]: %s\n", m.name, blockNum, configUpdate)
	return m.err
}
