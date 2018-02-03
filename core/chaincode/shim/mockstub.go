/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

// Package shim provides APIs for the chaincode to access its state
// variables, transaction context and call other chaincodes.
package shim

import (
	"container/list"
	"fmt"
	"strings"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/protos/ledger/queryresult"
	pb "github.com/hyperledger/fabric/protos/peer"
	"github.com/op/go-logging"
	"github.com/pkg/errors"
)

// Logger for the shim package.
var mockLogger = logging.MustGetLogger("mock")

// MockStub is an implementation of ChaincodeStubInterface for unit testing chaincode.
// Use this instead of ChaincodeStub in your chaincode's unit test calls to Init or Invoke.
type MockStub struct {
	// arguments the stub was called with
	args [][]byte

	// A pointer back to the chaincode that will invoke this, set by constructor.
	// If a peer calls this stub, the chaincode will be invoked from here.
	cc Chaincode

	// A nice name that can be used for logging
	Name string

	// State keeps name value pairs
	State map[string][]byte

	// Keys stores the list of mapped values in lexical order
	Keys *list.List

	// privateData keeps name value pairs for each collection
	privateData map[string]map[string][]byte

	// privateKeys stores the list of mapped values in lexical order (per collection)
	privateKeys map[string]*list.List

	// registered list of other MockStub chaincodes that can be called from this MockStub
	Invokables map[string]*MockStub

	// stores a transaction uuid while being Invoked / Deployed
	// TODO if a chaincode uses recursion this may need to be a stack of TxIDs or possibly a reference counting map
	TxID string

	TxTimestamp *timestamp.Timestamp

	// mocked signedProposal
	signedProposal *pb.SignedProposal

	// stores a channel ID of the proposal
	ChannelID string
}

// GetTxID returns the tx_id of the transaction proposal (see ChannelHeader
// in protos/common/common.proto)
func (stub *MockStub) GetTxID() string {
	return stub.TxID
}

func (stub *MockStub) GetChannelID() string {
	return stub.ChannelID
}

// GetArgs returns the arguments intended for the chaincode Init and Invoke
// as an array of byte arrays.
func (stub *MockStub) GetArgs() [][]byte {
	return stub.args
}

// GetStringArgs returns the arguments intended for the chaincode Init and
// Invoke as a string array. Only use GetStringArgs if the client passes
// arguments intended to be used as strings.
func (stub *MockStub) GetStringArgs() []string {
	args := stub.GetArgs()
	strargs := make([]string, 0, len(args))
	for _, barg := range args {
		strargs = append(strargs, string(barg))
	}
	return strargs
}

// GetFunctionAndParameters returns the first argument as the function
// name and the rest of the arguments as parameters in a string array.
// Only use GetFunctionAndParameters if the client passes arguments intended
// to be used as strings.
func (stub *MockStub) GetFunctionAndParameters() (function string, params []string) {
	allargs := stub.GetStringArgs()
	function = ""
	params = []string{}
	if len(allargs) >= 1 {
		function = allargs[0]
		params = allargs[1:]
	}
	return
}

// MockTransactionStart is used to indicate to a chaincode that it is part of a transaction.
// This is important when chaincodes invoke each other.
// MockStub doesn't support concurrent transactions at present.
func (stub *MockStub) MockTransactionStart(txid string) {
	stub.TxID = txid
	stub.setSignedProposal(&pb.SignedProposal{})
	stub.setTxTimestamp(util.CreateUtcTimestamp())
}

// MockTransactionEnd ends a mocked transaction, clearing the UUID.
func (stub *MockStub) MockTransactionEnd(uuid string) {
	stub.signedProposal = nil
	stub.TxID = ""
}

// MockPeerChaincode registers a peer chaincode with this MockStub
// invokableChaincodeName is the name or hash of the peer
// otherStub is a MockStub of the peer, already intialised
func (stub *MockStub) MockPeerChaincode(invokableChaincodeName string, otherStub *MockStub) {
	stub.Invokables[invokableChaincodeName] = otherStub
}

// MockInit initialises this chaincode,  also starts and ends a transaction.
func (stub *MockStub) MockInit(uuid string, args [][]byte) pb.Response {
	stub.args = args
	stub.MockTransactionStart(uuid)
	res := stub.cc.Init(stub)
	stub.MockTransactionEnd(uuid)
	return res
}

// MockInvoke invokes this chaincode, also starts and ends a transaction.
func (stub *MockStub) MockInvoke(uuid string, args [][]byte) pb.Response {
	stub.args = args
	stub.MockTransactionStart(uuid)
	res := stub.cc.Invoke(stub)
	stub.MockTransactionEnd(uuid)
	return res
}

// GetDecorations returns additional data (if applicable)
// about the proposal that originated from the peer
func (stub *MockStub) GetDecorations() map[string][]byte {
	return nil
}

// MockInvokeWithSignedProposal invokes this chaincode, also starts and ends a transaction.
func (stub *MockStub) MockInvokeWithSignedProposal(uuid string, args [][]byte, sp *pb.SignedProposal) pb.Response {
	stub.args = args
	stub.MockTransactionStart(uuid)
	stub.signedProposal = sp
	res := stub.cc.Invoke(stub)
	stub.MockTransactionEnd(uuid)
	return res
}

// GetPrivateData returns the value of the specified `key` from the specified
// `collection`.
func (stub *MockStub) GetPrivateData(collection string, key string) ([]byte, error) {
	value := stub.PrivateData(collection)[key]
	mockLogger.Debug("MockStub", stub.Name, "Getting", collection, key, value)
	return value, nil
}

// PutPrivateData puts the specified `key` and `value` into the specified `collection`.
func (stub *MockStub) PutPrivateData(collection string, key string, value []byte) error {
	return stub.putState(stub.PrivateData(collection), stub.PrivateKeys(collection), key, value)
}

// DelPrivateData deletes the specified `key` from the specified `collection`.
func (stub *MockStub) DelPrivateData(collection string, key string) error {
	state := stub.PrivateData(collection)
	mockLogger.Debug("MockStub", stub.Name, "Deleting", collection, key, state[key])
	return stub.delState(state, stub.PrivateKeys(collection), key)
}

// GetPrivateDataByRange returns a range iterator over a set of keys in a
// given private collection. The iterator can be used to iterate over all keys
// between the startKey (inclusive) and endKey (exclusive).
// The keys are returned by the iterator in lexical order. Note
// that startKey and endKey can be empty string, which implies unbounded range
// query on start or end.
// Call Close() on the returned StateQueryIteratorInterface object when done.
// The query is re-executed during validation phase to ensure result set
// has not changed since transaction endorsement (phantom reads detected).
func (stub *MockStub) GetPrivateDataByRange(collection, startKey, endKey string) (StateQueryIteratorInterface, error) {
	if err := validateSimpleKeys(startKey, endKey); err != nil {
		return nil, err
	}
	return NewMockPrivateStateRangeQueryIterator(stub, collection, startKey, endKey), nil
}

// GetPrivateDataByPartialCompositeKey queries the state in a given private
// collection based on a given partial composite key. This function returns
// an iterator which can be used to iterate over all composite keys whose prefix
// matches the given partial composite key. The `objectType` and attributes are
// expected to have only valid utf8 strings and should not contain
// U+0000 (nil byte) and U+10FFFF (biggest and unallocated code point).
// See related functions SplitCompositeKey and CreateCompositeKey.
// Call Close() on the returned StateQueryIteratorInterface object when done.
// The query is re-executed during validation phase to ensure result set
// has not changed since transaction endorsement (phantom reads detected).
func (stub *MockStub) GetPrivateDataByPartialCompositeKey(collection, objectType string, attributes []string) (StateQueryIteratorInterface, error) {
	partialCompositeKey, err := stub.CreateCompositeKey(objectType, attributes)
	if err != nil {
		return nil, err
	}
	return NewMockPrivateStateRangeQueryIterator(stub, collection, partialCompositeKey, partialCompositeKey+string(maxUnicodeRuneValue)), nil
}

// GetPrivateDataQueryResult performs a "rich" query against a given private
// collection. It is only supported for state databases that support rich query,
// e.g.CouchDB. The query string is in the native syntax
// of the underlying state database. An iterator is returned
// which can be used to iterate (next) over the query result set.
// The query is NOT re-executed during validation phase, phantom reads are
// not detected. That is, other committed transactions may have added,
// updated, or removed keys that impact the result set, and this would not
// be detected at validation/commit time.  Applications susceptible to this
// should therefore not use GetQueryResult as part of transactions that update
// ledger, and should limit use to read-only chaincode operations.
func (stub *MockStub) GetPrivateDataQueryResult(collection, query string) (StateQueryIteratorInterface, error) {
	// Not implemented since the mock engine does not have a query engine.
	// However, a very simple query engine that supports string matching
	// could be implemented to test that the framework supports queries
	return nil, errors.New("Not Implemented")
}

// GetState retrieves the value for a given key from the ledger
func (stub *MockStub) GetState(key string) ([]byte, error) {
	value := stub.State[key]
	mockLogger.Debug("MockStub", stub.Name, "Getting", key, value)
	return value, nil
}

// PutState writes the specified `value` and `key` into the ledger.
func (stub *MockStub) PutState(key string, value []byte) error {
	return stub.putState(stub.State, stub.Keys, key, value)
}

// DelState removes the specified `key` and its value from the ledger.
func (stub *MockStub) DelState(key string) error {
	mockLogger.Debug("MockStub", stub.Name, "Deleting", key, stub.State[key])
	return stub.delState(stub.State, stub.Keys, key)
}

// GetStateByRange returns a range iterator over a set of keys in the
// ledger. The iterator can be used to iterate over all keys
// between the startKey (inclusive) and endKey (exclusive).
// The keys are returned by the iterator in lexical order. Note
// that startKey and endKey can be empty string, which implies unbounded range
// query on start or end.
// Call Close() on the returned StateQueryIteratorInterface object when done.
// The query is re-executed during validation phase to ensure result set
// has not changed since transaction endorsement (phantom reads detected).
func (stub *MockStub) GetStateByRange(startKey, endKey string) (StateQueryIteratorInterface, error) {
	if err := validateSimpleKeys(startKey, endKey); err != nil {
		return nil, err
	}
	return NewMockStateRangeQueryIterator(stub, startKey, endKey), nil
}

// GetQueryResult function can be invoked by a chaincode to perform a
// rich query against state database.  Only supported by state database implementations
// that support rich query.  The query string is in the syntax of the underlying
// state database. An iterator is returned which can be used to iterate (next) over
// the query result set
func (stub *MockStub) GetQueryResult(query string) (StateQueryIteratorInterface, error) {
	// Not implemented since the mock engine does not have a query engine.
	// However, a very simple query engine that supports string matching
	// could be implemented to test that the framework supports queries
	return nil, errors.New("not implemented")
}

// GetHistoryForKey function can be invoked by a chaincode to return a history of
// key values across time. GetHistoryForKey is intended to be used for read-only queries.
func (stub *MockStub) GetHistoryForKey(key string) (HistoryQueryIteratorInterface, error) {
	return nil, errors.New("not implemented")
}

//GetStateByPartialCompositeKey function can be invoked by a chaincode to query the
//state based on a given partial composite key. This function returns an
//iterator which can be used to iterate over all composite keys whose prefix
//matches the given partial composite key. This function should be used only for
//a partial composite key. For a full composite key, an iter with empty response
//would be returned.
func (stub *MockStub) GetStateByPartialCompositeKey(objectType string, attributes []string) (StateQueryIteratorInterface, error) {
	partialCompositeKey, err := stub.CreateCompositeKey(objectType, attributes)
	if err != nil {
		return nil, err
	}
	return NewMockStateRangeQueryIterator(stub, partialCompositeKey, partialCompositeKey+string(maxUnicodeRuneValue)), nil
}

// CreateCompositeKey combines the list of attributes
//to form a composite key.
func (stub *MockStub) CreateCompositeKey(objectType string, attributes []string) (string, error) {
	return createCompositeKey(objectType, attributes)
}

// SplitCompositeKey splits the composite key into attributes
// on which the composite key was formed.
func (stub *MockStub) SplitCompositeKey(compositeKey string) (string, []string, error) {
	return splitCompositeKey(compositeKey)
}

// InvokeChaincode calls a peered chaincode.
// E.g. stub1.InvokeChaincode("stub2Hash", funcArgs, channel)
// Before calling this make sure to create another MockStub stub2, call stub2.MockInit(uuid, func, args)
// and register it with stub1 by calling stub1.MockPeerChaincode("stub2Hash", stub2)
func (stub *MockStub) InvokeChaincode(chaincodeName string, args [][]byte, channel string) pb.Response {
	// Internally we use chaincode name as a composite name
	if channel != "" {
		chaincodeName = chaincodeName + "/" + channel
	}
	// TODO "args" here should possibly be a serialized pb.ChaincodeInput
	otherStub := stub.Invokables[chaincodeName]
	mockLogger.Debug("MockStub", stub.Name, "Invoking peer chaincode", otherStub.Name, args)
	//	function, strings := getFuncArgs(args)
	res := otherStub.MockInvoke(stub.TxID, args)
	mockLogger.Debug("MockStub", stub.Name, "Invoked peer chaincode", otherStub.Name, "got", fmt.Sprintf("%+v", res))
	return res
}

// GetCreator Not implemented
func (stub *MockStub) GetCreator() ([]byte, error) {
	return nil, nil
}

// GetTransient Not implemented
func (stub *MockStub) GetTransient() (map[string][]byte, error) {
	return nil, nil
}

// GetBinding Not implemented
func (stub *MockStub) GetBinding() ([]byte, error) {
	return nil, nil
}

// GetSignedProposal Not implemented
func (stub *MockStub) GetSignedProposal() (*pb.SignedProposal, error) {
	return stub.signedProposal, nil
}

func (stub *MockStub) setSignedProposal(sp *pb.SignedProposal) {
	stub.signedProposal = sp
}

// GetArgsSlice Not implemented
func (stub *MockStub) GetArgsSlice() ([]byte, error) {
	return nil, nil
}

func (stub *MockStub) setTxTimestamp(time *timestamp.Timestamp) {
	stub.TxTimestamp = time
}

// GetTxTimestamp returns the timestamp when the transaction was created. This
// is taken from the transaction ChannelHeader, therefore it will indicate the
// client's timestamp, and will have the same value across all endorsers.
func (stub *MockStub) GetTxTimestamp() (*timestamp.Timestamp, error) {
	if stub.TxTimestamp == nil {
		return nil, errors.New("txTimestamp not set")
	}
	return stub.TxTimestamp, nil
}

// SetEvent Not implemented
func (stub *MockStub) SetEvent(name string, payload []byte) error {
	return nil
}

// NewMockStub is the constructor to initialise the internal State map
func NewMockStub(name string, cc Chaincode) *MockStub {
	mockLogger.Debug("MockStub(", name, cc, ")")
	s := new(MockStub)
	s.Name = name
	s.cc = cc
	s.State = make(map[string][]byte)
	s.Invokables = make(map[string]*MockStub)
	s.Keys = list.New()
	s.privateData = make(map[string]map[string][]byte)
	s.privateKeys = make(map[string]*list.List)

	return s
}

/*****************************
 Range Query Iterator
*****************************/

// MockStateRangeQueryIterator is a mock implementation of the range query iterator
type MockStateRangeQueryIterator struct {
	Closed   bool
	StartKey string
	EndKey   string
	Keys     *list.List
	Current  *list.Element
	State    map[string][]byte
}

// HasNext returns true if the range query iterator contains additional keys
// and values.
func (iter *MockStateRangeQueryIterator) HasNext() bool {
	if iter.Closed {
		// previously called Close()
		mockLogger.Error("HasNext() but already closed")
		return false
	}

	if iter.Current == nil {
		mockLogger.Error("HasNext() couldn't get Current")
		return false
	}

	current := iter.Current
	for current != nil {
		// if this is an open-ended query for all keys, return true
		if iter.StartKey == "" && iter.EndKey == "" {
			return true
		}
		comp1 := strings.Compare(current.Value.(string), iter.StartKey)
		comp2 := strings.Compare(current.Value.(string), iter.EndKey)
		if comp1 >= 0 {
			if comp2 <= 0 {
				mockLogger.Debug("HasNext() got next")
				return true
			}
			mockLogger.Debug("HasNext() but no next")
			return false
		}
		current = current.Next()
	}

	// we've reached the end of the underlying values
	mockLogger.Debug("HasNext() but no next")
	return false
}

// Next returns the next key and value in the range query iterator.
func (iter *MockStateRangeQueryIterator) Next() (*queryresult.KV, error) {
	if iter.Closed == true {
		err := errors.New("MockStateRangeQueryIterator.Next() called after Close()")
		mockLogger.Errorf("%+v", err)
		return nil, err
	}

	if iter.HasNext() == false {
		err := errors.New("MockStateRangeQueryIterator.Next() called when it does not HaveNext()")
		mockLogger.Errorf("%+v", err)
		return nil, err
	}

	for iter.Current != nil {
		comp1 := strings.Compare(iter.Current.Value.(string), iter.StartKey)
		comp2 := strings.Compare(iter.Current.Value.(string), iter.EndKey)
		// compare to start and end keys. or, if this is an open-ended query for
		// all keys, it should always return the key and value
		if (comp1 >= 0 && comp2 <= 0) || (iter.StartKey == "" && iter.EndKey == "") {
			key := iter.Current.Value.(string)
			value := iter.State[key]
			iter.Current = iter.Current.Next()
			return &queryresult.KV{Key: key, Value: value}, nil
		}
		iter.Current = iter.Current.Next()
	}
	err := errors.New("MockStateRangeQueryIterator.Next() went past end of range")
	mockLogger.Errorf("%+v", err)
	return nil, err
}

// Close closes the range query iterator. This should be called when done
// reading from the iterator to free up resources.
func (iter *MockStateRangeQueryIterator) Close() error {
	if iter.Closed == true {
		err := errors.New("MockStateRangeQueryIterator.Close() called after Close()")
		mockLogger.Errorf("%+v", err)
		return err
	}

	iter.Closed = true
	return nil
}

func (stub *MockStub) putState(state map[string][]byte, keys *list.List, key string, value []byte) error {
	if stub.TxID == "" {
		err := errors.New("cannot PutState without a transactions - call stub.MockTransactionStart()?")
		mockLogger.Errorf("%+v", err)
		return err
	}

	mockLogger.Debug("MockStub", stub.Name, "Putting", key, value)
	state[key] = value

	// insert key into ordered list of keys
	for elem := keys.Front(); elem != nil; elem = elem.Next() {
		elemValue := elem.Value.(string)
		comp := strings.Compare(key, elemValue)
		mockLogger.Debug("MockStub", stub.Name, "Compared", key, elemValue, " and got ", comp)
		if comp < 0 {
			// key < elem, insert it before elem
			keys.InsertBefore(key, elem)
			mockLogger.Debug("MockStub", stub.Name, "Key", key, " inserted before", elem.Value)
			break
		} else if comp == 0 {
			// keys exists, no need to change
			mockLogger.Debug("MockStub", stub.Name, "Key", key, "already in State")
			break
		} else { // comp > 0
			// key > elem, keep looking unless this is the end of the list
			if elem.Next() == nil {
				keys.PushBack(key)
				mockLogger.Debug("MockStub", stub.Name, "Key", key, "appended")
				break
			}
		}
	}

	// special case for empty Keys list
	if keys.Len() == 0 {
		keys.PushFront(key)
		mockLogger.Debug("MockStub", stub.Name, "Key", key, "is first element in list")
	}

	return nil
}

// Print prints out the variables of the MockStateRangeQueryIterator
func (iter *MockStateRangeQueryIterator) Print() {
	mockLogger.Debug("MockStateRangeQueryIterator {")
	mockLogger.Debug("Closed?", iter.Closed)
	mockLogger.Debug("StartKey", iter.StartKey)
	mockLogger.Debug("EndKey", iter.EndKey)
	mockLogger.Debug("Current", iter.Current)
	mockLogger.Debug("HasNext?", iter.HasNext())
	mockLogger.Debug("}")
}

// NewMockStateRangeQueryIterator returns a new MockStateRangeQueryIterator
func NewMockStateRangeQueryIterator(stub *MockStub, startKey, endKey string) *MockStateRangeQueryIterator {
	mockLogger.Debug("NewMockStateRangeQueryIterator(", stub, startKey, endKey, ")")
	return newMockStateRangeQueryIterator(stub, stub.State, stub.Keys, startKey, endKey)
}

// NewMockPrivateStateRangeQueryIterator returns a new MockStateRangeQueryIterator
func NewMockPrivateStateRangeQueryIterator(stub *MockStub, collection, startKey, endKey string) *MockStateRangeQueryIterator {
	mockLogger.Debug("NewMockPrivateStateRangeQueryIterator(", stub, collection, startKey, endKey, ")")
	return newMockStateRangeQueryIterator(stub, stub.PrivateData(collection), stub.PrivateKeys(collection), startKey, endKey)
}

func newMockStateRangeQueryIterator(stub *MockStub, state map[string][]byte, keys *list.List, startKey, endKey string) *MockStateRangeQueryIterator {
	iter := new(MockStateRangeQueryIterator)
	iter.Closed = false
	iter.StartKey = startKey
	iter.EndKey = endKey
	iter.Keys = keys
	iter.Current = keys.Front()
	iter.State = state

	iter.Print()

	return iter
}

func getBytes(function string, args []string) [][]byte {
	bytes := make([][]byte, 0, len(args)+1)
	bytes = append(bytes, []byte(function))
	for _, s := range args {
		bytes = append(bytes, []byte(s))
	}
	return bytes
}

func getFuncArgs(bytes [][]byte) (string, []string) {
	mockLogger.Debugf("getFuncArgs(%x)", bytes)
	function := string(bytes[0])
	args := make([]string, len(bytes)-1)
	for i := 1; i < len(bytes); i++ {
		mockLogger.Debugf("getFuncArgs - i:%x, len(bytes):%x", i, len(bytes))
		args[i-1] = string(bytes[i])
	}
	return function, args
}

// PrivateKeys returns the private keys for the given collection
func (stub *MockStub) PrivateKeys(collection string) *list.List {
	if _, ok := stub.privateKeys[collection]; !ok {
		stub.privateKeys[collection] = list.New()
	}
	return stub.privateKeys[collection]
}

func (stub *MockStub) delState(state map[string][]byte, keys *list.List, key string) error {
	delete(state, key)

	for elem := keys.Front(); elem != nil; elem = elem.Next() {
		if strings.Compare(key, elem.Value.(string)) == 0 {
			keys.Remove(elem)
		}
	}

	return nil
}

// PrivateData returns the private data key-value map for the given collection
func (stub *MockStub) PrivateData(collection string) map[string][]byte {
	stateMap, ok := stub.privateData[collection]
	if !ok {
		stateMap = make(map[string][]byte)
		stub.privateData[collection] = stateMap
	}
	return stateMap
}
