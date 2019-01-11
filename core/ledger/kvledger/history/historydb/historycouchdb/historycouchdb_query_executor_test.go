/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package historycouchdb

import (
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/hyperledger/fabric/common/ledger/blkstorage/mocks"
	"github.com/hyperledger/fabric/core/ledger/kvledger/history/historydb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/ledger/queryresult"
	"github.com/hyperledger/fabric/protos/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric/protos/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Base name for our test historyDB Couch database.
const historyDbName = "historydb"

// DBHandle.NewHistoryQueryExecutor() must return a history query executor
func TestDbHandleReturnsQueryExecutor(t *testing.T) {
	def, cleanup := startCouchDB()
	defer cleanup()
	provider, err := NewHistoryDBProvider(def)
	require.NoError(t, err)
	handle, err := provider.GetDBHandle(historyDbName)
	qe, err := handle.NewHistoryQueryExecutor(&mocks.MockBlockStore{})
	require.NoError(t, err)
	assert.NotNil(t, qe, "historycouchdb DBHandle failed to return the HistoryQueryExecutor")
}

// Iteration through the internal cache of results from CouchDB should work.
func TestIterateOverInternalResultsPage(t *testing.T) {
	sets := writeSets{writeSet{}, writeSet{}, writeSet{}}
	page := &resultsPage{
		resultsPos: 0,
		results:    sets,
	}
	for _, _ = range sets {
		assert.NotNil(t, page.Next(), "internal results page prematurely exhausted during iteration")
	}
	assert.Nil(t, page.Next(), "internal results page not exhausted after iterating through all expected elements")
}

// The ResultsIterator must iterate through all committed writesets (in this case, 3).
// For this test, we instantiate the results iterator directly instead of fetching it
// via DBHandle.NewHistoryQueryExecutor() in order to control the pageSize and make sure
// the internal, transparent pagination of CouchDB queries works.
func TestResultsIterator(t *testing.T) {
	const namespace = "namespace"
	const key = "key"
	const pageSize = 1
	def, cleanup := startCouchDB()
	defer cleanup()
	provider, err := NewHistoryDBProvider(def)
	require.NoError(t, err)
	handle, err := provider.GetDBHandle(historyDbName)
	require.NoError(t, err)
	values := []string{"value1", "value2", "value3"}
	blockNum := uint64(0)
	envelopes := make(map[uint64]map[uint64]*common.Envelope)
	for _, value := range values {
		blockNum++
		commitWriteTrxInNewBlock(handle, namespace, key, value, blockNum)
		envelopes[blockNum] = map[uint64]*common.Envelope{
			uint64(0): mocks.CreateEnvelope(
				newChannelHeader(),
				newTransaction(namespace, key, value),
			),
		}
	}
	iter := resultsIter{
		couchDB:    newCouchDbClient(def, couchdb.ConstructBlockchainDBName(historyDbName, dbNameSuffix)),
		namespace:  namespace,
		key:        key,
		pageNum:    0,
		pageSize:   pageSize,
		page:       &resultsPage{},
		blockStore: &mocks.MockBlockStore{EnvelopeByBlknumTxNum: envelopes},
	}
	for _, value := range values {
		next, err := iter.Next()
		require.NoError(t, err)
		require.NotNilf(t, next, "HistoryCouchDB results iterator prematurely exhausted while checking for value [%s]", value)
		keymod := next.(*queryresult.KeyModification)
		assert.Equal(t, []byte(value), keymod.Value, "HistoryCouchDB results iterator not returning correct values for key modifications")
	}
	next, err := iter.Next()
	require.NoError(t, err)
	assert.Nil(t, next, "HistoryCouchDB results iterator not exhausted after iterating through all expected elements")
}

// New mock channel header.
func newChannelHeader() *common.ChannelHeader {
	return &common.ChannelHeader{
		ChannelId: "",
		TxId:      "123",
		Type:      int32(common.HeaderType_ENDORSER_TRANSACTION),
		Timestamp: &timestamp.Timestamp{Seconds: time.Now().Unix()},
	}
}

// New mock transaction with a write-set that will modify the given namespace-key to the
// given value.
func newTransaction(namespace, key, value string) *peer.Transaction {
	return mocks.CreateTransaction(
		&rwsetutil.NsRwSet{
			NameSpace: namespace,
			KvRwSet: &kvrwset.KVRWSet{
				Writes: []*kvrwset.KVWrite{{Key: key, IsDelete: false, Value: []byte(value)}},
			},
		},
	)
}

// Creates a new mock block with the given blockNum and commits it to the historyDB.
// The new block will have a new mock transaction with a write-set that will modify the given namespace-key to the
// given value.
func commitWriteTrxInNewBlock(handle historydb.HistoryDB, namespace, key, value string, blockNum uint64) {
	err := handle.Commit(
		mocks.CreateBlock(
			blockNum,
			&mocks.Transaction{
				newChannelHeader(),
				peer.TxValidationCode_VALID,
				newTransaction(namespace, key, value),
			},
		),
	)
	panicIfErr(err)
}

func panicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}
