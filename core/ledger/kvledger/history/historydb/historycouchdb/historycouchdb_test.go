/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package historycouchdb

import (
	"fmt"
	"github.com/hyperledger/fabric/common/ledger/testutil"
	"github.com/hyperledger/fabric/core/ledger/kvledger/history/historydb/mocks"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/hyperledger/fabric/integration/runner"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric/protos/peer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// Can create a new HistoryDBProvider
func TestCanReturnProvider(t *testing.T) {
	def, cleanup := startCouchDB()
	defer cleanup()
	provider, err := NewHistoryDBProvider(def)
	testutil.AssertNoError(t, err, "cannot create historycouchdb provider")
	testutil.AssertNotNil(t, provider)
}

// Can get a DBHandle from the HistoryDBProvider
func TestProviderCanGetDbHandle(t *testing.T) {
	def, cleanup := startCouchDB()
	defer cleanup()
	provider, _ := NewHistoryDBProvider(def)
	handle, err := provider.GetDBHandle("historydb")
	testutil.AssertNoError(t, err, "cannot get DBHandle from historycouchdb provider")
	testutil.AssertNotNil(t, handle)
}

// DBHandle.Commit() only saves write sets to database for valid transactions that are endorsed.
func TestDbHandleCommitsWriteSet(t *testing.T) {
	const namespace = "test_namespace"
	const key = "test_key"
	const trxID = "12345"
	const blockNum = uint64(1)
	const dbname = "historydb"
	def, cleanup := startCouchDB()
	defer cleanup()
	provider, err := NewHistoryDBProvider(def)
	require.NoError(t, err)
	handle, err := provider.GetDBHandle(dbname)
	require.NoError(t, err)
	err = handle.Commit(
		mocks.NewBlock(
			blockNum,
			&common.ChannelHeader{
				ChannelId: "",
				TxId:      trxID,
				Type:      int32(common.HeaderType_ENDORSER_TRANSACTION),
			},
			[]peer.TxValidationCode{peer.TxValidationCode_VALID},
			mocks.NewTransaction(
				&rwsetutil.NsRwSet{
					NameSpace: namespace,
					KvRwSet: &kvrwset.KVRWSet{
						Writes: []*kvrwset.KVWrite{{Key: key, IsDelete: false, Value: []byte("some_value")}},
					},
				},
			),
		),
	)
	require.NoError(t, err)
	results, err := queryCouchDbById(
		def, dbname, formatKey(namespace, key, blockNum, uint64(0)),
	)
	require.NoErrorf(t, err, "failed to query test CouchDB")
	assert.NotEmpty(t, results, "write-sets must be saved to history couchdb")
}

// DBHandle.Commit() saves the block height document as a save point.
func TestDbHandleCommitsBlockHeightAsSavePoint(t *testing.T) {
	const namespace = "test_namespace"
	const key = "test_key"
	const trxID = "12345"
	const blockNum = uint64(1)
	const dbname = "historydb"
	def, cleanup := startCouchDB()
	defer cleanup()
	provider, err := NewHistoryDBProvider(def)
	require.NoError(t, err)
	handle, err := provider.GetDBHandle(dbname)
	require.NoError(t, err)
	err = handle.Commit(
		mocks.NewBlock(
			blockNum,
			&common.ChannelHeader{
				ChannelId: "",
				TxId:      trxID,
				Type:      int32(common.HeaderType_ENDORSER_TRANSACTION),
			},
			[]peer.TxValidationCode{peer.TxValidationCode_VALID},
			mocks.NewTransaction(
				&rwsetutil.NsRwSet{
					NameSpace: namespace,
					KvRwSet: &kvrwset.KVRWSet{
						Writes: []*kvrwset.KVWrite{{Key: key, IsDelete: false, Value: []byte("some_value")}},
					},
				},
			),
		),
	)
	require.NoError(t, err)
	results, err := queryCouchDbById(def, dbname, heightDocKey)
	require.NoErrorf(t, err, "failed to query test CouchDB")
	assert.NotEmpty(t, results, "block height must be saved to history couchdb")
}

// DBHandle.Commit() must also save the block height document even if there are no write-sets.
func TestDbHandleCommitsBlockHeightAsSavePointWhenNoWriteWriteSet(t *testing.T) {
	const namespace = "test_namespace"
	const key = "test_key"
	const trxID = "12345"
	const blockNum = uint64(1)
	const dbname = "historydb"
	def, cleanup := startCouchDB()
	defer cleanup()
	provider, err := NewHistoryDBProvider(def)
	require.NoError(t, err)
	handle, err := provider.GetDBHandle(dbname)
	require.NoError(t, err)
	err = handle.Commit(
		mocks.NewBlock(
			blockNum,
			&common.ChannelHeader{
				ChannelId: "",
				TxId:      trxID,
				Type:      int32(common.HeaderType_ENDORSER_TRANSACTION),
			},
			[]peer.TxValidationCode{peer.TxValidationCode_VALID},
			mocks.NewTransaction(
				&rwsetutil.NsRwSet{
					NameSpace: namespace,
					KvRwSet: &kvrwset.KVRWSet{
						Reads: []*kvrwset.KVRead{{Key: key, Version: &kvrwset.Version{BlockNum: blockNum, TxNum: uint64(5)}}},
					},
				},
			),
		),
	)
	require.NoError(t, err)
	results, err := queryCouchDbById(def, dbname, "height")
	require.NoErrorf(t, err, "failed to query test CouchDB")
	assert.NotEmpty(t, results, "block height must be saved to history couchdb")
}

// DbHandle.Commit() must not save write-sets that are not endorsements
func TestDbHandleDoesNotCommitNonEndorsementWriteSet(t *testing.T) {
	const namespace = "test_namespace"
	const key = "test_key"
	const trxID = "12345"
	const blockNum = uint64(1)
	const dbname = "historydb"
	const headerType = common.HeaderType_CONFIG_UPDATE
	require.NotEqual(t, headerType, common.HeaderType_ENDORSER_TRANSACTION)
	def, cleanup := startCouchDB()
	defer cleanup()
	provider, err := NewHistoryDBProvider(def)
	require.NoError(t, err)
	handle, err := provider.GetDBHandle(dbname)
	require.NoError(t, err)
	err = handle.Commit(
		mocks.NewBlock(
			blockNum,
			&common.ChannelHeader{
				ChannelId: "",
				TxId:      trxID,
				Type:      int32(headerType),
			},
			[]peer.TxValidationCode{peer.TxValidationCode_VALID},
			mocks.NewTransaction(
				&rwsetutil.NsRwSet{
					NameSpace: namespace,
					KvRwSet: &kvrwset.KVRWSet{
						Writes: []*kvrwset.KVWrite{{Key: key, IsDelete: false, Value: []byte("some_value")}},
					},
				},
			),
		),
	)
	require.NoError(t, err)
	results, err := queryCouchDbById(
		def, dbname, formatKey(namespace, key, blockNum, uint64(0)),
	)
	require.NoErrorf(t, err, "failed to query test CouchDB")
	assert.Empty(t, results, "non-endorsement transactions must not be saved to history couchdb")
}

// DbHandle.Commit() must not save write-sets for transactions that don't have the peer.TxValidationCode_VALID flag set.
func TestDbHandleDoesNotCommitInvalidWriteSet(t *testing.T) {
	const namespace = "test_namespace"
	const key = "test_key"
	const trxID = "12345"
	const blockNum = uint64(1)
	const dbname = "historydb"
	const txValidationCode = peer.TxValidationCode_BAD_CHANNEL_HEADER
	require.NotEqual(t, txValidationCode, peer.TxValidationCode_VALID)
	def, cleanup := startCouchDB()
	defer cleanup()
	provider, err := NewHistoryDBProvider(def)
	require.NoError(t, err)
	handle, err := provider.GetDBHandle(dbname)
	require.NoError(t, err)
	err = handle.Commit(
		mocks.NewBlock(
			blockNum,
			&common.ChannelHeader{
				ChannelId: "",
				TxId:      trxID,
				Type:      int32(common.HeaderType_ENDORSER_TRANSACTION),
			},
			[]peer.TxValidationCode{txValidationCode},
			mocks.NewTransaction(
				&rwsetutil.NsRwSet{
					NameSpace: namespace,
					KvRwSet: &kvrwset.KVRWSet{
						Writes: []*kvrwset.KVWrite{{Key: key, IsDelete: false, Value: []byte("some_value")}},
					},
				},
			),
		),
	)
	require.NoError(t, err)
	results, err := queryCouchDbById(
		def, dbname, formatKey(namespace, key, blockNum, uint64(0)),
	)
	require.NoErrorf(t, err, "failed to query test CouchDB")
	assert.Empty(t, results, "transactions that don't have peer.TxValidationCode_VALID set must not be saved to history couchdb")
}

// DbHandle.Commit() does not save read-sets.
func TestDbHandleDoesNotCommitReadSet(t *testing.T) {
	const namespace = "test_namespace"
	const key = "test_key"
	const trxID = "12345"
	const blockNum = uint64(1)
	const trxNum = uint64(0)
	const dbname = "historydb"
	def, cleanup := startCouchDB()
	defer cleanup()
	provider, err := NewHistoryDBProvider(def)
	require.NoError(t, err)
	handle, err := provider.GetDBHandle(dbname)
	require.NoError(t, err)
	err = handle.Commit(
		mocks.NewBlock(
			blockNum,
			&common.ChannelHeader{
				ChannelId: "",
				TxId:      trxID,
				Type:      int32(common.HeaderType_ENDORSER_TRANSACTION),
			},
			[]peer.TxValidationCode{peer.TxValidationCode_VALID},
			mocks.NewTransaction(
				&rwsetutil.NsRwSet{
					NameSpace: namespace,
					KvRwSet: &kvrwset.KVRWSet{
						Reads: []*kvrwset.KVRead{{Key: key, Version: &kvrwset.Version{BlockNum: blockNum, TxNum: trxNum}}},
					},
				},
			),
		),
	)
	require.NoError(t, err)
	results, err := queryCouchDbById(
		def, dbname, formatKey(namespace, key, blockNum, trxNum),
	)
	require.NoError(t, err, "failed to query test CouchDB")
	assert.Empty(t, results, "read-sets must not be saved to history couchdb")
}

// Query CouchDB with the given def and dbname for documents matching the given id
func queryCouchDbById(def *couchdb.CouchDBDef, dbname, id string) (*[]couchdb.QueryResult, error) {
	return newCouchDbClient(
		def,
		couchdb.ConstructBlockchainDBName(dbname, dbNameSuffix),
	).QueryDocuments(
		fmt.Sprintf(`{ "selector": { "_id": "%s" } }`, id),
	)
}

// Start a CouchDB test instance.
// Use the cleanup function to stop it.
func startCouchDB() (couchDbDef *couchdb.CouchDBDef, cleanup func()) {
	couchDB := &runner.CouchDB{}
	if err := couchDB.Start(); err != nil {
		err := fmt.Errorf("failed to start couchDB: %s", err)
		panic(err)
	}
	return &couchdb.CouchDBDef{
		URL:                 couchDB.Address(),
		MaxRetries:          3,
		Password:            "",
		Username:            "",
		MaxRetriesOnStartup: 3,
		RequestTimeout:      35 * time.Second,
	}, func() { couchDB.Stop() }
}

// Create a new CouchDB client.
func newCouchDbClient(def *couchdb.CouchDBDef, dbname string) *couchdb.CouchDatabase {
	instance, err := couchdb.CreateCouchInstance(
		def.URL, def.Username, def.Password,
		def.MaxRetries, def.MaxRetriesOnStartup,
		def.RequestTimeout,
	)
	if err != nil {
		panic(fmt.Sprintf("cannot create couchdb instance. error: %s", err))
	}
	client, err := couchdb.CreateCouchDatabase(instance, dbname)
	if err != nil {
		panic(fmt.Sprintf("cannot create couchdb database. error: %s", err))
	}
	return client
}

// Composite key format used as CouchDB doc IDs for write-sets.
func formatKey(namespace, key string, blockNum, trxNum uint64) string {
	return fmt.Sprintf("%s-%s-%d-%d", namespace, key, blockNum, trxNum)
}
