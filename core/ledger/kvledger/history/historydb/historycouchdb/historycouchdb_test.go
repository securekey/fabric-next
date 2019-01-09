/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package historycouchdb

import (
	"fmt"
	"testing"
	"time"

	"github.com/hyperledger/fabric/core/ledger"

	"github.com/hyperledger/fabric/common/ledger/blkstorage/mocks"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/hyperledger/fabric/integration/runner"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric/protos/peer"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Can create a new HistoryDBProvider
func TestCanReturnProvider(t *testing.T) {
	def, cleanup := startCouchDB()
	defer cleanup()
	provider, err := NewHistoryDBProvider(def)
	assert.NoError(t, err, "cannot create historycouchdb provider")
	assert.NotNil(t, provider)
}

// Can get a DBHandle from the HistoryDBProvider
func TestProviderCanGetDbHandle(t *testing.T) {
	def, cleanup := startCouchDB()
	defer cleanup()
	provider, _ := NewHistoryDBProvider(def)
	handle, err := provider.GetDBHandle("historydb")
	assert.NoError(t, err, "cannot get DBHandle from historycouchdb provider")
	assert.NotNil(t, handle)
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
		mocks.CreateBlock(
			blockNum,
			&mocks.Transaction{
				ChannelHeader: &common.ChannelHeader{
					ChannelId: "",
					TxId:      trxID,
					Type:      int32(common.HeaderType_ENDORSER_TRANSACTION),
				},
				ValidationCode: peer.TxValidationCode_VALID,
				Transaction: mocks.CreateTransaction(
					&rwsetutil.NsRwSet{
						NameSpace: namespace,
						KvRwSet: &kvrwset.KVRWSet{
							Writes: []*kvrwset.KVWrite{{Key: key, IsDelete: false, Value: []byte("some_value")}},
						},
					},
				),
			},
		),
	)
	require.NoError(t, err)
	results, err := queryCouchDbByNsAndKey(def, dbname, namespace, key)
	require.NoErrorf(t, err, "failed to query test CouchDB")
	assert.NotEmpty(t, results, "write-sets must be saved to history couchdb")
}

// DBHandle.Commit() only saves write sets to database for valid transactions that are endorsed.
func TestHistoryDB_Commit_NoDuplicateCommits(t *testing.T) {
	const namespace = "test_namespace"
	const key = "test_key"
	const dbname = "historydb"
	def, cleanup := startCouchDB()
	defer cleanup()
	provider, err := NewHistoryDBProvider(def)
	require.NoError(t, err)
	handle, err := provider.GetDBHandle(dbname)
	require.NoError(t, err)
	block := mocks.CreateBlock(
		uint64(123),
		&mocks.Transaction{
			ChannelHeader: &common.ChannelHeader{
				ChannelId: "",
				TxId:      "12345",
				Type:      int32(common.HeaderType_ENDORSER_TRANSACTION),
			},
			ValidationCode: peer.TxValidationCode_VALID,
			Transaction: mocks.CreateTransaction(
				&rwsetutil.NsRwSet{
					NameSpace: namespace,
					KvRwSet: &kvrwset.KVRWSet{
						Writes: []*kvrwset.KVWrite{{Key: key, IsDelete: false, Value: []byte("some_value")}},
					},
				},
			),
		},
	)
	err = handle.Commit(block)
	require.NoError(t, err)
	err = handle.Commit(block)
	require.NoError(t, err)
	results, err := queryCouchDbByNsAndKey(def, dbname, namespace, key)
	require.NoErrorf(t, err, "failed to query test CouchDB")
	assert.NotEmpty(t, results, "write-sets must be saved to history couchdb")
	assert.Len(t, results, 1, "write-sets are being duplicated to HistoryCouchDB when calling Commit() with the same block")
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
		mocks.CreateBlock(
			blockNum,
			&mocks.Transaction{
				ChannelHeader: &common.ChannelHeader{
					ChannelId: "",
					TxId:      trxID,
					Type:      int32(common.HeaderType_ENDORSER_TRANSACTION),
				},
				ValidationCode: peer.TxValidationCode_VALID,
				Transaction: mocks.CreateTransaction(
					&rwsetutil.NsRwSet{
						NameSpace: namespace,
						KvRwSet: &kvrwset.KVRWSet{
							Writes: []*kvrwset.KVWrite{{Key: key, IsDelete: false, Value: []byte("some_value")}},
						},
					},
				),
			},
		),
	)
	require.NoError(t, err)
	results, _, err := queryCouchDbById(def, dbname, heightDocIdKey)
	require.NoErrorf(t, err, "failed to query test CouchDB")
	assert.NotEmpty(t, results, "block height must be saved to history couchdb")
}

// DBHandle.GetLastSavePoint() should return the block height with the block number of the _last_ committed
// block and the number of transactions in said block.
func TestGetLastSavePointReturnsBlockHeight(t *testing.T) {
	const dbname = "historydb"
	const blockNum1 = uint64(1234)
	const blockNum2 = uint64(5678)
	require.NotEqual(t, blockNum1, blockNum2)
	def, cleanup := startCouchDB()
	defer cleanup()
	provider, err := NewHistoryDBProvider(def)
	require.NoError(t, err)
	handle, err := provider.GetDBHandle(dbname)
	require.NoError(t, err)
	err = handle.Commit(
		mocks.CreateBlock(
			blockNum1,
			&mocks.Transaction{
				ChannelHeader: &common.ChannelHeader{
					ChannelId: "",
					TxId:      "123",
					Type:      int32(common.HeaderType_ENDORSER_TRANSACTION),
				},
				ValidationCode: peer.TxValidationCode_VALID,
				Transaction: mocks.CreateTransaction(
					&rwsetutil.NsRwSet{
						NameSpace: "namespace",
						KvRwSet: &kvrwset.KVRWSet{
							Reads:  []*kvrwset.KVRead{{Key: "key1", Version: &kvrwset.Version{BlockNum: blockNum1, TxNum: uint64(5)}}},
							Writes: []*kvrwset.KVWrite{{Key: "key2", IsDelete: false, Value: []byte("some_value")}},
						},
					},
				),
			},
		),
	)
	require.NoError(t, err)
	err = handle.Commit(
		mocks.CreateBlock(
			blockNum2,
			&mocks.Transaction{
				ChannelHeader: &common.ChannelHeader{
					ChannelId: "",
					TxId:      "123",
					Type:      int32(common.HeaderType_ENDORSER_TRANSACTION),
				},
				ValidationCode: peer.TxValidationCode_VALID,
				Transaction: mocks.CreateTransaction(
					&rwsetutil.NsRwSet{
						NameSpace: "namespace",
						KvRwSet: &kvrwset.KVRWSet{
							Reads:  []*kvrwset.KVRead{{Key: "key1", Version: &kvrwset.Version{BlockNum: blockNum2, TxNum: uint64(5)}}},
							Writes: []*kvrwset.KVWrite{{Key: "key2", IsDelete: false, Value: []byte("some_value")}},
						},
					},
				),
			},
			&mocks.Transaction{
				ChannelHeader: &common.ChannelHeader{
					ChannelId: "",
					TxId:      "123",
					Type:      int32(common.HeaderType_ENDORSER_TRANSACTION),
				},
				ValidationCode: peer.TxValidationCode_VALID,

				Transaction: mocks.CreateTransaction(
					&rwsetutil.NsRwSet{
						NameSpace: "namespace",
						KvRwSet: &kvrwset.KVRWSet{
							Reads:  []*kvrwset.KVRead{{Key: "key1", Version: &kvrwset.Version{BlockNum: blockNum2, TxNum: uint64(5)}}},
							Writes: []*kvrwset.KVWrite{{Key: "key2", IsDelete: false, Value: []byte("some_other_value")}},
						},
					},
				),
			},
		),
	)
	require.NoError(t, err)
	height, err := handle.GetLastSavepoint()
	require.NoError(t, err)
	require.NotNil(t, height)
	assert.Equal(t, blockNum2, height.BlockNum)
	assert.Equal(t, uint64(2), height.TxNum)
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
		mocks.CreateBlock(
			blockNum,
			&mocks.Transaction{
				ChannelHeader: &common.ChannelHeader{
					ChannelId: "",
					TxId:      trxID,
					Type:      int32(common.HeaderType_ENDORSER_TRANSACTION),
				},
				ValidationCode: peer.TxValidationCode_VALID,
				Transaction: mocks.CreateTransaction(
					&rwsetutil.NsRwSet{
						NameSpace: namespace,
						KvRwSet: &kvrwset.KVRWSet{
							Reads: []*kvrwset.KVRead{{Key: key, Version: &kvrwset.Version{BlockNum: blockNum, TxNum: uint64(5)}}},
						},
					},
				),
			},
		),
	)
	require.NoError(t, err)
	results, _, err := queryCouchDbById(def, dbname, "height")
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
		mocks.CreateBlock(
			blockNum,
			&mocks.Transaction{
				ChannelHeader: &common.ChannelHeader{
					ChannelId: "",
					TxId:      trxID,
					Type:      int32(headerType),
				},
				ValidationCode: peer.TxValidationCode_VALID,
				Transaction: mocks.CreateTransaction(
					&rwsetutil.NsRwSet{
						NameSpace: namespace,
						KvRwSet: &kvrwset.KVRWSet{
							Writes: []*kvrwset.KVWrite{{Key: key, IsDelete: false, Value: []byte("some_value")}},
						},
					},
				),
			},
		),
	)
	require.NoError(t, err)
	results, err := queryCouchDbByNsAndKey(def, dbname, namespace, key)
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
		mocks.CreateBlock(
			blockNum,
			&mocks.Transaction{
				ChannelHeader: &common.ChannelHeader{
					ChannelId: "",
					TxId:      trxID,
					Type:      int32(common.HeaderType_ENDORSER_TRANSACTION),
				},
				ValidationCode: txValidationCode,
				Transaction: mocks.CreateTransaction(
					&rwsetutil.NsRwSet{
						NameSpace: namespace,
						KvRwSet: &kvrwset.KVRWSet{
							Writes: []*kvrwset.KVWrite{{Key: key, IsDelete: false, Value: []byte("some_value")}},
						},
					},
				),
			},
		),
	)
	require.NoError(t, err)
	results, err := queryCouchDbByNsAndKey(def, dbname, namespace, key)
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
		mocks.CreateBlock(
			blockNum,
			&mocks.Transaction{
				ChannelHeader: &common.ChannelHeader{
					ChannelId: "",
					TxId:      trxID,
					Type:      int32(common.HeaderType_ENDORSER_TRANSACTION),
				},
				ValidationCode: peer.TxValidationCode_VALID,
				Transaction: mocks.CreateTransaction(
					&rwsetutil.NsRwSet{
						NameSpace: namespace,
						KvRwSet: &kvrwset.KVRWSet{
							Reads: []*kvrwset.KVRead{{Key: key, Version: &kvrwset.Version{BlockNum: blockNum, TxNum: trxNum}}},
						},
					},
				),
			},
		),
	)
	require.NoError(t, err)
	results, err := queryCouchDbByNsAndKey(def, dbname, namespace, key)
	require.NoError(t, err, "failed to query test CouchDB")
	assert.Empty(t, results, "read-sets must not be saved to history couchdb")
}

// DBHandle.ShouldRecover() should:
// - return 'true' for recovery because we're passing in a different block number than the last committed block's number
// - return (last_committed_blockNum + 1) as the value for the recovery block's number
// - return no error
func TestShouldRecover(t *testing.T) {
	viper.Set("ledger.history.enableHistoryDatabase", true)
	const committedBlockNum = uint64(456)
	const someOtherBlockNum = uint64(987)
	require.NotEqual(t, committedBlockNum, someOtherBlockNum)
	def, cleanup := startCouchDB()
	defer cleanup()
	provider, err := NewHistoryDBProvider(def)
	require.NoError(t, err)
	handle, err := provider.GetDBHandle("testhistorydb")
	require.NoError(t, err)
	err = handle.Commit(
		mocks.CreateBlock(
			committedBlockNum,
			&mocks.Transaction{
				ChannelHeader: &common.ChannelHeader{
					ChannelId: "",
					TxId:      "123",
					Type:      int32(common.HeaderType_ENDORSER_TRANSACTION),
				},
				ValidationCode: peer.TxValidationCode_VALID,
				Transaction: mocks.CreateTransaction(
					&rwsetutil.NsRwSet{
						NameSpace: "namespace",
						KvRwSet: &kvrwset.KVRWSet{
							Reads: []*kvrwset.KVRead{{Key: "key1", Version: &kvrwset.Version{BlockNum: committedBlockNum, TxNum: uint64(123)}}},
						},
					},
				),
			},
		),
	)
	require.NoError(t, err)
	recover, recoveryBlockNum, err := handle.ShouldRecover(someOtherBlockNum)
	assert.True(t, recover, "recovery should be signalled if the given block number is different than the last committed block's number")
	assert.Equal(t, committedBlockNum+1, recoveryBlockNum, "recovery should indicate the recovery block's number")
	assert.NoError(t, err, "recovery should not throw an error")
}

// DBHandle.ShouldRecover() should:
// - return 'false' for recovery because we're passing in the same block number equal to the last committed block's number
// - return (last_committed_blockNum + 1) as the value for the recovery block's number
// - return no error
func TestShouldNotRecoverIfBlockNumbersMatch(t *testing.T) {
	viper.Set("ledger.history.enableHistoryDatabase", true)
	const committedBlockNum = uint64(456)
	def, cleanup := startCouchDB()
	defer cleanup()
	provider, err := NewHistoryDBProvider(def)
	require.NoError(t, err)
	handle, err := provider.GetDBHandle("testhistorydb")
	require.NoError(t, err)
	err = handle.Commit(
		mocks.CreateBlock(
			committedBlockNum,
			&mocks.Transaction{
				ChannelHeader: &common.ChannelHeader{
					ChannelId: "",
					TxId:      "123",
					Type:      int32(common.HeaderType_ENDORSER_TRANSACTION),
				},
				ValidationCode: peer.TxValidationCode_VALID,
				Transaction: mocks.CreateTransaction(
					&rwsetutil.NsRwSet{
						NameSpace: "namespace",
						KvRwSet: &kvrwset.KVRWSet{
							Reads: []*kvrwset.KVRead{{Key: "key1", Version: &kvrwset.Version{BlockNum: committedBlockNum, TxNum: uint64(123)}}},
						},
					},
				),
			},
		),
	)
	require.NoError(t, err)
	recover, recoveryBlockNum, err := handle.ShouldRecover(committedBlockNum)
	assert.False(t, recover, "recovery should not be signalled if the given block number is equal to the last committed block's number")
	assert.Equal(t, committedBlockNum+1, recoveryBlockNum, "recovery should indicate the recovery block's expected number")
	assert.NoError(t, err, "recovery should not throw an error")
}

// DBHandle.ShouldRecover() should:
// - return 'false' for recovery because we're passing in the same block number equal to the last committed block's number
// - return (last_committed_blockNum + 1) as the value for the recovery block's number
// - return no error
func TestShouldNotRecoverIfHistoryDbIsDisabled(t *testing.T) {
	viper.Set("ledger.history.enableHistoryDatabase", false)
	const committedBlockNum = uint64(456)
	def, cleanup := startCouchDB()
	defer cleanup()
	provider, err := NewHistoryDBProvider(def)
	require.NoError(t, err)
	handle, err := provider.GetDBHandle("testhistorydb")
	require.NoError(t, err)
	recover, recoveryBlockNum, err := handle.ShouldRecover(committedBlockNum)
	assert.False(t, recover, "recovery should not be signalled if historyDB is disabled in the configuration")
	assert.Equal(t, uint64(0), recoveryBlockNum, "recovery should indicate 0 as the recovery block number when historyDB is disabled")
	assert.NoError(t, err, "recovery should not throw an error")
}

// DBHandle.ShouldRecover() should:
// - return 'true' for recovery because there is no savepoint
// - return 0 as the block to recover from
// - return no error
func TestShouldRecoverIfNoSavepoint(t *testing.T) {
	viper.Set("ledger.history.enableHistoryDatabase", true)
	def, cleanup := startCouchDB()
	defer cleanup()
	provider, err := NewHistoryDBProvider(def)
	require.NoError(t, err)
	handle, err := provider.GetDBHandle("testhistorydb")
	require.NoError(t, err)
	savepoint, err := handle.GetLastSavepoint()
	require.NoError(t, err)
	require.Nil(t, savepoint)
	recover, recoveryBlockNum, err := handle.ShouldRecover(uint64(123))
	assert.True(t, recover, "recovery should always be signalled if there is no savepoint")
	assert.Equal(t, uint64(0), recoveryBlockNum, "recovery should indicate 0 as the recovery block number when there is no savepoint")
	assert.NoError(t, err, "recovery should not throw an error")
}

// Test of CommitLostBlock()
func TestHistoryDB_CommitLostBlock(t *testing.T) {
	const namespace = "test_namespace"
	const key = "test_key"
	const dbname = "historydb"
	def, cleanup := startCouchDB()
	defer cleanup()
	provider, err := NewHistoryDBProvider(def)
	require.NoError(t, err)
	handle, err := provider.GetDBHandle(dbname)
	require.NoError(t, err)
	block := mocks.CreateBlock(
		uint64(1),
		&mocks.Transaction{
			ChannelHeader: &common.ChannelHeader{
				ChannelId: "",
				TxId:      "12345",
				Type:      int32(common.HeaderType_ENDORSER_TRANSACTION),
			},
			ValidationCode: peer.TxValidationCode_VALID,
			Transaction: mocks.CreateTransaction(
				&rwsetutil.NsRwSet{
					NameSpace: namespace,
					KvRwSet: &kvrwset.KVRWSet{
						Writes: []*kvrwset.KVWrite{{Key: key, IsDelete: false, Value: []byte("some_value")}},
					},
				},
			),
		},
	)
	err = handle.CommitLostBlock(
		&ledger.BlockAndPvtData{Block: block},
	)
	require.NoError(t, err)
	results, err := queryCouchDbByNsAndKey(def, dbname, namespace, key)
	require.NoErrorf(t, err, "failed to query test CouchDB")
	assert.NotEmpty(t, results, "write-sets must be saved to history couchdb when committing a lost block")
	savepoint, err := handle.GetLastSavepoint()
	require.NoError(t, err)
	assert.Equal(t, block.GetHeader().GetNumber(), savepoint.BlockNum, "savepoint not updated after CommitLostBlock()")
}

// Query CouchDB with the given def and dbname for documents matching the given id
func queryCouchDbById(def *couchdb.CouchDBDef, dbname, id string) (*couchdb.CouchDoc, string, error) {
	return newCouchDbClient(
		def,
		couchdb.ConstructBlockchainDBName(dbname, dbNameSuffix),
	).ReadDoc(id)
}

// Query CouchDB for writesets for the given namespace and key.
func queryCouchDbByNsAndKey(def *couchdb.CouchDBDef, dbname, namespace, key string) ([]*couchdb.QueryResult, error) {
	queryResult, _ , err := newCouchDbClient(
		def,
		couchdb.ConstructBlockchainDBName(dbname, dbNameSuffix),
	).QueryDocuments(fmt.Sprintf(`
		{
			"selector": {
				"Namespace": "%s",
				"Key": "%s"
			}
		}
	`, namespace, key))
	return queryResult, err
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
		def.RequestTimeout, false, nil,
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
