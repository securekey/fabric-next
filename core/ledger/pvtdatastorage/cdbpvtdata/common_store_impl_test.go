/*
Copyright IBM Corp, SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbpvtdata

import (
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"

	"github.com/hyperledger/fabric/common/ledger/testutil"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/core/ledger/pvtdatapolicy"
	btltestutil "github.com/hyperledger/fabric/core/ledger/pvtdatapolicy/testutil"
	"github.com/hyperledger/fabric/core/ledger/pvtdatastorage"
)

func TestEmptyStore(t *testing.T) {
	env := NewTestStoreEnv(t, "TestEmptyStore", nil)
	defer env.Cleanup()
	assert := assert.New(t)
	store := env.TestStore
	testEmpty(true, assert, store)
	testPendingBatch(false, assert, store)
}

func TestMetadata(t *testing.T) {
	cs := btltestutil.NewMockCollectionStore()
	cs.SetBTL("ns-1", "coll-1", 0)
	cs.SetBTL("ns-1", "coll-2", 0)
	cs.SetBTL("ns-2", "coll-1", 0)
	cs.SetBTL("ns-2", "coll-2", 0)
	btlPolicy := pvtdatapolicy.ConstructBTLPolicy(cs)

	env := NewTestStoreEnv(t, "TestMetadata", btlPolicy)
	defer env.Cleanup()
	assert := assert.New(t)
	store := env.TestStore
	testEmpty(true, assert, store)
	testPendingBatch(false, assert, store)

	testData := []*ledger.TxPvtData{
		produceSamplePvtdata(t, 2, []string{"ns-1:coll-1", "ns-1:coll-2", "ns-2:coll-1", "ns-2:coll-2"}),
		produceSamplePvtdata(t, 4, []string{"ns-1:coll-1", "ns-1:coll-2", "ns-2:coll-1", "ns-2:coll-2"}),
	}

	isEmpty, err := store.IsEmpty()
	assert.NoError(err)
	assert.True(isEmpty)
	testLastCommittedBlockHeight(0, assert, store)
	// no pvt data with block 0 should not store any pvt data
	assert.NoError(store.Prepare(0, nil))
	// nil pvtData does not expect to set pending anymore
	testPendingBatch(false, assert, store)
	// lastCommittedBlockHeight in Commit should still be increased for the next block even if there's no doc to commit
	assert.NoError(store.Commit())
	testPendingBatch(false, assert, store)
	testLastCommittedBlockHeight(1, assert, store)

	isEmpty, err = store.IsEmpty()
	assert.NoError(err)
	assert.False(isEmpty)

	// non empty pvt data for block 1
	assert.NoError(store.Prepare(1, testData))
	testPendingBatch(true, assert, store)
	testLastCommittedBlockHeight(1, assert, store)
	assert.NoError(store.Commit())
	testPendingBatch(false, assert, store)
	testLastCommittedBlockHeight(2, assert, store)

	// pvt data with block 2 - rollback
	assert.NoError(store.Prepare(2, testData))
	testPendingBatch(true, assert, store)
	assert.NoError(store.Rollback())
	testPendingBatch(false, assert, store)
	testLastCommittedBlockHeight(2, assert, store)

	// write non empty pvt data for block 2
	assert.NoError(store.Prepare(2, testData))
	testPendingBatch(true, assert, store)
	testLastCommittedBlockHeight(2, assert, store)
	assert.NoError(store.Commit())
	testPendingBatch(false, assert, store)
	testLastCommittedBlockHeight(3, assert, store)

	// write pvt data for block 3 (empty pvt data)
	assert.NoError(store.Prepare(3, nil))
	testPendingBatch(false, assert, store)
	testLastCommittedBlockHeight(3, assert, store)
	assert.NoError(store.Commit())
	testLastCommittedBlockHeight(4, assert, store)

	// try to write an old block # (should fail)
	assert.Error(store.Prepare(2, testData))
	// calling Commit again should have no effect (as it only purges old blocks if applicable with empty pendingDocs)
	assert.NoError(store.Commit())
}

func TestStoreBasicCommitAndRetrieval(t *testing.T) {
	cs := btltestutil.NewMockCollectionStore()
	cs.SetBTL("ns-1", "coll-1", 0)
	cs.SetBTL("ns-1", "coll-2", 0)
	cs.SetBTL("ns-2", "coll-1", 0)
	cs.SetBTL("ns-2", "coll-2", 0)
	btlPolicy := pvtdatapolicy.ConstructBTLPolicy(cs)
	env := NewTestStoreEnv(t, "TestStoreBasicCommitAndRetrieval", btlPolicy)
	defer env.Cleanup()
	assert := assert.New(t)
	store := env.TestStore
	testData := []*ledger.TxPvtData{
		produceSamplePvtdata(t, 2, []string{"ns-1:coll-1", "ns-1:coll-2", "ns-2:coll-1", "ns-2:coll-2"}),
		produceSamplePvtdata(t, 4, []string{"ns-1:coll-1", "ns-1:coll-2", "ns-2:coll-1", "ns-2:coll-2"}),
	}

	// no pvt data with block 0
	assert.NoError(store.Prepare(0, nil))
	assert.NoError(store.Commit())

	// pvt data (not empty) with block 1 - commit
	assert.NoError(store.Prepare(1, testData))
	assert.NoError(store.Commit())

	// pvt data with block 2 - rollback
	assert.NoError(store.Prepare(2, testData))
	assert.NoError(store.Rollback())

	// pvt data retrieval for block 0 should return nil
	var nilFilter ledger.PvtNsCollFilter
	retrievedData, err := store.GetPvtDataByBlockNum(0, nilFilter)
	assert.NoError(err)
	assert.Nil(retrievedData)

	// pvt data retrieval for block 1 should return full pvtdata
	retrievedData, err = store.GetPvtDataByBlockNum(1, nilFilter)
	assert.NoError(err)
	assert.Equal(testData, retrievedData)

	// pvt data retrieval for block 1 with filter should return filtered pvtdata
	filter := ledger.NewPvtNsCollFilter()
	filter.Add("ns-1", "coll-1")
	filter.Add("ns-2", "coll-2")
	retrievedData, err = store.GetPvtDataByBlockNum(1, filter)
	expectedRetrievedData := []*ledger.TxPvtData{
		produceSamplePvtdata(t, 2, []string{"ns-1:coll-1", "ns-2:coll-2"}),
		produceSamplePvtdata(t, 4, []string{"ns-1:coll-1", "ns-2:coll-2"}),
	}
	testutil.AssertEquals(t, retrievedData, expectedRetrievedData)

	// pvt data retrieval for block 2 should return ErrOutOfRange
	retrievedData, err = store.GetPvtDataByBlockNum(2, nilFilter)
	_, ok := err.(*pvtdatastorage.ErrOutOfRange)
	assert.True(ok)
	assert.Nil(retrievedData)
}

func TestExpiryDataNotIncluded(t *testing.T) {
	ledgerid := "TestExpiryDataNotIncluded"
	cs := btltestutil.NewMockCollectionStore()
	cs.SetBTL("ns-1", "coll-1", 1)
	cs.SetBTL("ns-1", "coll-2", 0)
	cs.SetBTL("ns-2", "coll-1", 0)
	cs.SetBTL("ns-2", "coll-2", 2)
	btlPolicy := pvtdatapolicy.ConstructBTLPolicy(cs)

	env := NewTestStoreEnv(t, ledgerid, btlPolicy)
	defer env.Cleanup()
	assert := assert.New(t)
	store := env.TestStore

	// no pvt data with block 0
	assert.NoError(store.Prepare(0, nil))
	assert.NoError(store.Commit())

	// write pvt data (not empty) for block 1
	testDataForBlk1 := []*ledger.TxPvtData{
		produceSamplePvtdata(t, 2, []string{"ns-1:coll-1", "ns-1:coll-2", "ns-2:coll-1", "ns-2:coll-2"}),
		produceSamplePvtdata(t, 4, []string{"ns-1:coll-1", "ns-1:coll-2", "ns-2:coll-1", "ns-2:coll-2"}),
	}
	assert.NoError(store.Prepare(1, testDataForBlk1))
	assert.NoError(store.Commit())

	// write pvt data for block 2
	testDataForBlk2 := []*ledger.TxPvtData{
		produceSamplePvtdata(t, 3, []string{"ns-1:coll-1", "ns-1:coll-2", "ns-2:coll-1", "ns-2:coll-2"}),
		produceSamplePvtdata(t, 5, []string{"ns-1:coll-1", "ns-1:coll-2", "ns-2:coll-1", "ns-2:coll-2"}),
	}
	assert.NoError(store.Prepare(2, testDataForBlk2))
	assert.NoError(store.Commit())

	retrievedData, err := store.GetPvtDataByBlockNum(0, nil)
	assert.NoError(err)
	// block 0 has no pvt data so it should not be stored
	assert.Nil(retrievedData)

	// Commit block 3 with no pvtdata
	assert.NoError(store.Prepare(3, nil))
	assert.NoError(store.Commit())

	// After committing block 3, the data for "ns-1:coll1" of block 1 should have expired and should not be returned by the store
	expectedPvtdataFromBlock1 := []*ledger.TxPvtData{
		produceSamplePvtdata(t, 2, []string{"ns-1:coll-2", "ns-2:coll-1", "ns-2:coll-2"}),
		produceSamplePvtdata(t, 4, []string{"ns-1:coll-2", "ns-2:coll-1", "ns-2:coll-2"}),
	}
	retrievedData, _ = store.GetPvtDataByBlockNum(1, nil)
	testutil.AssertEquals(t, retrievedData, expectedPvtdataFromBlock1)

	// Commit block 4 with no pvtdata
	assert.NoError(store.Prepare(4, nil))
	assert.NoError(store.Commit())

	// After committing block 4, the data for "ns-2:coll2" of block 1 should also have expired and should not be returned by the store
	expectedPvtdataFromBlock1 = []*ledger.TxPvtData{
		produceSamplePvtdata(t, 2, []string{"ns-1:coll-2", "ns-2:coll-1"}),
		produceSamplePvtdata(t, 4, []string{"ns-1:coll-2", "ns-2:coll-1"}),
	}
	retrievedData, _ = store.GetPvtDataByBlockNum(1, nil)
	testutil.AssertEquals(t, retrievedData, expectedPvtdataFromBlock1)

	// Now, for block 2, "ns-1:coll1" should also have expired
	expectedPvtdataFromBlock2 := []*ledger.TxPvtData{
		produceSamplePvtdata(t, 3, []string{"ns-1:coll-2", "ns-2:coll-1", "ns-2:coll-2"}),
		produceSamplePvtdata(t, 5, []string{"ns-1:coll-2", "ns-2:coll-1", "ns-2:coll-2"}),
	}
	retrievedData, _ = store.GetPvtDataByBlockNum(2, nil)
	testutil.AssertEquals(t, retrievedData, expectedPvtdataFromBlock2)
}

func TestStorePurge(t *testing.T) {
	ledgerid := "TestStorePurge"
	viper.Set("ledger.pvtdataStore.purgeInterval", 2)
	cs := btltestutil.NewMockCollectionStore()
	cs.SetBTL("ns-1", "coll-1", 1)
	cs.SetBTL("ns-1", "coll-2", 0)
	cs.SetBTL("ns-2", "coll-1", 0)
	cs.SetBTL("ns-2", "coll-2", 4)
	btlPolicy := pvtdatapolicy.ConstructBTLPolicy(cs)

	env := NewTestStoreEnv(t, ledgerid, btlPolicy)
	defer env.Cleanup()
	assert := assert.New(t)
	s := env.TestStore

	// no pvt data with block 0
	assert.NoError(s.Prepare(0, nil))
	assert.NoError(s.Commit())

	// write pvt data for block 1
	testDataForBlk1 := []*ledger.TxPvtData{
		produceSamplePvtdata(t, 2, []string{"ns-1:coll-1", "ns-1:coll-2", "ns-2:coll-1", "ns-2:coll-2"}),
		produceSamplePvtdata(t, 4, []string{"ns-1:coll-1", "ns-1:coll-2", "ns-2:coll-1", "ns-2:coll-2"}),
	}
	assert.NoError(s.Prepare(1, testDataForBlk1))
	assert.NoError(s.Commit())

	// write pvt data for block 2
	assert.NoError(s.Prepare(2, nil))
	assert.NoError(s.Commit())
	// data for ns-1:coll-1 and ns-2:coll-2 should exist in store
	testWaitForPurgerRoutineToFinish(s)
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-1", coll: "coll-1"}))
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-2", coll: "coll-2"}))

	// write pvt data for block 3
	assert.NoError(s.Prepare(3, nil))
	assert.NoError(s.Commit())
	// data for ns-1:coll-1 and ns-2:coll-2 should exist in store (because purger should not be launched at block 3)
	testWaitForPurgerRoutineToFinish(s)
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-1", coll: "coll-1"}))
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-2", coll: "coll-2"}))

	// write pvt data for block 4
	assert.NoError(s.Prepare(4, nil))
	assert.NoError(s.Commit())
	// data for ns-1:coll-1 should not exist in store (because purger should be launched at block 4) but ns-2:coll-2 should exist because it
	// expires at block 5
	testWaitForPurgerRoutineToFinish(s)
	assert.False(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-1", coll: "coll-1"}))
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-2", coll: "coll-2"}))

	// write pvt data for block 5
	assert.NoError(s.Prepare(5, nil))
	assert.NoError(s.Commit())
	// ns-2:coll-2 should exist because though the data expires at block 5 but purger is launched every second block
	testWaitForPurgerRoutineToFinish(s)
	assert.False(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-1", coll: "coll-1"}))
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-2", coll: "coll-2"}))

	// write pvt data for block 6
	assert.NoError(s.Prepare(6, nil))
	assert.NoError(s.Commit())
	// ns-2:coll-2 should not exists now (because purger should be launched at block 6)
	testWaitForPurgerRoutineToFinish(s)
	assert.False(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-1", coll: "coll-1"}))
	assert.False(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-2", coll: "coll-2"}))

	// "ns-2:coll-1" should never have been purged (because, it was no btl was declared for this)
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-1", coll: "coll-2"}))
}

func TestStorePurgeWithEmptPvtData(t *testing.T) {
	ledgerid := "TestStorePurgeWithEmptPvtData"
	viper.Set("ledger.pvtdataStore.purgeInterval", 3)
	cs := btltestutil.NewMockCollectionStore()
	cs.SetBTL("ns-1", "coll-1", 1)
	cs.SetBTL("ns-1", "coll-2", 2)
	btlPolicy := pvtdatapolicy.ConstructBTLPolicy(cs)

	env := NewTestStoreEnv(t, ledgerid, btlPolicy)
	defer env.Cleanup()
	assert := assert.New(t)
	s := env.TestStore

	// no pvt data with block 0
	assert.NoError(s.Prepare(0, nil))
	assert.NoError(s.Commit())

	// write pvt data for block 1
	testDataForBlk1 := []*ledger.TxPvtData{
		produceSamplePvtdata(t, 1, []string{"ns-1:coll-1", "ns-1:coll-2"}),
	}
	assert.NoError(s.Prepare(1, testDataForBlk1))
	assert.NoError(s.Commit())

	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 1, ns: "ns-1", coll: "coll-1"}))
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 1, ns: "ns-1", coll: "coll-2"}))

	assert.NoError(s.Prepare(2, nil))
	assert.NoError(s.Commit())

	assert.NoError(s.Prepare(3, nil))
	assert.NoError(s.Commit())

	testWaitForPurgerRoutineToFinish(s)
	assert.False(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 1, ns: "ns-1", coll: "coll-1"}))
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 1, ns: "ns-1", coll: "coll-2"}))

	assert.NoError(s.Prepare(4, nil))
	assert.NoError(s.Commit())

	assert.NoError(s.Prepare(5, nil))
	assert.NoError(s.Commit())

	assert.NoError(s.Prepare(6, nil))
	assert.NoError(s.Commit())

	pvtData, err := s.GetPvtDataByBlockNum(1, nil)
	assert.NoError(err)
	assert.Nil(pvtData)
}

func TestStoreExpireWithoutPurge(t *testing.T) {
	ledgerid := "TestStoreExpiredWithoutPurge"
	viper.Set("ledger.pvtdataStore.purgeInterval", 2)
	viper.Set("ledger.pvtdataStore.skipPurgeForCollections", "coll-1,coll-2")
	cs := btltestutil.NewMockCollectionStore()
	cs.SetBTL("ns-1", "coll-1", 1)
	cs.SetBTL("ns-1", "coll-2", 0)
	cs.SetBTL("ns-2", "coll-1", 0)
	cs.SetBTL("ns-2", "coll-2", 4)
	btlPolicy := pvtdatapolicy.ConstructBTLPolicy(cs)

	env := NewTestStoreEnv(t, ledgerid, btlPolicy)
	defer env.Cleanup()
	assert := assert.New(t)
	s := env.TestStore

	// no pvt data with block 0
	assert.NoError(s.Prepare(0, nil))
	assert.NoError(s.Commit())

	// write pvt data for block 1
	testDataForBlk1 := []*ledger.TxPvtData{
		produceSamplePvtdata(t, 2, []string{"ns-1:coll-1", "ns-1:coll-2", "ns-2:coll-1", "ns-2:coll-2"}),
		produceSamplePvtdata(t, 4, []string{"ns-1:coll-1", "ns-1:coll-2", "ns-2:coll-1", "ns-2:coll-2"}),
	}
	assert.NoError(s.Prepare(1, testDataForBlk1))
	assert.NoError(s.Commit())

	// write pvt data for block 2
	assert.NoError(s.Prepare(2, nil))
	assert.NoError(s.Commit())
	// data for ns-1:coll-1 and ns-2:coll-2 should exist in store
	testWaitForPurgerRoutineToFinish(s)
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-1", coll: "coll-1"}))
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-2", coll: "coll-2"}))

	// write pvt data for block 3
	assert.NoError(s.Prepare(3, nil))
	assert.NoError(s.Commit())

	// data for ns-1:coll-1 and ns-2:coll-2 should exist in store (because purger should not be launched at block 3)
	testWaitForPurgerRoutineToFinish(s)
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-1", coll: "coll-1"}))
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-2", coll: "coll-2"}))

	// write pvt data for block 4
	assert.NoError(s.Prepare(4, nil))
	assert.NoError(s.Commit())
	// data for ns-1:coll-1 should exist in store since it is in skip-purge-for-collections list, however it is expired  ns-2:coll-2 should exist because it
	// expires at block 5
	testWaitForPurgerRoutineToFinish(s)
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-1", coll: "coll-1"}))
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-2", coll: "coll-2"}))

	pvtData, _ := s.GetPvtDataByBlockNum(1, nil)
	for _, v := range pvtData {
		// data for ns-1:coll-1 expired in store so we should not be able to get it
		assert.False(v.Has("ns-1", "coll-1"))
		// btl for ns-2, coll-2 is 4 so this one did not expire yet
		assert.True(v.Has("ns-2", "coll-2"))
	}

	// write pvt data for block 5
	assert.NoError(s.Prepare(5, nil))
	assert.NoError(s.Commit())
	// ns-2:coll-2 should exist because though the data expires at block 5 but purger is launched every second block
	testWaitForPurgerRoutineToFinish(s)
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-1", coll: "coll-1"}))
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-2", coll: "coll-2"}))

	// write pvt data for block 6
	assert.NoError(s.Prepare(6, nil))
	assert.NoError(s.Commit())
	// purger should be launched at block 6
	testWaitForPurgerRoutineToFinish(s)
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-1", coll: "coll-1"}))
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-2", coll: "coll-2"}))

	pvtData, _ = s.GetPvtDataByBlockNum(1, nil)
	for _, v := range pvtData {
		// btl for ns-2, coll-2 is 4 so this one expired at block 6 too
		assert.False(v.Has("ns-2", "coll-2"))
	}

	// "ns-2:coll-1" should never have been purged (because, it was no btl was declared for this)
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-1", coll: "coll-2"}))
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-2", coll: "coll-1"}))

}

func TestStoreExpireMixed(t *testing.T) {
	ledgerid := "TestStoreExpireMixed"
	viper.Set("ledger.pvtdataStore.purgeInterval", 2)
	viper.Set("ledger.pvtdataStore.skipPurgeForCollections", "coll-1")
	cs := btltestutil.NewMockCollectionStore()
	cs.SetBTL("ns-1", "coll-1", 1)
	cs.SetBTL("ns-1", "coll-2", 3)
	cs.SetBTL("ns-2", "coll-1", 3)
	cs.SetBTL("ns-2", "coll-2", 0)
	btlPolicy := pvtdatapolicy.ConstructBTLPolicy(cs)

	env := NewTestStoreEnv(t, ledgerid, btlPolicy)
	defer env.Cleanup()
	assert := assert.New(t)
	s := env.TestStore

	// no pvt data with block 0
	assert.NoError(s.Prepare(0, nil))
	assert.NoError(s.Commit())

	// write pvt data for block 1
	testDataForBlk1 := []*ledger.TxPvtData{
		produceSamplePvtdata(t, 2, []string{"ns-1:coll-1", "ns-1:coll-2", "ns-2:coll-1", "ns-2:coll-2"}),
		produceSamplePvtdata(t, 4, []string{"ns-1:coll-1", "ns-1:coll-2", "ns-2:coll-1", "ns-2:coll-2"}),
	}
	assert.NoError(s.Prepare(1, testDataForBlk1))
	assert.NoError(s.Commit())

	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-1", coll: "coll-1"}))
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 4, ns: "ns-2", coll: "coll-1"}))

	testWaitForPurgerRoutineToFinish(s)

	// write pvt data for block 2
	assert.NoError(s.Prepare(2, nil))
	assert.NoError(s.Commit())
	testWaitForPurgerRoutineToFinish(s)

	// write pvt data for block 3
	assert.NoError(s.Prepare(3, nil))
	assert.NoError(s.Commit())
	testWaitForPurgerRoutineToFinish(s)

	pvtData, _ := s.GetPvtDataByBlockNum(1, nil)
	for _, v := range pvtData {
		// btl for ns-1, coll-1 is 1 so this one  should be expired
		assert.False(v.Has("ns-1", "coll-1"))
		// btl for ns-1, coll-1 is 3 so this one is not expired (purge = false)
		assert.True(v.Has("ns-2", "coll-1"))
		// btl for ns-1, coll-1 is 3 so this one is not purged yet (purge = true)
		assert.True(v.Has("ns-1", "coll-2"))
		// btl for ns-2, coll-2 is 0 so this one will never be expired or purged
		assert.True(v.Has("ns-2", "coll-2"))
	}

	// write pvt data for block 4
	assert.NoError(s.Prepare(4, nil))
	assert.NoError(s.Commit())
	testWaitForPurgerRoutineToFinish(s)

	// write pvt data for block 5
	assert.NoError(s.Prepare(5, nil))
	assert.NoError(s.Commit())
	testWaitForPurgerRoutineToFinish(s)

	pvtData, _ = s.GetPvtDataByBlockNum(1, nil)
	for _, v := range pvtData {
		// btl for ns-1, coll-1 is 1 so this one should be expired
		assert.False(v.Has("ns-1", "coll-1"))
		// btl for ns-1, coll-1 is 3 so this one is expired too at block 4
		assert.False(v.Has("ns-2", "coll-1"))
		// btl for ns-1, coll-1 is 3 so this one exires at block 4, purges at block 6
		assert.False(v.Has("ns-1", "coll-2"))
		// btl for ns-2, coll-2 is 0 so this one will never be expired or purged
		assert.True(v.Has("ns-2", "coll-2"))
	}

	// coll-1 is never purged (skip purge for this collection is true)
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-1", coll: "coll-1"}))
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-1", coll: "coll-2"}))

	// ns-1,coll-2 gets will get purged at block 6
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 4, ns: "ns-2", coll: "coll-1"}))
	// ns-2, coll-2 never gets purged since blt=0
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-2", coll: "coll-2"}))

	// purger kicks in at block 6
	assert.NoError(s.Prepare(6, nil))
	assert.NoError(s.Commit())
	testWaitForPurgerRoutineToFinish(s)

	// coll-1 is never purged (skip purge for this collection is true)
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-1", coll: "coll-1"}))
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 4, ns: "ns-2", coll: "coll-1"}))

	// coll-2 will be purged for blt=3
	assert.False(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-1", coll: "coll-2"}))
	// ns-2, coll-2 never gets purged since blt=0
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-2", coll: "coll-2"}))
}



func TestIncreasePurgeInterval(t *testing.T) {
	ledgerid := "TestIncreasePurgeInterval"
	viper.Set("ledger.pvtdataStore.purgeInterval", 3)
	viper.Set("ledger.pvtdataStore.skipPurgeForCollections", "coll-1")
	cs := btltestutil.NewMockCollectionStore()
	cs.SetBTL("ns-1", "coll-1", 1)
	cs.SetBTL("ns-1", "coll-2", 3)
	cs.SetBTL("ns-2", "coll-1", 5)
	cs.SetBTL("ns-2", "coll-2", 0)
	btlPolicy := pvtdatapolicy.ConstructBTLPolicy(cs)

	env := NewTestStoreEnv(t, ledgerid, btlPolicy)
	defer env.Cleanup()
	assert := assert.New(t)
	s := env.TestStore

	// no pvt data with block 0
	assert.NoError(s.Prepare(0, nil))
	assert.NoError(s.Commit())

	// write pvt data for block 1
	testDataForBlk1 := []*ledger.TxPvtData{
		produceSamplePvtdata(t, 1, []string{"ns-1:coll-1"}),
		produceSamplePvtdata(t, 2, []string{"ns-1:coll-2"}),
		produceSamplePvtdata(t, 3, []string{"ns-2:coll-1"}),
		produceSamplePvtdata(t, 4, []string{"ns-2:coll-2"}),
	}
	assert.NoError(s.Prepare(1, testDataForBlk1))
	assert.NoError(s.Commit())

	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 1, ns: "ns-1", coll: "coll-1"}))
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-1", coll: "coll-2"}))
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 3, ns: "ns-2", coll: "coll-1"}))
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 4, ns: "ns-2", coll: "coll-2"}))

	testWaitForPurgerRoutineToFinish(s)

	// write pvt data for block 2
	assert.NoError(s.Prepare(2, nil))
	assert.NoError(s.Commit())
	testWaitForPurgerRoutineToFinish(s)

	pvtData, _ := s.GetPvtDataByBlockNum(1, nil)
	for _, v := range pvtData {
		if v.SeqInBlock == 1 {
			assert.True(v.Has("ns-1", "coll-1"))
		}
		if v.SeqInBlock == 2 {
			assert.True(v.Has("ns-1", "coll-2"))
		}
		if v.SeqInBlock == 3 {
			assert.True(v.Has("ns-2", "coll-1"))
		}
		if v.SeqInBlock == 4 {
			assert.True(v.Has("ns-2", "coll-2"))
		}
	}

	// write pvt data for block 3 (purger kicks in)
	assert.NoError(s.Prepare(3, nil))
	assert.NoError(s.Commit())
	testWaitForPurgerRoutineToFinish(s)

	pvtData, _ = s.GetPvtDataByBlockNum(1, nil)
	for _, v := range pvtData {
		if v.SeqInBlock == 1 {
			assert.False(v.Has("ns-1", "coll-1"))
		}
		if v.SeqInBlock == 2 {
			assert.True(v.Has("ns-1", "coll-2"))
		}
		if v.SeqInBlock == 3 {
			assert.True(v.Has("ns-2", "coll-1"))
		}
		if v.SeqInBlock == 4 {
			assert.True(v.Has("ns-2", "coll-2"))
		}
	}

	viper.Set("ledger.pvtdataStore.purgeInterval", 7)

	// write pvt data for block 4
	assert.NoError(s.Prepare(4, nil))
	assert.NoError(s.Commit())
	testWaitForPurgerRoutineToFinish(s)

	// write pvt data for block 5
	assert.NoError(s.Prepare(5, nil))
	assert.NoError(s.Commit())
	testWaitForPurgerRoutineToFinish(s)

	// write pvt data for block 6
	assert.NoError(s.Prepare(6, nil))
	assert.NoError(s.Commit())
	testWaitForPurgerRoutineToFinish(s)

	pvtData, _ = s.GetPvtDataByBlockNum(1, nil)
	for _, v := range pvtData {
		if v.SeqInBlock == 1 {
			assert.False(v.Has("ns-1", "coll-1"))
		}
		if v.SeqInBlock == 2 {
			// purge block was at 6 so it will get cleaned at 7 when purger with new interval kicks in
			assert.True(v.Has("ns-1", "coll-2"))
		}
		if v.SeqInBlock == 3 {
			assert.True(v.Has("ns-2", "coll-1"))
		}
		if v.SeqInBlock == 4 {
			assert.True(v.Has("ns-2", "coll-2"))
		}
	}

	// write pvt data for block 7 (purger kicks in)
	assert.NoError(s.Prepare(7, nil))
	assert.NoError(s.Commit())
	testWaitForPurgerRoutineToFinish(s)

	pvtData, _ = s.GetPvtDataByBlockNum(1, nil)
	for _, v := range pvtData {
		if v.SeqInBlock == 1 {
			assert.False(v.Has("ns-1", "coll-1"))
		}
		if v.SeqInBlock == 2 {
			// purge block was at 6 so it got cleaned at 7 when purger with new interval kicked in
			assert.False(v.Has("ns-1", "coll-2"))
		}
		if v.SeqInBlock == 3 {
			// expiry block equals 7 so it expired
			assert.False(v.Has("ns-2", "coll-1"))
		}
		if v.SeqInBlock == 4 {
			assert.True(v.Has("ns-2", "coll-2"))
		}
	}

	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 1, ns: "ns-1", coll: "coll-1"}))
	assert.False(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-1", coll: "coll-2"}))
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 3, ns: "ns-2", coll: "coll-1"}))
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 4, ns: "ns-2", coll: "coll-2"}))
}



func TestDecreasePurgeInterval(t *testing.T) {
	ledgerid := "TestDecreasePurgeInterval"
	viper.Set("ledger.pvtdataStore.purgeInterval", 5)
	viper.Set("ledger.pvtdataStore.skipPurgeForCollections", "coll-2")
	cs := btltestutil.NewMockCollectionStore()
	cs.SetBTL("ns-1", "coll-1", 1)
	cs.SetBTL("ns-1", "coll-2", 3)
	btlPolicy := pvtdatapolicy.ConstructBTLPolicy(cs)

	env := NewTestStoreEnv(t, ledgerid, btlPolicy)
	defer env.Cleanup()
	assert := assert.New(t)
	s := env.TestStore

	// no pvt data with block 0
	assert.NoError(s.Prepare(0, nil))
	assert.NoError(s.Commit())

	// write pvt data for block 1
	testDataForBlk1 := []*ledger.TxPvtData{
		produceSamplePvtdata(t, 1, []string{"ns-1:coll-1"}),
		produceSamplePvtdata(t, 2, []string{"ns-1:coll-2"}),
	}

	assert.NoError(s.Prepare(1, testDataForBlk1))
	assert.NoError(s.Commit())

	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 1, ns: "ns-1", coll: "coll-1"}))
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-1", coll: "coll-2"}))

	testWaitForPurgerRoutineToFinish(s)

	assert.NoError(s.Prepare(2, nil))
	assert.NoError(s.Commit())
	testWaitForPurgerRoutineToFinish(s)

	viper.Set("ledger.pvtdataStore.purgeInterval", 3)

	// write pvt data for block 3 (purger kicks in)
	assert.NoError(s.Prepare(3, nil))
	assert.NoError(s.Commit())
	testWaitForPurgerRoutineToFinish(s)

	pvtData, _ := s.GetPvtDataByBlockNum(1, nil)
	for _, v := range pvtData {
		if v.SeqInBlock == 1 {
			assert.False(v.Has("ns-1", "coll-1"))
		}
		if v.SeqInBlock == 2 {
			assert.True(v.Has("ns-1", "coll-2"))
		}
	}

	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 1, ns: "ns-1", coll: "coll-1"}))
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-1", coll: "coll-2"}))

	assert.NoError(s.Prepare(4, nil))
	assert.NoError(s.Commit())
	testWaitForPurgerRoutineToFinish(s)

	assert.NoError(s.Prepare(5, nil))
	assert.NoError(s.Commit())
	testWaitForPurgerRoutineToFinish(s)

	// write pvt data for block 6 (purger kicks in)
	assert.NoError(s.Prepare(6, nil))
	assert.NoError(s.Commit())
	testWaitForPurgerRoutineToFinish(s)

	assert.False(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 1, ns: "ns-1", coll: "coll-1"}))
	assert.True(testDataKeyExists(t, s, &dataKey{blkNum: 1, txNum: 2, ns: "ns-1", coll: "coll-2"}))

}



func TestStoreState(t *testing.T) {
	cs := btltestutil.NewMockCollectionStore()
	cs.SetBTL("ns-1", "coll-1", 0)
	cs.SetBTL("ns-1", "coll-2", 0)
	btlPolicy := pvtdatapolicy.ConstructBTLPolicy(cs)

	env := NewTestStoreEnv(t, "TestStoreState", btlPolicy)
	defer env.Cleanup()
	assert := assert.New(t)
	store := env.TestStore
	testData := []*ledger.TxPvtData{
		produceSamplePvtdata(t, 0, []string{"ns-1:coll-1", "ns-1:coll-2"}),
	}
	assert.NoError(store.Prepare(0, nil))
	assert.NoError(store.Commit())
	assert.NoError(store.Prepare(1, testData))

	_, ok := store.Prepare(0, testData).(*pvtdatastorage.ErrIllegalCall)
	assert.True(ok)
	assert.NoError(store.Commit())

	assert.Nil(store.Prepare(1, testData))
	_, ok = store.Prepare(2, testData).(*pvtdatastorage.ErrIllegalCall)
	assert.True(ok)
}

func TestInitLastCommittedBlock(t *testing.T) {
	env := NewTestStoreEnv(t, "TestInitLastCommittedBlock", nil)
	defer env.Cleanup()
	assert := assert.New(t)
	store := env.TestStore
	existingLastBlockNum := uint64(25)
	assert.NoError(store.InitLastCommittedBlock(existingLastBlockNum))

	testEmpty(false, assert, store)
	testPendingBatch(false, assert, store)
	testLastCommittedBlockHeight(existingLastBlockNum+1, assert, store)

	env.CloseAndReopen()
	testEmpty(false, assert, store)
	testPendingBatch(false, assert, store)
	testLastCommittedBlockHeight(existingLastBlockNum+1, assert, store)

	err := store.InitLastCommittedBlock(30)
	_, ok := err.(*pvtdatastorage.ErrIllegalCall)
	assert.True(ok)
}

func TestRestartStore(t *testing.T) {
	testRestart(t, "ledgera")
	testRestart(t, "ledgerb")
}

func testRestart(t *testing.T, ledgerID string) {
	cs := btltestutil.NewMockCollectionStore()
	cs.SetBTL("ns-1", "coll-1", 0)
	cs.SetBTL("ns-1", "coll-2", 0)
	cs.SetBTL("ns-2", "coll-1", 0)
	cs.SetBTL("ns-2", "coll-2", 0)
	btlPolicy := pvtdatapolicy.ConstructBTLPolicy(cs)
	env := NewTestStoreEnv(t, fmt.Sprintf("%s-testRestart", ledgerID), btlPolicy)
	defer env.Cleanup()
	assert := assert.New(t)
	s := env.TestStore

	// no pvt data with block 0
	assert.NoError(s.Prepare(0, nil))
	assert.NoError(s.Commit())

	testDataForBlk := []*ledger.TxPvtData{
		produceSamplePvtdata(t, 2, []string{"ns-1:coll-1", "ns-1:coll-2", "ns-2:coll-1", "ns-2:coll-2"}),
		produceSamplePvtdata(t, 4, []string{"ns-1:coll-1", "ns-1:coll-2", "ns-2:coll-1", "ns-2:coll-2"}),
	}

	assert.NoError(s.Prepare(1, testDataForBlk))
	assert.NoError(s.Commit())
	testLastCommittedBlockHeight(2, assert, s)

	for i := 2; i < 25; i++ {
		env.CloseAndReopen()
		s = env.TestStore

		assert.NoError(s.Prepare(uint64(i), nil))
		assert.NoError(s.Commit())
		testLastCommittedBlockHeight(uint64(i+1), assert, s)
		time.Sleep(10 * time.Millisecond)
	}
}

func testEmpty(expectedEmpty bool, assert *assert.Assertions, store pvtdatastorage.Store) {
	isEmpty, err := store.IsEmpty()
	assert.NoError(err)
	assert.Equal(expectedEmpty, isEmpty)
}

func testPendingBatch(expectedPending bool, assert *assert.Assertions, s pvtdatastorage.Store) {
	hasPendingBatch, err := s.HasPendingBatch()
	assert.NoError(err)
	assert.Equal(expectedPending, hasPendingBatch)
}

func testLastCommittedBlockHeight(expectedBlockHt uint64, assert *assert.Assertions, s pvtdatastorage.Store) {
	blkHt, err := s.LastCommittedBlockHeight()
	assert.NoError(err)
	assert.Equal(expectedBlockHt, blkHt)

	isEmpty, err := s.IsEmpty()
	assert.NoError(err)
	if expectedBlockHt > 0 {
		assert.False(isEmpty)
	}
}

func testDataKeyExists(t *testing.T, s pvtdatastorage.Store, dataKey *dataKey) bool {
	dataKeyBytes := encodeDataKey(dataKey)
	dataKeyHex := hex.EncodeToString(dataKeyBytes)

	pd, err := s.(*store).getPvtDataByBlockNumDB(dataKey.blkNum)
	assert.NoError(t, err)

	_, ok := pd[dataKeyHex]
	return ok
}

func testWaitForPurgerRoutineToFinish(s pvtdatastorage.Store) {
	time.Sleep(1 * time.Second)
	s.(*store).purgerLock.Lock()
	s.(*store).purgerLock.Unlock()
}

func produceSamplePvtdata(t *testing.T, txNum uint64, nsColls []string) *ledger.TxPvtData {
	builder := rwsetutil.NewRWSetBuilder()
	for _, nsColl := range nsColls {
		nsCollSplit := strings.Split(nsColl, ":")
		ns := nsCollSplit[0]
		coll := nsCollSplit[1]
		builder.AddToPvtAndHashedWriteSet(ns, coll, fmt.Sprintf("key-%s-%s", ns, coll), []byte(fmt.Sprintf("value-%s-%s", ns, coll)))
	}
	simRes, err := builder.GetTxSimulationResults()
	assert.NoError(t, err)
	return &ledger.TxPvtData{SeqInBlock: txNum, WriteSet: simRes.PvtSimulationResults}
}
