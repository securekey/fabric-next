/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package statedb

import (
	"fmt"
	"testing"

	"github.com/hyperledger/fabric/common/ledger/testutil"
)

const N_LOOP = 500
const N_PVT_LOOP = 2

func TestKVCache(t *testing.T) {
	InitKVCache()

	kvCache, _ := GetKVCache("MyCh", "LSCC")

	for i := 0; i < N_LOOP; i++ {
		theKey := fmt.Sprintf("%s-%d", "Key", i)
		theValue := fmt.Sprintf("%s-%d", "Val", i)
		theBlockNum := uint64(i / 100)
		theIndex := 100

		theValidatedTx := &ValidatedTx{
			Key:          theKey,
			Value:        []byte(theValue),
			BlockNum:     theBlockNum,
			IndexInBlock: theIndex,
		}

		kvCache.Put(theValidatedTx)
		validatedTx, ok := kvCache.Get(theKey)
		testutil.AssertEquals(t, ok, true)
		testutil.AssertEquals(t, theValidatedTx.Key, validatedTx.Key)
		testutil.AssertEquals(t, theValidatedTx.Value, validatedTx.Value)
		testutil.AssertEquals(t, theValidatedTx.BlockNum, validatedTx.BlockNum)
		testutil.AssertEquals(t, theValidatedTx.IndexInBlock, validatedTx.IndexInBlock)
		if i+1 > kvCache.Capacity() {
			testutil.AssertEquals(t, kvCache.Size(), kvCache.Capacity())
		} else {
			testutil.AssertEquals(t, kvCache.Size(), i+1)
		}
	}

	kvCache2, _ := GetKVCache("MyCh", "VSCC")

	for i := 0; i < N_LOOP; i++ {
		theKey := fmt.Sprintf("%s-%d", "Key", i)
		theValue := fmt.Sprintf("%s-%d", "Val", i)
		theBlockNum := uint64(i / 100)
		theIndex := 100

		theValidatedTx := &ValidatedTx{
			Key:          theKey,
			Value:        []byte(theValue),
			BlockNum:     theBlockNum,
			IndexInBlock: theIndex,
		}

		kvCache2.Put(theValidatedTx)
		validatedTx, ok := kvCache2.Get(theKey)
		testutil.AssertEquals(t, ok, true)
		testutil.AssertEquals(t, theValidatedTx.Key, validatedTx.Key)
		testutil.AssertEquals(t, theValidatedTx.Value, validatedTx.Value)
		testutil.AssertEquals(t, theValidatedTx.BlockNum, validatedTx.BlockNum)
		testutil.AssertEquals(t, theValidatedTx.IndexInBlock, validatedTx.IndexInBlock)
		if i+1 > kvCache2.Capacity() {
			testutil.AssertEquals(t, kvCache2.Size(), kvCache.Capacity())
		} else {
			testutil.AssertEquals(t, kvCache2.Size(), i+1)
		}
	}

	for i := 0; i < N_LOOP; i++ {
		theKey := fmt.Sprintf("%s-%d", "Key", i)
		kvCache.MustRemove(theKey)
		_, ok := kvCache.Get(theKey)
		testutil.AssertEquals(t, ok, false)
	}

	testutil.AssertEquals(t, kvCache.Size(), 0)

	kvCache2.Clear()

	testutil.AssertEquals(t, kvCache2.Size(), 0)
}

func TestKVCachePrivate(t *testing.T) {
	InitKVCache()

	kvCache, _ := GetKVCache("MyCh", "LSCC")

	for i := 0; i < N_PVT_LOOP; i++ {
		theKey := fmt.Sprintf("%s-%d", "Key", i)
		theValue := fmt.Sprintf("%s-%d", "Val", i)
		theBlockNum := uint64(i / 100)
		theIndex := 100

		theValidatedTx := ValidatedTx{
			Key:          theKey,
			Value:        []byte(theValue),
			BlockNum:     theBlockNum,
			IndexInBlock: theIndex,
		}

		namespace := DerivePvtDataNs("LSCC", "mycoll")

		pvtData := &ValidatedPvtData{Level1ExpiringBlock: uint64(i), Level2ExpiringBlock: 1, ValidatedTxOp: ValidatedTxOp{ChId: "MyCh", Namespace: namespace, ValidatedTx: theValidatedTx}, Collection: "mycoll"}

		kvCache.PutPrivate(pvtData)
		validatedTx, ok := kvCache.Get(theKey)
		testutil.AssertEquals(t, ok, true)
		testutil.AssertEquals(t, theValidatedTx.Key, validatedTx.Key)
		testutil.AssertEquals(t, theValidatedTx.Value, validatedTx.Value)
		testutil.AssertEquals(t, theValidatedTx.BlockNum, validatedTx.BlockNum)
		testutil.AssertEquals(t, theValidatedTx.IndexInBlock, validatedTx.IndexInBlock)
	}

	purgeNonDurable(0)

	// Check that first key has been moved to LRU
	theKey := fmt.Sprintf("%s-%d", "Key", 0)

	// first key is stored in 'permanent' cache hence missing in 'non-durable'
	pvtData, ok := kvCache.getNonDurable(theKey)
	testutil.AssertEquals(t, ok, false)
	testutil.AssertNil(t, pvtData)

	validatedTx, ok := kvCache.Get(theKey)
	testutil.AssertEquals(t, ok, true)
	testutil.AssertNotNil(t, validatedTx)

	// Second key has not expired yet
	theKey = fmt.Sprintf("%s-%d", "Key", 1)
	pvtData, ok = kvCache.getNonDurable(theKey)
	testutil.AssertEquals(t, ok, true)
	testutil.AssertNotNil(t, pvtData)

	validatedTx, ok = kvCache.Get(theKey)
	testutil.AssertEquals(t, ok, true)
	testutil.AssertNotNil(t, validatedTx)

	purgeNonDurable(1)

	// second key has been removed from expired
	pvtData, ok = kvCache.getNonDurable(theKey)
	testutil.AssertEquals(t, ok, false)
	testutil.AssertNil(t, pvtData)

	// second key has not been moved to lru since level1=level2
	validatedTx, ok = kvCache.Get(theKey)
	testutil.AssertEquals(t, ok, false)
	testutil.AssertNil(t, validatedTx)

}
