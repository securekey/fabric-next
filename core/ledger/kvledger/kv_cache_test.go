/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package kvledger

import (
	"fmt"
	"testing"

	"github.com/hyperledger/fabric/common/ledger/testutil"
)

func TestKVCache(t *testing.T) {
	InitKVCache()

	kvCache, _ := GetKVCache("LSCC")

	for i := 0; i < 500; i++ {
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

	kvCache2, _ := GetKVCache("VSCC")

	for i := 0; i < 500; i++ {
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

	for i := 0; i < 500; i++ {
		theKey := fmt.Sprintf("%s-%d", "Key", i)
		kvCache.Remove(theKey)
		_, ok := kvCache.Get(theKey)
		testutil.AssertEquals(t, ok, false)
	}

	testutil.AssertEquals(t, kvCache.Size(), 0)

	kvCache2.Clear()

	testutil.AssertEquals(t, kvCache2.Size(), 0)
}
