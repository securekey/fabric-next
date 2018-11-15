/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package kvledger

import (
	"math"
	"testing"

	"github.com/hyperledger/fabric/common/ledger/testutil"
)

func TestMin(t *testing.T) {

	r := min(1, 2)
	testutil.AssertEquals(t, r, uint64(1))

	r = min(5, 2)
	testutil.AssertEquals(t, r, uint64(2))

}

func TestFistLevelCacheExpiryBlock(t *testing.T) {

	r := getFirstLevelCacheExpiryBlock(1, 2)
	testutil.AssertEquals(t, r, uint64(4))

	r = getFirstLevelCacheExpiryBlock(1, 0)
	testutil.AssertEquals(t, r, uint64(math.MaxUint64))

}

func TestSecondLevelCacheExpiryBlock(t *testing.T) {

	r := getSecondLevelCacheExpiryBlock(1, 2)
	testutil.AssertEquals(t, r, uint64(4))

	r = getSecondLevelCacheExpiryBlock(1, 0)
	testutil.AssertEquals(t, r, uint64(math.MaxUint64))

	r = getSecondLevelCacheExpiryBlock(1000, math.MaxUint64-100)
	testutil.AssertEquals(t, r, uint64(math.MaxUint64))

}
