/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package validationresults

import (
	"testing"

	"github.com/hyperledger/fabric/core/committer/txvalidator/validationpolicy"
	"github.com/stretchr/testify/assert"
)

const (
	org1MSP = "Org1MSP"
	org2MSP = "Org2MSP"
	p1Org1  = "p1.org1.com"
	p2Org1  = "p2.org1.com"
	p1Org2  = "p1.org2.com"
	p2Org2  = "p2.org2.com"
)

func TestValidationResultsCache(t *testing.T) {
	cache := NewCache()

	blockNum := uint64(1000)

	r1 := &validationpolicy.ValidationResults{
		BlockNumber: blockNum,
		TxFlags:     []uint8{255, 0, 255, 0},
		MSPID:       org1MSP,
		Endpoint:    p1Org1,
	}
	r2 := &validationpolicy.ValidationResults{
		BlockNumber: blockNum,
		TxFlags:     []uint8{255, 0, 255, 0},
		MSPID:       org2MSP,
		Endpoint:    p2Org2,
	}
	r3 := &validationpolicy.ValidationResults{
		BlockNumber: blockNum,
		TxFlags:     []uint8{0, 255, 0, 255},
		MSPID:       org2MSP,
		Endpoint:    p1Org2,
	}

	results := cache.Add(r1)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, r1, results[0])

	results = cache.Add(r2)
	assert.Equal(t, 2, len(results))
	assert.Equal(t, r1, results[0])
	assert.Equal(t, r2, results[1])

	results = cache.Add(r3)
	assert.Equal(t, 1, len(results))
	assert.Equal(t, r3, results[0])

	results = cache.Remove(blockNum)
	assert.Equal(t, 3, len(results))

	results = cache.Remove(blockNum)
	assert.Empty(t, results)

}
