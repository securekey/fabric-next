/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package validationresults

import (
	"hash/fnv"
	"sync"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/committer/txvalidator/validationpolicy"
	ledgerUtil "github.com/hyperledger/fabric/core/ledger/util"
	"github.com/op/go-logging"
)

var logger = flogging.MustGetLogger("committer/txvalidator")

// Cache is a cache of validation results segmented by TxFlags. One or more
// peers will validate a certain subset of transactions within a block. If two
// peers, say, validate the same set of transactions and come up with the exact
// same results then both of the results will be added to the same segment under
// the block.
type Cache struct {
	lock           sync.Mutex
	resultsByBlock map[uint64]results
}

// NewCache returns a new validation results cache
func NewCache() *Cache {
	return &Cache{
		resultsByBlock: make(map[uint64]results),
	}
}

// Add adds the given validation result to the cache under the appropriate
// block/segment and returns all validation results for the segment.
func (c *Cache) Add(result *validationpolicy.ValidationResults) []*validationpolicy.ValidationResults {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.get(result.BlockNumber).add(result)
}

// Remove removes all validation results for the given block
func (c *Cache) Remove(blockNum uint64) []*validationpolicy.ValidationResults {
	c.lock.Lock()
	defer c.lock.Unlock()

	segmentsForBlock, ok := c.resultsByBlock[blockNum]
	if !ok {
		return nil
	}

	var resultsForBlock []*validationpolicy.ValidationResults
	for _, results := range segmentsForBlock {
		resultsForBlock = append(resultsForBlock, results...)
	}

	logger.Infof("Deleting cached validation results for block %d", blockNum)

	delete(c.resultsByBlock, blockNum)
	return resultsForBlock
}

func (c *Cache) get(blockNum uint64) results {
	resultsForBlock, ok := c.resultsByBlock[blockNum]
	if !ok {
		resultsForBlock = make(results)
		c.resultsByBlock[blockNum] = resultsForBlock
	}
	return resultsForBlock
}

func hash(txFlags ledgerUtil.TxValidationFlags) uint32 {
	h := fnv.New32a()
	_, err := h.Write(txFlags)
	if err != nil {
		panic(err.Error())
	}
	return h.Sum32()
}

type results map[uint32][]*validationpolicy.ValidationResults

func (r results) add(result *validationpolicy.ValidationResults) []*validationpolicy.ValidationResults {
	segment := hash(result.TxFlags)
	results := append(r[segment], result)
	r[segment] = results

	if logger.IsEnabledFor(logging.INFO) {
		logger.Infof("Results for segment %d (block: %d)", segment, result.BlockNumber)
		for _, r := range results {
			logger.Infof("- from [%s]: %v", r.Endpoint, r.TxFlags)
		}
	}

	return results
}
