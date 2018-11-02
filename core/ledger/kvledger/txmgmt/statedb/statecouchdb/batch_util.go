/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package statecouchdb

import (
	"sync"

	"github.com/hyperledger/fabric/common/metrics"
)

// batch is executed in a separate goroutine.
type batch interface {
	execute() error
}

// executeBatches executes each batch in a separate goroutine and returns error if
// any of the batches return error during its execution
func executeBatches(batches []batch) error {
	logger.Debugf("Executing batches = %s", batches)
	if metrics.IsDebug() {
		stopWatch := metrics.RootScope.Timer("statecouchdb_executeBatches_time_seconds").Start()
		defer stopWatch.Stop()
	}

	numBatches := len(batches)
	if metrics.IsDebug() {
		metrics.RootScope.Gauge("statecouchdb_numBatches").Update(float64(numBatches))
	}
	if numBatches == 0 {
		return nil
	}
	if numBatches == 1 {
		return batches[0].execute()
	}
	var batchWG sync.WaitGroup
	batchWG.Add(numBatches)
	errsChan := make(chan error, numBatches)
	defer close(errsChan)
	for _, b := range batches {
		go func(b batch) {
			defer batchWG.Done()
			if err := b.execute(); err != nil {
				errsChan <- err
				return
			}
		}(b)
	}
	batchWG.Wait()
	if len(errsChan) > 0 {
		return <-errsChan
	}
	return nil
}
