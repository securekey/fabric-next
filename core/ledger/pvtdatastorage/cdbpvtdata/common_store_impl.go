/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbpvtdata

import (
	"fmt"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/pvtdatapolicy"
	"github.com/hyperledger/fabric/core/ledger/pvtdatastorage"
	"github.com/hyperledger/fabric/protos/ledger/rwset"
	"sync"
)

// TODO: This file contains code copied from the base private data store. Both of these packages should be refactored.

type commonStore struct {
	ledgerid  string
	btlPolicy pvtdatapolicy.BTLPolicy

	isEmpty            bool
	lastCommittedBlock uint64
	batchPending       bool
	purgerLock         sync.Mutex
}

type blkTranNumKey []byte

type dataEntry struct {
	key   *dataKey
	value *rwset.CollectionPvtReadWriteSet
}

type expiryEntry struct {
	key   *expiryKey
	value *ExpiryData
}

type expiryKey struct {
	expiringBlk   uint64
	committingBlk uint64
}

type dataKey struct {
	blkNum   uint64
	txNum    uint64
	ns, coll string
}

func (s *store) nextBlockNum() uint64 {
	if s.isEmpty {
		return 0
	}
	return s.lastCommittedBlock + 1
}

func (s *store) prepareInit(blockNum uint64, pvtData []*ledger.TxPvtData) error {
	if s.batchPending {
		return pvtdatastorage.NewErrIllegalCall(`A pending batch exists as as result of last invoke to "Prepare" call.
			 Invoke "Commit" or "Rollback" on the pending batch before invoking "Prepare" function`)
	}
	expectedBlockNum := s.nextBlockNum()
	if expectedBlockNum != blockNum {
		return pvtdatastorage.NewErrIllegalArgs(fmt.Sprintf("Expected block number=%d, recived block number=%d", expectedBlockNum, blockNum))
	}

	return nil
}

func (s *store) prepareDone(blockNum uint64, pvtData []*ledger.TxPvtData) error {
	s.batchPending = true
	logger.Debugf("Saved %d private data write sets for block [%d]", len(pvtData), blockNum)

	return nil
}