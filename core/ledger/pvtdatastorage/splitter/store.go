/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package splitter

import (
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/pvtdatapolicy"
	"github.com/hyperledger/fabric/core/ledger/pvtdatastorage"
)

type store struct {
	sa pvtdatastorage.Store
	sb pvtdatastorage.Store
}

func (s *store) Init(btlPolicy pvtdatapolicy.BTLPolicy) {
	s.sb.Init(btlPolicy)
	s.sa.Init(btlPolicy)
}

func (s *store) InitLastCommittedBlock(blockNum uint64) error {
	err := s.sb.InitLastCommittedBlock(blockNum)
	if err != nil {
		return err
	}
	return s.sa.InitLastCommittedBlock(blockNum)
}

func (s *store) Prepare(blockNum uint64, pvtData []*ledger.TxPvtData) error {
	err := s.sb.Prepare(blockNum, pvtData)
	if err != nil {
		return err
	}
	return s.sa.Prepare(blockNum, pvtData)
}

func (s *store) Commit() error {
	err := s.sb.Commit()
	if err != nil {
		return err
	}
	return s.sa.Commit()
}

func (s *store) Rollback() error {
	return s.sb.Rollback()
	//return s.sa.Rollback()
}

func (s *store) GetPvtDataByBlockNum(blockNum uint64, filter ledger.PvtNsCollFilter) ([]*ledger.TxPvtData, error) {
	return s.sb.GetPvtDataByBlockNum(blockNum, filter)
	//return s.sa.GetPvtDataByBlockNum(blockNum, filter)
}

func (s *store) IsEmpty() (bool, error) {
	return s.sb.IsEmpty()
	//return s.sa.IsEmpty()
}

func (s *store) LastCommittedBlockHeight() (uint64, error) {
	return s.sb.LastCommittedBlockHeight()
	//return s.sa.LastCommittedBlockHeight()
}

func (s *store) HasPendingBatch() (bool, error) {
	return s.sb.HasPendingBatch()
	//return s.sa.HasPendingBatch()
}

func (s *store) Shutdown() {
	s.sb.Shutdown()
	//s.sa.Shutdown()
}
