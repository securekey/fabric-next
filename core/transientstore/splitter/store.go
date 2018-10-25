/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package splitter

import (
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/transientstore"
	"github.com/hyperledger/fabric/protos/ledger/rwset"
	"github.com/hyperledger/fabric/protos/peer"
	pb "github.com/hyperledger/fabric/protos/transientstore"
)

type store struct {
	sa transientstore.Store
	sb transientstore.Store
}

func (s *store) Persist(txid string, blockHeight uint64, privateSimulationResults *rwset.TxPvtReadWriteSet) error {
	s.sb.Persist(txid, blockHeight, privateSimulationResults)
	return s.sa.Persist(txid, blockHeight, privateSimulationResults)
}

func (s *store) PersistWithConfig(txid string, blockHeight uint64, privateSimulationResultsWithConfig *pb.TxPvtReadWriteSetWithConfigInfo) error {
	s.sb.PersistWithConfig(txid, blockHeight, privateSimulationResultsWithConfig)
	return s.sa.PersistWithConfig(txid, blockHeight, privateSimulationResultsWithConfig)
}

func (s *store) GetTxPvtRWSetByTxid(txid string, filter ledger.PvtNsCollFilter, endorsers []*peer.Endorsement) (transientstore.RWSetScanner, error) {
	s.sb.GetTxPvtRWSetByTxid(txid, filter, endorsers)
	return s.sa.GetTxPvtRWSetByTxid(txid, filter, endorsers)
}

func (s *store) PurgeByTxids(txids []string) error {
	s.sb.PurgeByTxids(txids)
	return s.sa.PurgeByTxids(txids)
}

func (s *store) PurgeByHeight(maxBlockNumToRetain uint64) error {
	s.sb.PurgeByHeight(maxBlockNumToRetain)
	return s.sa.PurgeByHeight(maxBlockNumToRetain)
}

func (s *store) GetMinTransientBlkHt() (uint64, error) {
	s.sb.GetMinTransientBlkHt()
	return s.sa.GetMinTransientBlkHt()
}

func (s *store) Shutdown() {
	s.sb.Shutdown()
	s.sa.Shutdown()
}
