/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbtransientdata

import (
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/transientstore"
	"github.com/hyperledger/fabric/protos/ledger/rwset"
	pb "github.com/hyperledger/fabric/protos/transientstore"
	"github.com/pkg/errors"
)

// TODO: This file contains code copied from the base transient store. Both of these packages should be refactored.

func (s *store) Persist(txid string, blockHeight uint64, privateSimulationResults *rwset.TxPvtReadWriteSet) error {
	return errors.New("not implemented")
}

func (s *store) PersistWithConfig(txid string, blockHeight uint64, privateSimulationResultsWithConfig *pb.TxPvtReadWriteSetWithConfigInfo) error {
	return errors.New("not implemented")
}

func (s *store) GetTxPvtRWSetByTxid(txid string, filter ledger.PvtNsCollFilter) (transientstore.RWSetScanner, error) {
	return nil, errors.New("not implemented")
}

func (s *store) PurgeByTxids(txids []string) error {
	return errors.New("not implemented")
}

func (s *store) PurgeByHeight(maxBlockNumToRetain uint64) error {
	return errors.New("not implemented")
}

func (s *store) GetMinTransientBlkHt() (uint64, error) {
	return 0, errors.New("not implemented")
}

func (s *store) Shutdown() {

}
