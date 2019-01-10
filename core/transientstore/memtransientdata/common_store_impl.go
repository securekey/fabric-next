/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package memtransientdata

import (
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/transientstore"
	"github.com/hyperledger/fabric/protos/ledger/rwset"
	"github.com/hyperledger/fabric/protos/peer"
	pb "github.com/hyperledger/fabric/protos/transientstore"
)

// TODO: This file contains code copied from the base transient store. Both of these packages should be refactored.

func (s *store) Persist(txid string, blockHeight uint64, privateSimulationResults *rwset.TxPvtReadWriteSet) error {

	logger.Debugf("Persisting private data to transient store for txid [%s] at block height [%d]", txid, blockHeight)
	return s.persistDB(txid, blockHeight, privateSimulationResults)
}

func (s *store) PersistWithConfig(txid string, blockHeight uint64, privateSimulationResultsWithConfig *pb.TxPvtReadWriteSetWithConfigInfo) error {
	/*stopWatch := metrics.StopWatch("memtransientdata_persistwithconfig_duration")
	defer stopWatch()*/
	return s.persistWithConfigDB(txid, blockHeight, privateSimulationResultsWithConfig)
}

func (s *store) GetTxPvtRWSetByTxid(txid string, filter ledger.PvtNsCollFilter, endorsers []*peer.Endorsement) (transientstore.RWSetScanner, error) {
	/*stopWatch := metrics.StopWatch("memtransientdata_gettxpvtrwsetbytxid_duration")
	defer stopWatch()*/
	return s.getTxPvtRWSetByTxidDB(txid, filter, endorsers)
}

func (s *store) PurgeByTxids(txids []string) error {
	/*stopWatch := metrics.StopWatch("memtransientdata_purgebytxids_duration")
	defer stopWatch()*/
	return s.purgeByTxidsDB(txids)
}

func (s *store) PurgeByHeight(maxBlockNumToRetain uint64) error {
	return s.purgeByHeightDB(maxBlockNumToRetain)
}

func (s *store) GetMinTransientBlkHt() (uint64, error) {
	return s.getMinTransientBlkHtDB()
}

func (s *store) Shutdown() {

}
