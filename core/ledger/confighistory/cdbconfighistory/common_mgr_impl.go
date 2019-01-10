/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbconfighistory

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/core/common/privdata"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/confighistory"
	"github.com/hyperledger/fabric/protos/common"
)

// TODO: This file contains code copied from the base config history mgr. Both of these packages should be refactored.

const (
	lsccNamespace = "lscc"
)

type compositeKey struct {
	ns, key  string
	blockNum uint64
}

type compositeKV struct {
	*compositeKey
	value []byte
}

func (c *ConfigHistoryMgr) InterestedInNamespaces() []string {
	return []string{lsccNamespace}
}

func (c *ConfigHistoryMgr) HandleStateUpdates(trigger *ledger.StateUpdateTrigger) error {
//TODO: This function is completely changed in 1.4 Has to be re written
	return nil
}

func (c *ConfigHistoryMgr) StateCommitDone(channelID string) {
	// Noop
}

func (c *ConfigHistoryMgr) GetRetriever(ledgerID string, ledgerInfoRetriever confighistory.LedgerInfoRetriever) ledger.ConfigHistoryRetriever {
	db, err := c.getDB(ledgerID)
	if err != nil {
		logger.Errorf("getDB return error %s", err)
	}
	return &retriever{db: db, ledgerInfoRetriever: ledgerInfoRetriever}
}

// Close implements the function in the interface 'Mgr'
func (c *ConfigHistoryMgr) Close() {

}

// MostRecentCollectionConfigBelow implements function from the interface ledger.ConfigHistoryRetriever
func (r *retriever) MostRecentCollectionConfigBelow(blockNum uint64, chaincodeName string) (*ledger.CollectionConfigInfo, error) {
	compositeKV, err := r.mostRecentEntryBelow(blockNum, lsccNamespace, constructCollectionConfigKey(chaincodeName), chaincodeName)
	if err != nil || compositeKV == nil {
		return nil, err
	}
	return compositeKVToCollectionConfig(compositeKV)
}

// CollectionConfigAt implements function from the interface ledger.ConfigHistoryRetriever
func (r *retriever) CollectionConfigAt(blockNum uint64, chaincodeName string) (*ledger.CollectionConfigInfo, error) {
	info, err := r.ledgerInfoRetriever.GetBlockchainInfo()
	if err != nil {
		return nil, err
	}
	maxCommittedBlockNum := info.Height - 1
	if maxCommittedBlockNum < blockNum {
		return nil, &ledger.ErrCollectionConfigNotYetAvailable{MaxBlockNumCommitted: maxCommittedBlockNum,
			Msg: fmt.Sprintf("The maximum block number committed [%d] is less than the requested block number [%d]", maxCommittedBlockNum, blockNum)}
	}

	compositeKV, err := r.entryAt(blockNum, lsccNamespace, constructCollectionConfigKey(chaincodeName))
	if err != nil || compositeKV == nil {
		return nil, err
	}
	return compositeKVToCollectionConfig(compositeKV)
}

func constructCollectionConfigKey(chaincodeName string) string {
	return privdata.BuildCollectionKVSKey(chaincodeName)
}

func compositeKVToCollectionConfig(compositeKV *compositeKV) (*ledger.CollectionConfigInfo, error) {
	conf := &common.CollectionConfigPackage{}
	if err := proto.Unmarshal(compositeKV.value, conf); err != nil {
		logger.Debugf("Error unmarshalling collection config: %s", err)
		return nil, err
	}
	logger.Debugf("Unmarshalled collection config: %+v", conf)
	return &ledger.CollectionConfigInfo{CollectionConfig: conf, CommittingBlockNum: compositeKV.blockNum}, nil
}
