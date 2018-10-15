/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbconfighistory

import (
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/confighistory"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/pkg/errors"
)

// TODO: This file contains code copied from the base config history mgr. Both of these packages should be refactored.

const (
	lsccNamespace = "lscc"
)

func (c *ConfigHistoryMgr) InterestedInNamespaces() []string {
	return []string{lsccNamespace}
}

func (c *ConfigHistoryMgr) HandleStateUpdates(ledgerID string, stateUpdates ledger.StateUpdates, commitHeight uint64) error {
	return c.prepareDBBatch(stateUpdates, commitHeight, ledgerID)
}

func (c *ConfigHistoryMgr) StateCommitDone(channelID string) {
	// Noop
}

func (c *ConfigHistoryMgr) GetRetriever(ledgerID string, ledgerInfoRetriever confighistory.LedgerInfoRetriever) ledger.ConfigHistoryRetriever {
	return &retriever{db: nil, ledgerInfoRetriever: ledgerInfoRetriever}
}

// Close implements the function in the interface 'Mgr'
func (c *ConfigHistoryMgr) Close() {

}

type retriever struct {
	ledgerInfoRetriever confighistory.LedgerInfoRetriever
	db                  *couchdb.CouchDatabase
}

// MostRecentCollectionConfigBelow implements function from the interface ledger.ConfigHistoryRetriever
func (r *retriever) MostRecentCollectionConfigBelow(blockNum uint64, chaincodeName string) (*ledger.CollectionConfigInfo, error) {
	return nil, errors.New("not implemented")
}

// CollectionConfigAt implements function from the interface ledger.ConfigHistoryRetriever
func (r *retriever) CollectionConfigAt(blockNum uint64, chaincodeName string) (*ledger.CollectionConfigInfo, error) {
	return nil, errors.New("not implemented")
}
