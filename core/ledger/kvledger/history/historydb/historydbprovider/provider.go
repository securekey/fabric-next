/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package historydbprovider

import (
	"github.com/pkg/errors"

	"github.com/hyperledger/fabric/core/ledger/kvledger/history/historydb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/history/historydb/historycouchdb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/history/historydb/historyleveldb"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
)

// NewHistoryDBProvider instantiates HistoryDBProvider
func NewHistoryDBProvider() (historydb.HistoryDBProvider, error) {
	historyStorageConfig := ledgerconfig.GetHistoryStoreProvider()

	switch historyStorageConfig {
	case ledgerconfig.LevelDBHistoryStorage:
		return historyleveldb.NewHistoryDBProvider()
	case ledgerconfig.CouchDBHistoryStorage:
		return historycouchdb.NewHistoryDBProvider()
	}

	return nil, errors.New("history storage provider creation failed due to unknown configuration")
}
