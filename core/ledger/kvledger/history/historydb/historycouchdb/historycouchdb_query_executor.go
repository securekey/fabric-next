/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package historycouchdb

import (
	"encoding/json"
	"fmt"

	"github.com/hyperledger/fabric/core/ledger/kvledger/history/historydb"

	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/protos/ledger/queryresult"

	"github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/pkg/errors"
)

// queryExecutor implements ledger.HistoryQueryExecutor.
type queryExecutor struct {
	couchDB    *couchdb.CouchDatabase
	blockStore blkstorage.BlockStore
}

// resultsIter implements ledger.ResultsIterator.
// Pagination of results from CouchDB is handled transparently.
type resultsIter struct {
	couchDB    *couchdb.CouchDatabase
	namespace  string
	key        string
	pageNum    int
	pageSize   int
	page       *resultsPage
	blockStore blkstorage.BlockStore
}

// Custom iterator that holds a single page of results from querying CouchDB.
type resultsPage struct {
	results    writeSets
	resultsPos int
}

// Returns the next writeSet and advances the internal cursor's position, or returns nil if there are no more writesets.
func (page *resultsPage) Next() *writeSet {
	if page.resultsPos < len(page.results) {
		idx := page.resultsPos
		page.resultsPos++
		return &page.results[idx]
	}
	return nil
}

// queryExecutor implements HistoryQueryExecutor.GetHistoryForKey()
func (qe *queryExecutor) GetHistoryForKey(namespace, key string) (ledger.ResultsIterator, error) {
	return &resultsIter{
		couchDB:    qe.couchDB,
		namespace:  namespace,
		key:        key,
		pageNum:    0,
		pageSize:   30,
		page:       &resultsPage{},
		blockStore: qe.blockStore,
	}, nil
}

// resultsIter implements ResultsIterator.Next()
func (iter *resultsIter) Next() (ledger.QueryResult, error) {
	var next ledger.QueryResult
	var err error
	if writeset := iter.page.Next(); writeset != nil {
		next, err = getKeyModificationFromBlockStore(iter.blockStore, *writeset)
	} else {
		// Refresh cache
		if err = iter.loadNextPage(); err == nil {
			if writeset = iter.page.Next(); writeset != nil {
				next, err = getKeyModificationFromBlockStore(iter.blockStore, *writeset)
			}
		}
	}
	return next, err
}

// resultsIter implements ResultIterator.Close()
func (iter *resultsIter) Close() {
	iter.pageNum = 0
	iter.page = &resultsPage{}
}

// Fetches the next page of results from CouchDB, advancing the page number.
func (iter *resultsIter) loadNextPage() error {
	query := fmt.Sprintf(`
		{
			"selector": {
				"Namespace": "%s",
				"Key": "%s"
			},
			"use_index": "_design/%s",
			"sort": [ {"BlockNum": "asc"}, {"TrxNum": "asc"} ],
			"skip": %d,
			"limit": %d
		}`,
		iter.namespace, iter.key, writesetIndexDesignDoc, iter.pageNum*iter.pageSize, iter.pageSize,
	)
	results, _, err := iter.couchDB.QueryDocuments(query)
	if err != nil {
		return errors.Wrapf(err, "failed to query HistoryDB on CouchDB; query: [%s]", query)
	}
	var writesets writeSets
	for _, result := range results {
		ws := writeSet{}
		if err := json.Unmarshal(result.Value, &ws); err != nil {
			return errors.Wrapf(err, "failed to unmarshal this HistoryDB writeset document from CouchDB: [%s]", string(result.Value))
		}
		writesets = append(writesets, ws)
	}
	iter.pageNum++
	iter.page = &resultsPage{resultsPos: 0, results: writesets}
	return nil
}

// Returns a queryresult.KeyModification{} by querying the blockstore for the timestamp and the modification made to the
// given key.
func getKeyModificationFromBlockStore(blockstore blkstorage.BlockStore, writeset writeSet) (ledger.QueryResult, error) {
	envlpe, err := blockstore.RetrieveTxByBlockNumTranNum(writeset.BlockNum, writeset.TrxNum)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to retrieve transaction from blockstore for %v", writeset)
	}
	if envlpe == nil {
		return nil, errors.New(fmt.Sprintf("transaction envelope not found in blockstore for writeset [%+v]", writeset))
	}
	modification, err := historydb.GetKeyModificationFromTran(envlpe, writeset.Namespace, writeset.Key)
	if err != nil {
		return nil, err
	}
	logger.Debugf(
		"Found historic key value for namespace:%s key:%s from transaction %s in CouchDB\n",
		writeset.Namespace, writeset.Key, modification.(*queryresult.KeyModification).TxId,
	)
	return modification, nil
}
