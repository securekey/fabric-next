/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package pvtmetadata

// TODO: This file contains code copied from the base private data store. The base package should be refactored.

// NewExpiryData creates an expiry data instance
func NewExpiryData() *ExpiryData {
	return &ExpiryData{Map: make(map[string]*Collections)}
}

// NewCollections creates a collections instance
func NewCollections() *Collections {
	return &Collections{Map: make(map[string]*TxNums)}
}

// Add appends txNum to the expiry data list
func (e *ExpiryData) Add(ns, coll string, txNum uint64) {
	collections, ok := e.Map[ns]
	if !ok {
		collections = NewCollections()
		e.Map[ns] = collections
	}
	txNums, ok := collections.Map[coll]
	if !ok {
		txNums = &TxNums{}
		collections.Map[coll] = txNums
	}
	txNums.List = append(txNums.List, txNum)
}
