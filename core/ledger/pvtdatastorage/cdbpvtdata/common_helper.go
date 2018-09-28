/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbpvtdata

import (
	"math"

	"github.com/hyperledger/fabric/core/ledger/pvtdatastorage/pvtmetadata"

	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/pvtdatapolicy"
	"github.com/hyperledger/fabric/protos/ledger/rwset"
)

// TODO: This file contains code copied from the base private data store. Both of these packages should be refactored.

func prepareStoreEntries(blockNum uint64, pvtdata []*ledger.TxPvtData, btlPolicy pvtdatapolicy.BTLPolicy) ([]*dataEntry, []*expiryEntry, error) {
	dataEntries := prepareDataEntries(blockNum, pvtdata)
	expiryEntries, err := prepareExpiryEntries(blockNum, dataEntries, btlPolicy)
	if err != nil {
		return nil, nil, err
	}
	return dataEntries, expiryEntries, nil
}

func prepareDataEntries(blockNum uint64, pvtData []*ledger.TxPvtData) []*dataEntry {
	var dataEntries []*dataEntry
	for _, txPvtdata := range pvtData {
		for _, nsPvtdata := range txPvtdata.WriteSet.NsPvtRwset {
			for _, collPvtdata := range nsPvtdata.CollectionPvtRwset {
				txnum := txPvtdata.SeqInBlock
				ns := nsPvtdata.Namespace
				coll := collPvtdata.CollectionName
				dataKey := &dataKey{blockNum, txnum, ns, coll}
				dataEntries = append(dataEntries, &dataEntry{key: dataKey, value: collPvtdata})
			}
		}
	}
	return dataEntries
}

func prepareExpiryEntries(committingBlk uint64, dataEntries []*dataEntry, btlPolicy pvtdatapolicy.BTLPolicy) ([]*expiryEntry, error) {
	mapByExpiringBlk := make(map[uint64]*pvtmetadata.ExpiryData)
	for _, dataEntry := range dataEntries {
		expiringBlk, err := btlPolicy.GetExpiringBlock(dataEntry.key.ns, dataEntry.key.coll, dataEntry.key.blkNum)
		if err != nil {
			return nil, err
		}
		if neverExpires(expiringBlk) {
			continue
		}
		expiryData, ok := mapByExpiringBlk[expiringBlk]
		if !ok {
			expiryData = pvtmetadata.NewExpiryData()
			mapByExpiringBlk[expiringBlk] = expiryData
		}
		expiryData.Add(dataEntry.key.ns, dataEntry.key.coll, dataEntry.key.txNum)
	}
	var expiryEntries []*expiryEntry
	for expiryBlk, expiryData := range mapByExpiringBlk {
		expiryKey := &expiryKey{expiringBlk: expiryBlk, committingBlk: committingBlk}
		expiryEntries = append(expiryEntries, &expiryEntry{key: expiryKey, value: expiryData})
	}
	return expiryEntries, nil
}

func deriveDataKeys(expiryEntry *expiryEntry) []*dataKey {
	var dataKeys []*dataKey
	for ns, colls := range expiryEntry.value.Map {
		for coll, txNums := range colls.Map {
			for _, txNum := range txNums.List {
				dataKeys = append(dataKeys, &dataKey{expiryEntry.key.committingBlk, txNum, ns, coll})
			}
		}
	}
	return dataKeys
}

func passesFilter(dataKey *dataKey, filter ledger.PvtNsCollFilter) bool {
	return filter == nil || filter.Has(dataKey.ns, dataKey.coll)
}

func isExpired(dataKey *dataKey, btl pvtdatapolicy.BTLPolicy, latestBlkNum uint64) (bool, error) {
	expiringBlk, err := btl.GetExpiringBlock(dataKey.ns, dataKey.coll, dataKey.blkNum)
	if err != nil {
		return false, err
	}
	return latestBlkNum >= expiringBlk, nil
}

func neverExpires(expiringBlkNum uint64) bool {
	return expiringBlkNum == math.MaxUint64
}

type txPvtdataAssembler struct {
	blockNum, txNum uint64
	txWset          *rwset.TxPvtReadWriteSet
	currentNsWSet   *rwset.NsPvtReadWriteSet
	firstCall       bool
}

func newTxPvtdataAssembler(blockNum, txNum uint64) *txPvtdataAssembler {
	return &txPvtdataAssembler{blockNum, txNum, &rwset.TxPvtReadWriteSet{}, nil, true}
}

func (a *txPvtdataAssembler) add(ns string, collPvtWset *rwset.CollectionPvtReadWriteSet) {
	// start a NsWset
	if a.firstCall {
		a.currentNsWSet = &rwset.NsPvtReadWriteSet{Namespace: ns}
		a.firstCall = false
	}

	// if a new ns started, add the existing NsWset to TxWset and start a new one
	if a.currentNsWSet.Namespace != ns {
		a.txWset.NsPvtRwset = append(a.txWset.NsPvtRwset, a.currentNsWSet)
		a.currentNsWSet = &rwset.NsPvtReadWriteSet{Namespace: ns}
	}
	// add the collWset to the current NsWset
	a.currentNsWSet.CollectionPvtRwset = append(a.currentNsWSet.CollectionPvtRwset, collPvtWset)
}

func (a *txPvtdataAssembler) done() {
	if a.currentNsWSet != nil {
		a.txWset.NsPvtRwset = append(a.txWset.NsPvtRwset, a.currentNsWSet)
	}
	a.currentNsWSet = nil
}

func (a *txPvtdataAssembler) getTxPvtdata() *ledger.TxPvtData {
	a.done()
	return &ledger.TxPvtData{SeqInBlock: a.txNum, WriteSet: a.txWset}
}

// ErrOutOfRange is to be thrown for the request for the data that is not yet committed
type ErrOutOfRange struct {
	msg string
}

// NewErrOutOfRange creates an out of range error
func NewErrOutOfRange(msg string) *ErrOutOfRange {
	return &ErrOutOfRange{msg}
}

func (err *ErrOutOfRange) Error() string {
	return err.msg
}

// NotFoundInIndexErr is used to indicate missing entry in the index
type NotFoundInIndexErr string

func (NotFoundInIndexErr) Error() string {
	return "Entry not found in index"
}
