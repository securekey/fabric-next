/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package pvtstatepurgemgmt

import (
	"math"
	"sync"

	"github.com/hyperledger/fabric/core/ledger/kvledger/bookkeeping"

	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/privacyenabledstate"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/core/ledger/pvtdatapolicy"
)

// PurgeMgr manages purging of the expired pvtdata
type PurgeMgr interface {
	// PrepareForExpiringKeys gives a chance to the PurgeMgr to do background work in advance if any
	PrepareForExpiringKeys(expiringAtBlk uint64)
	// DeleteExpiredAndUpdateBookkeeping updates the bookkeeping and modifies the update batch by adding the deletes for the expired pvtdata
	DeleteExpiredAndUpdateBookkeeping(
		pvtUpdates *privacyenabledstate.PvtUpdateBatch,
		hashedUpdates *privacyenabledstate.HashedUpdateBatch) error
	// BlockCommitDone is a callback to the PurgeMgr when the block is committed to the ledger
	BlockCommitDone()
}

type keyAndVersion struct {
	key             string
	committingBlock uint64
}

type expiryInfoMap map[*privacyenabledstate.HashedCompositeKey]*keyAndVersion

type workingset struct {
	toPurge             expiryInfoMap
	toClearFromSchedule []*expiryInfoKey
	expiringBlk         uint64
	err                 error
}

type purgeMgr struct {
	btlPolicy pvtdatapolicy.BTLPolicy
	db        privacyenabledstate.DB
	expKeeper expiryKeeper

	lock    *sync.Mutex
	waitGrp *sync.WaitGroup

	workingset *workingset
}

// InstantiatePurgeMgr instantiates a PurgeMgr.
func InstantiatePurgeMgr(ledgerid string, db privacyenabledstate.DB, btlPolicy pvtdatapolicy.BTLPolicy, bookkeepingProvider bookkeeping.Provider) (PurgeMgr, error) {
	return &purgeMgr{
		btlPolicy: btlPolicy,
		db:        db,
		expKeeper: newExpiryKeeper(ledgerid, bookkeepingProvider),
		lock:      &sync.Mutex{},
		waitGrp:   &sync.WaitGroup{},
	}, nil
}

// PrepareForExpiringKeys implements function in the interface 'PurgeMgr'
func (p *purgeMgr) PrepareForExpiringKeys(expiringAtBlk uint64) {
	p.waitGrp.Add(1)
	go func() {
		p.lock.Lock()
		p.waitGrp.Done()
		defer p.lock.Unlock()
		p.workingset = p.prepareWorkingsetFor(expiringAtBlk)
	}()
	p.waitGrp.Wait()
}

// DeleteExpiredAndUpdateBookkeeping implements function in the interface 'PurgeMgr'
func (p *purgeMgr) DeleteExpiredAndUpdateBookkeeping(
	pvtUpdates *privacyenabledstate.PvtUpdateBatch,
	hashedUpdates *privacyenabledstate.HashedUpdateBatch) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	if p.workingset.err != nil {
		return p.workingset.err
	}
	for compositeHashedKey, keyAndVersion := range p.workingset.toPurge {
		ns := compositeHashedKey.Namespace
		coll := compositeHashedKey.CollectionName
		keyHash := []byte(compositeHashedKey.KeyHash)
		key := keyAndVersion.key
		if hashedUpdates.Contains(ns, coll, keyHash) {
			continue
		}
		expiringTxVersion := version.NewHeight(p.workingset.expiringBlk, math.MaxUint64)
		hashedUpdates.Delete(ns, coll, keyHash, expiringTxVersion)
		if keyAndVersion.key != "" {
			pvtUpdates.Delete(ns, coll, key, expiringTxVersion)
		}
	}
	listExpiryInfo, err := buildExpirySchedule(p.btlPolicy, pvtUpdates, hashedUpdates)
	if err != nil {
		return err
	}
	return p.expKeeper.updateBookkeeping(listExpiryInfo, nil)
}

// BlockCommitDone implements function in the interface 'PurgeMgr'
// These orphan entries for purge-schedule can be cleared off in bulk in a separate background routine as well
// If we maintian the following logic (i.e., clear off entries just after block commit), we need a TODO -
// We need to perform a check in the start, becasue there could be a crash between the block commit and
// invocation to this function resulting in the orphan entry for the deletes scheduled for the last block
// Also, the another way is to club the delete of these entries in the same batch that adds entries for the future expirations -
// however, that requires updating the expiry store by replaying the last block from blockchain in order to sustain a crash between
// entries updates and block commit
func (p *purgeMgr) BlockCommitDone() {
	p.expKeeper.updateBookkeeping(nil, p.workingset.toClearFromSchedule)
	p.workingset = nil
}

func (p *purgeMgr) prepareWorkingsetFor(expiringAtBlk uint64) *workingset {
	workingset := &workingset{expiringBlk: expiringAtBlk}
	expiryInfo, err := p.expKeeper.retrieve(expiringAtBlk)
	if err != nil {
		workingset.err = err
		return workingset
	}
	toPurge := transformToExpiryInfoMap(expiryInfo)
	p.preloadCommittedVersionsInCache(toPurge)
	var expiryInfoKeysToClear []*expiryInfoKey

	for hashedKey, keyAndVersion := range toPurge {
		expiryInfoKeysToClear = append(expiryInfoKeysToClear, &expiryInfoKey{committingBlk: keyAndVersion.committingBlock, expiryBlk: expiringAtBlk})
		currentVersion, err := p.db.GetKeyHashVersion(hashedKey.Namespace, hashedKey.CollectionName, []byte(hashedKey.KeyHash))
		if err != nil {
			workingset.err = err
			return workingset
		}
		if !sameVersion(currentVersion, keyAndVersion.committingBlock) {
			delete(toPurge, hashedKey)
		}
	}
	workingset.toPurge = toPurge
	workingset.toClearFromSchedule = expiryInfoKeysToClear
	return workingset
}

func (p *purgeMgr) preloadCommittedVersionsInCache(expInfoMap expiryInfoMap) {
	if !p.db.IsBulkOptimizable() {
		return
	}
	var hashedKeys []*privacyenabledstate.HashedCompositeKey
	for k := range expInfoMap {
		hashedKeys = append(hashedKeys, k)
	}
	p.db.LoadCommittedVersionsOfPubAndHashedKeys(nil, hashedKeys)
}

func transformToExpiryInfoMap(expiryInfo []*expiryInfo) expiryInfoMap {
	var expinfoMap expiryInfoMap = make(map[*privacyenabledstate.HashedCompositeKey]*keyAndVersion)
	for _, expinfo := range expiryInfo {
		for ns, colls := range expinfo.pvtdataKeys.Map {
			for coll, keysAndHashes := range colls.Map {
				for _, keyAndHash := range keysAndHashes.List {
					compositeKey := &privacyenabledstate.HashedCompositeKey{Namespace: ns, CollectionName: coll, KeyHash: string(keyAndHash.Hash)}
					expinfoMap[compositeKey] = &keyAndVersion{key: keyAndHash.Key, committingBlock: expinfo.expiryInfoKey.committingBlk}
				}
			}
		}
	}
	return expinfoMap
}

func sameVersion(version *version.Height, blockNum uint64) bool {
	return version != nil && version.BlockNum == blockNum
}
