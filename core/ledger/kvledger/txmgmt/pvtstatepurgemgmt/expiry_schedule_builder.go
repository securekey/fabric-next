/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package pvtstatepurgemgmt

import (
	math "math"

	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/privacyenabledstate"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/pvtdatapolicy"
	"github.com/hyperledger/fabric/core/ledger/util"
)

type expiryScheduleBuilder struct {
	btlPolicy pvtdatapolicy.BTLPolicy
	m         map[expiryInfoKey]*PvtdataKeys
}

func newExpiryScheduleBuilder(btlPolicy pvtdatapolicy.BTLPolicy) *expiryScheduleBuilder {
	return &expiryScheduleBuilder{btlPolicy, make(map[expiryInfoKey]*PvtdataKeys)}
}

func (builder *expiryScheduleBuilder) add(ns, coll, key string, keyHash []byte, versionedValue *statedb.VersionedValue) error {
	committingBlk := versionedValue.Version.BlockNum
	expiryBlk, err := builder.btlPolicy.GetExpiringBlock(ns, coll, committingBlk)
	if err != nil {
		return err
	}
	if isDelete(versionedValue) || neverExpires(expiryBlk) {
		return nil
	}
	expinfoKey := expiryInfoKey{committingBlk: committingBlk, expiryBlk: expiryBlk}
	pvtdataKeys, ok := builder.m[expinfoKey]
	if !ok {
		pvtdataKeys = newPvtdataKeys()
		builder.m[expinfoKey] = pvtdataKeys
	}
	pvtdataKeys.add(ns, coll, key, keyHash)
	return nil
}

func isDelete(versionedValue *statedb.VersionedValue) bool {
	return versionedValue.Value == nil
}

func neverExpires(expiryBlk uint64) bool {
	return expiryBlk == math.MaxUint64
}

func (builder *expiryScheduleBuilder) getExpiryInfo() []*expiryInfo {
	var listExpinfo []*expiryInfo
	for expinfoKey, pvtdataKeys := range builder.m {
		expinfoKeyCopy := expinfoKey
		listExpinfo = append(listExpinfo, &expiryInfo{expiryInfoKey: &expinfoKeyCopy, pvtdataKeys: pvtdataKeys})
	}
	return listExpinfo
}

func buildExpirySchedule(
	btlPolicy pvtdatapolicy.BTLPolicy,
	pvtUpdates *privacyenabledstate.PvtUpdateBatch,
	hashedUpdates *privacyenabledstate.HashedUpdateBatch) ([]*expiryInfo, error) {

	hashedUpdateKeys := hashedUpdates.ToCompositeKeyMap()
	expiryScheduleBuilder := newExpiryScheduleBuilder(btlPolicy)

	for pvtUpdateKey, vv := range pvtUpdates.ToCompositeKeyMap() {
		keyHash := util.ComputeStringHash(pvtUpdateKey.Key)
		if err := expiryScheduleBuilder.add(pvtUpdateKey.Namespace, pvtUpdateKey.CollectionName, pvtUpdateKey.Key, keyHash, vv); err != nil {
			return nil, err
		}
		delete(hashedUpdateKeys, privacyenabledstate.HashedCompositeKey{
			Namespace: pvtUpdateKey.Namespace, CollectionName: pvtUpdateKey.CollectionName, KeyHash: string(keyHash)})
	}

	for hashedUpdateKey, vv := range hashedUpdateKeys {
		if err := expiryScheduleBuilder.add(hashedUpdateKey.Namespace, hashedUpdateKey.CollectionName, "", []byte(hashedUpdateKey.KeyHash), vv); err != nil {
			return nil, err
		}
	}
	return expiryScheduleBuilder.getExpiryInfo(), nil
}
