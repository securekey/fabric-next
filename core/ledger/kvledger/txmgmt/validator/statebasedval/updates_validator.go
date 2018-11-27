/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package statebasedval

import (
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/protos/ledger/rwset/kvrwset"
)

type rangeQueryUpdatesValidator struct {
	rqInfo *kvrwset.RangeQueryInfo
	itr    statedb.ResultsIterator
}

func newRangeQueryUpdatesValidator(rqInfo *kvrwset.RangeQueryInfo, itr statedb.ResultsIterator) *rangeQueryUpdatesValidator {
	return &rangeQueryUpdatesValidator{
		rqInfo: rqInfo,
		itr:    itr,
	}
}

func (v *rangeQueryUpdatesValidator) validate() (bool, error) {
	itr := v.itr
	var result statedb.QueryResult
	var err error
	if result, err = itr.Next(); err != nil {
		return false, err
	}

	if result == nil {
		// No results means that there were no updates in the requested range so return valid
		return true, nil
	}

	versionedKV := result.(*statedb.VersionedKV)
	if versionedKV.Value == nil {
		// FIXME: Change to Debug
		logger.Infof("Returning invalid since key was deleted: %+v", versionedKV)
	} else {
		// FIXME: Change to Debug
		logger.Infof("Returning invalid since there was an update to %+v", versionedKV)
	}
	return false, nil
}
