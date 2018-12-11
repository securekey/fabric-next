/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package memtransientdata

import (
	"bytes"
	"errors"

	"github.com/hyperledger/fabric/common/ledger/util"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/ledger/rwset"
)

var (
	prwsetPrefix    = []byte("P")[0] // key prefix for storing private write set in transient store.
	compositeKeySep = byte(0x00)
)

// createCompositeKeyForPvtRWSet creates a key for storing private write set
// in the transient store. The structure of the key is <prwsetPrefix>~txid~uuid~blockHeight.
func createCompositeKeyForPvtRWSet(txid string, uuid string, blockHeight uint64) []byte {
	var compositeKey []byte
	compositeKey = append(compositeKey, prwsetPrefix)
	compositeKey = append(compositeKey, compositeKeySep)
	compositeKey = append(compositeKey, createCompositeKeyWithoutPrefixForTxid(txid, uuid, blockHeight)...)

	return compositeKey
}

// createCompositeKeyWithoutPrefixForTxid creates a composite key of structure txid~uuid~blockHeight.
func createCompositeKeyWithoutPrefixForTxid(txid string, uuid string, blockHeight uint64) []byte {
	var compositeKey []byte
	compositeKey = append(compositeKey, []byte(txid)...)
	compositeKey = append(compositeKey, compositeKeySep)
	compositeKey = append(compositeKey, []byte(uuid)...)
	compositeKey = append(compositeKey, compositeKeySep)
	compositeKey = append(compositeKey, util.EncodeOrderPreservingVarUint64(blockHeight)...)

	return compositeKey
}

// splitCompositeKeyOfPvtRWSet splits the compositeKey (<prwsetPrefix>~txid~uuid~blockHeight)
// into uuid and blockHeight.
func splitCompositeKeyOfPvtRWSet(compositeKey []byte) (uuid string, blockHeight uint64) {
	return splitCompositeKeyWithoutPrefixForTxid(compositeKey[2:])
}

// splitCompositeKeyWithoutPrefixForTxid splits the composite key txid~uuid~blockHeight into
// uuid and blockHeight
func splitCompositeKeyWithoutPrefixForTxid(compositeKey []byte) (uuid string, blockHeight uint64) {
	// skip txid as all functions which requires split of composite key already has it
	firstSepIndex := bytes.IndexByte(compositeKey, compositeKeySep)
	secondSepIndex := firstSepIndex + bytes.IndexByte(compositeKey[firstSepIndex+1:], compositeKeySep) + 1
	uuid = string(compositeKey[firstSepIndex+1 : secondSepIndex])
	blockHeight, _ = util.DecodeOrderPreservingVarUint64(compositeKey[secondSepIndex+1:])
	return
}

// trimPvtWSet returns a `TxPvtReadWriteSet` that retains only list of 'ns/collections' supplied in the filter
// A nil filter does not filter any results and returns the original `pvtWSet` as is
func trimPvtWSet(pvtWSet *rwset.TxPvtReadWriteSet, filter ledger.PvtNsCollFilter) *rwset.TxPvtReadWriteSet {
	if filter == nil {
		return pvtWSet
	}

	var filteredNsRwSet []*rwset.NsPvtReadWriteSet
	for _, ns := range pvtWSet.NsPvtRwset {
		var filteredCollRwSet []*rwset.CollectionPvtReadWriteSet
		for _, coll := range ns.CollectionPvtRwset {
			if filter.Has(ns.Namespace, coll.CollectionName) {
				filteredCollRwSet = append(filteredCollRwSet, coll)
			}
		}
		if filteredCollRwSet != nil {
			filteredNsRwSet = append(filteredNsRwSet,
				&rwset.NsPvtReadWriteSet{
					Namespace:          ns.Namespace,
					CollectionPvtRwset: filteredCollRwSet,
				},
			)
		}
	}
	var filteredTxPvtRwSet *rwset.TxPvtReadWriteSet
	if filteredNsRwSet != nil {
		filteredTxPvtRwSet = &rwset.TxPvtReadWriteSet{
			DataModel:  pvtWSet.GetDataModel(),
			NsPvtRwset: filteredNsRwSet,
		}
	}
	return filteredTxPvtRwSet
}

func trimPvtCollectionConfigs(configs map[string]*common.CollectionConfigPackage,
	filter ledger.PvtNsCollFilter) (map[string]*common.CollectionConfigPackage, error) {
	if filter == nil {
		return configs, nil
	}
	result := make(map[string]*common.CollectionConfigPackage)

	for ns, pkg := range configs {
		result[ns] = &common.CollectionConfigPackage{}
		for _, colConf := range pkg.GetConfig() {
			switch cconf := colConf.Payload.(type) {
			case *common.CollectionConfig_StaticCollectionConfig:
				if filter.Has(ns, cconf.StaticCollectionConfig.Name) {
					result[ns].Config = append(result[ns].Config, colConf)
				}
			default:
				return nil, errors.New("unexpected collection type")
			}
		}
	}
	return result, nil
}
