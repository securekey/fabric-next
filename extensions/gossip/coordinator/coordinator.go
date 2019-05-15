/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package coordinator

import (
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	storeapi "github.com/hyperledger/fabric/extensions/collections/api/store"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/ledger/rwset"
	"github.com/hyperledger/fabric/protos/transientstore"
	"github.com/pkg/errors"
)

var logger = flogging.MustGetLogger("kevlar_gossip_state")

type transientStore interface {
	// PersistWithConfig stores the private write set of a transaction along with the collection config
	// in the transient store based on txid and the block height the private data was received at
	PersistWithConfig(txid string, blockHeight uint64, privateSimulationResultsWithConfig *transientstore.TxPvtReadWriteSetWithConfigInfo) error
}

// Coordinator is the Kevlar Gossip coordinator
type Coordinator struct {
	channelID      string
	transientStore transientStore
	collDataStore  storeapi.Store
}

// New returns a new Coordinator
func New(channelID string, transientStore transientStore, collDataStore storeapi.Store) *Coordinator {
	return &Coordinator{
		channelID:      channelID,
		transientStore: transientStore,
		collDataStore:  collDataStore,
	}
}

// StorePvtData used to persist private date into transient store
func (c *Coordinator) StorePvtData(txID string, privData *transientstore.TxPvtReadWriteSetWithConfigInfo, blkHeight uint64) error {
	transientRWSet, err := c.getTransientRWSets(txID, blkHeight, privData)
	if err != nil {
		logger.Errorf("[%s:%d:%s] Unable to extract transient r/w set from private data: %s", c.channelID, blkHeight, txID, err)
		return err
	}

	// Some of the write-sets may be transient and some not - need to invoke persist on both transient data and regular private data
	err = c.collDataStore.Persist(txID, privData)
	if err != nil {
		logger.Errorf("[%s:%d:%s] Unable to persist collection data: %s", c.channelID, blkHeight, txID, err)
		return err
	}

	if transientRWSet == nil {
		logger.Debugf("[%s:%d:%s] Nothing to persist to transient store", c.channelID, blkHeight, txID)
		return nil
	}

	logger.Debugf("[%s:%d:%s] Persisting private data to transient store", c.channelID, blkHeight, txID)

	return c.transientStore.PersistWithConfig(
		txID, blkHeight,
		&transientstore.TxPvtReadWriteSetWithConfigInfo{
			PvtRwset:          transientRWSet,
			CollectionConfigs: privData.CollectionConfigs,
			EndorsedAt:        privData.EndorsedAt,
		})
}

func (c *Coordinator) getTransientRWSets(txID string, blkHeight uint64, privData *transientstore.TxPvtReadWriteSetWithConfigInfo) (*rwset.TxPvtReadWriteSet, error) {
	txPvtRWSet, err := rwsetutil.TxPvtRwSetFromProtoMsg(privData.PvtRwset)
	if err != nil {
		return nil, errors.New("error getting pvt RW set from bytes")
	}

	nsPvtRwSet, modified, err := c.extractTransientRWSets(txPvtRWSet.NsPvtRwSet, privData.CollectionConfigs, txID, blkHeight)
	if err != nil {
		return nil, err
	}
	if !modified {
		logger.Debugf("[%s:%d:%s] Rewrite to NsPvtRwSet not required for transient store", c.channelID, blkHeight, txID)
		return privData.PvtRwset, nil
	}
	if len(nsPvtRwSet) == 0 {
		logger.Debugf("[%s:%d:%s] Didn't find any private data to persist to transient store", c.channelID, blkHeight, txID)
		return nil, nil
	}

	logger.Debugf("[%s:%d:%s] Rewriting NsPvtRwSet for transient store", c.channelID, blkHeight, txID)
	txPvtRWSet.NsPvtRwSet = nsPvtRwSet
	newPvtRwset, err := txPvtRWSet.ToProtoMsg()
	if err != nil {
		return nil, errors.WithMessage(err, "error marshalling private data r/w set")
	}

	logger.Debugf("[%s:%d:%s] Rewriting private data r/w set since it was modified", c.channelID, blkHeight, txID)
	return newPvtRwset, nil
}

func (c *Coordinator) extractTransientRWSets(srcNsPvtRwSets []*rwsetutil.NsPvtRwSet, collConfigs map[string]*common.CollectionConfigPackage, txID string, blkHeight uint64) ([]*rwsetutil.NsPvtRwSet, bool, error) {
	modified := false
	var nsPvtRwSet []*rwsetutil.NsPvtRwSet
	for _, nsRWSet := range srcNsPvtRwSets {
		var collPvtRwSets []*rwsetutil.CollPvtRwSet
		for _, collRWSet := range nsRWSet.CollPvtRwSets {
			ok, e := isPvtData(collConfigs, nsRWSet.NameSpace, collRWSet.CollectionName)
			if e != nil {
				return nil, false, errors.New("error in collection config")
			}
			if !ok {
				logger.Debugf("[%s:%d:%s] Not persisting collection [%s:%s] in transient store", c.channelID, blkHeight, txID, nsRWSet.NameSpace, collRWSet.CollectionName)
				modified = true
				continue
			}
			logger.Debugf("[%s:%d:%s] Persisting collection [%s:%s] in transient store", c.channelID, blkHeight, txID, nsRWSet.NameSpace, collRWSet.CollectionName)
			collPvtRwSets = append(collPvtRwSets, collRWSet)
		}
		if len(collPvtRwSets) > 0 {
			if len(collPvtRwSets) != len(nsRWSet.CollPvtRwSets) {
				logger.Debugf("[%s:%d:%s] Rewriting collections for [%s] in transient store", c.channelID, blkHeight, txID, nsRWSet.NameSpace)
				nsRWSet.CollPvtRwSets = collPvtRwSets
				modified = true
			} else {
				logger.Debugf("[%s:%d:%s] Not touching collections for [%s] in transient store", c.channelID, blkHeight, txID, nsRWSet.NameSpace)
			}
			logger.Debugf("[%s:%d:%s] Adding NsPvtRwSet for [%s] in transient store", c.channelID, blkHeight, txID, nsRWSet.NameSpace)
			nsPvtRwSet = append(nsPvtRwSet, nsRWSet)
		} else {
			logger.Debugf("[%s:%d:%s] NOT adding NsPvtRwSet for [%s] in transient store since no private data collections found", c.channelID, blkHeight, txID, nsRWSet.NameSpace)
		}
	}
	return nsPvtRwSet, modified, nil
}

func isPvtData(collConfigs map[string]*common.CollectionConfigPackage, ns, coll string) (bool, error) {
	pkg, ok := collConfigs[ns]
	if !ok {
		return false, errors.Errorf("could not find collection configs for namespace [%s]", ns)
	}

	var config *common.StaticCollectionConfig
	for _, c := range pkg.Config {
		staticConfig := c.GetStaticCollectionConfig()
		if staticConfig.Name == coll {
			config = staticConfig
			break
		}
	}

	if config == nil {
		return false, errors.Errorf("could not find collection config for collection [%s:%s]", ns, coll)
	}

	return config.Type == common.CollectionType_COL_UNKNOWN || config.Type == common.CollectionType_COL_PRIVATE, nil
}
