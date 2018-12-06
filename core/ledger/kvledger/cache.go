/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package kvledger

import (
	"encoding/base64"
	"fmt"
	"math"

	"github.com/hyperledger/fabric/common/metrics"

	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/customtx"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/txmgr"

	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/core/ledger/pvtdatapolicy"
	"github.com/hyperledger/fabric/core/ledger/pvtdatastorage"
	"github.com/hyperledger/fabric/core/ledger/pvtdatastorage/mempvtdatacache"

	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb/kvcache"
	"github.com/hyperledger/fabric/core/ledger/util"
	ledgerUtil "github.com/hyperledger/fabric/core/ledger/util"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/ledger/rwset"
	"github.com/hyperledger/fabric/protos/utils"
)

//indexUpdate contains index updates to be applied
type indexUpdate struct {
	TxOps       []kvcache.ValidatedTxOp
	PvtData     []kvcache.ValidatedPvtData
	PvtHashData []kvcache.ValidatedPvtData
}

func (l *kvLedger) cacheNonDurableBlock(pvtdataAndBlock *ledger.BlockAndPvtData) error {

	stopWatch := metrics.StopWatch("kvledger_cacheNonDurableBlock")
	defer stopWatch()

	block := pvtdataAndBlock.Block
	pvtData := pvtdataAndBlock.BlockPvtData
	logger.Debugf("*** cacheNonDurableBlock %d channelID %s\n", block.Header.Number, l.ledgerID)

	btlPolicy := pvtdatapolicy.NewBTLPolicy(l)
	_, pvtDataHashedKeys, txValidationFlags, err := l.getKVFromBlock(block, btlPolicy)
	if err != nil {
		return err
	}

	pvtDataKeys, _, err := getPrivateDataKV(block.Header.Number, l.ledgerID, pvtData, txValidationFlags, btlPolicy)
	if err != nil {
		return err
	}

	l.txtmgmt.Lock()
	defer l.txtmgmt.Unlock()

	// Update the 'non-durable' cache only
	l.kvCacheProvider.UpdateNonDurableKVCache(block.Header.Number, pvtDataKeys, pvtDataHashedKeys)

	return nil
}

func (l *kvLedger) cacheBlock(pvtdataAndBlock *ledger.BlockAndPvtData) (*indexUpdate, error) {

	stopWatch := metrics.StopWatch("kvledger_cacheBlock")
	defer stopWatch()

	block := pvtdataAndBlock.Block
	pvtData := pvtdataAndBlock.BlockPvtData
	logger.Debugf("*** cacheBlock %d channelID %s\n", block.Header.Number, l.ledgerID)

	btlPolicy := pvtdatapolicy.NewBTLPolicy(l)
	validatedTxOps, pvtDataHashedKeys, txValidationFlags, err := l.getKVFromBlock(block, btlPolicy)
	if err != nil {
		return nil, err
	}

	pvtDataKeys, validPvtData, err := getPrivateDataKV(block.Header.Number, l.ledgerID, pvtData, txValidationFlags, btlPolicy)

	if err != nil {
		return nil, err
	}

	//Cache update with pinning
	l.txtmgmt.Lock()
	l.kvCacheProvider.UpdateKVCache(block.Header.Number, validatedTxOps, pvtDataKeys, pvtDataHashedKeys, true)
	//update local block chain info
	l.updateBlockchainInfo(pvtdataAndBlock.Block)
	l.txtmgmt.Unlock()

	if !ledgerconfig.IsCommitter() {
		// Update pvt data cache
		pvtCache, err := getPrivateDataCache(l.ledgerID)
		if err != nil {
			return nil, err
		}
		err = pvtCache.Prepare(block.Header.Number, validPvtData)
		if err != nil {
			return nil, err
		}
		err = pvtCache.Commit(block.Header.Number)
		if err != nil {
			return nil, err
		}
	}
	return &indexUpdate{validatedTxOps, pvtDataKeys, pvtDataHashedKeys}, nil
}

func (l *kvLedger) getKVFromBlock(block *common.Block, btlPolicy pvtdatapolicy.BTLPolicy) ([]kvcache.ValidatedTxOp, []kvcache.ValidatedPvtData, util.TxValidationFlags, error) {
	validatedTxOps := make([]kvcache.ValidatedTxOp, 0)
	pvtHashedKeys := make([]kvcache.ValidatedPvtData, 0)
	txsFilter := ledgerUtil.TxValidationFlags(block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER])
	for txIndex, envBytes := range block.Data.Data {
		var env *common.Envelope
		var chdr *common.ChannelHeader
		var err error
		if env, err = utils.GetEnvelopeFromBlock(envBytes); err == nil {
			if payload, err := utils.GetPayload(env); err == nil {
				chdr, err = utils.UnmarshalChannelHeader(payload.Header.ChannelHeader)
			}
		}
		if txsFilter.IsInvalid(txIndex) {
			// Skipping invalid transaction
			logger.Warningf("Channel [%s]: Block [%d] Transaction index [%d] TxId [%s]"+
				" marked as invalid by committer will not add to cache. Reason code [%s]",
				chdr.GetChannelId(), block.Header.Number, txIndex, chdr.GetTxId(),
				txsFilter.Flag(txIndex).String())
			continue
		}
		if err != nil {
			return nil, nil, nil, err
		}

		var txRWSet *rwsetutil.TxRwSet
		txType := common.HeaderType(chdr.Type)
		logger.Debugf("txType=%s", txType)
		if txType == common.HeaderType_ENDORSER_TRANSACTION {
			// extract actions from the envelope message
			respPayload, err := utils.GetActionFromEnvelope(envBytes)
			if err != nil {
				logger.Warningf("Channel [%s]: Block [%d] Transaction index [%d] TxId [%s]"+
					" GetActionFromEnvelope return error will not add to cache. Reason code [%s]",
					chdr.GetChannelId(), block.Header.Number, txIndex, chdr.GetTxId(), err.Error())
				continue
			}
			txRWSet = &rwsetutil.TxRwSet{}
			if err = txRWSet.FromProtoBytes(respPayload.Results); err != nil {
				logger.Warningf("Channel [%s]: Block [%d] Transaction index [%d] TxId [%s]"+
					" FromProtoBytes return error will not add to cache. Reason code [%s]",
					chdr.GetChannelId(), block.Header.Number, txIndex, chdr.GetTxId(), err.Error())
				continue
			}
		} else {
			rwsetProto, err := processNonEndorserTx(env, chdr.TxId, txType, l.txtmgmt)
			if _, ok := err.(*customtx.InvalidTxError); ok {
				continue
			}
			if err != nil {
				return nil, nil, nil, err
			}
			if rwsetProto != nil {
				if txRWSet, err = rwsetutil.TxRwSetFromProtoMsg(rwsetProto); err != nil {
					return nil, nil, nil, err
				}
			}
		}
		if txRWSet != nil {
			for _, nsRwSet := range txRWSet.NsRwSets {
				if nsRwSet == nil {
					continue
				}
				pubWriteset := nsRwSet.KvRwSet
				if pubWriteset == nil {
					continue
				}

				for _, kvwrite := range pubWriteset.Writes {
					validatedTxOps = append(validatedTxOps,
						kvcache.ValidatedTxOp{ValidatedTx: kvcache.ValidatedTx{Key: kvwrite.Key, Value: kvwrite.Value, BlockNum: block.Header.Number, IndexInBlock: txIndex},
							IsDeleted: kvwrite.IsDelete, Namespace: nsRwSet.NameSpace, ChId: chdr.ChannelId})
				}
				for _, collHashedRwSets := range nsRwSet.CollHashedRwSets {
					for _, hashedWrite := range collHashedRwSets.HashedRwSet.HashedWrites {
						btl, err := btlPolicy.GetBTL(nsRwSet.NameSpace, collHashedRwSets.CollectionName)
						if err != nil {
							return nil, nil, nil, err
						}
						pvtHashedKeys = append(pvtHashedKeys,
							kvcache.ValidatedPvtData{ValidatedTxOp: kvcache.ValidatedTxOp{ValidatedTx: kvcache.ValidatedTx{Key: base64.StdEncoding.EncodeToString(hashedWrite.KeyHash),
								Value: hashedWrite.ValueHash, BlockNum: block.Header.Number, IndexInBlock: txIndex},
								IsDeleted: hashedWrite.IsDelete, Namespace: nsRwSet.NameSpace, ChId: chdr.ChannelId}, Collection: collHashedRwSets.CollectionName,
								Level1ExpiringBlock: getFirstLevelCacheExpiryBlock(block.Header.Number, btl),
								Level2ExpiringBlock: getSecondLevelCacheExpiryBlock(block.Header.Number, btl), PolicyBTL: btl})
					}
				}
			}
		}
	}
	return validatedTxOps, pvtHashedKeys, txsFilter, nil
}

//indexWriter worker thread which looks for index updates to be applied to db
func (l *kvLedger) indexWriter(notifyIndexReady chan struct{}) {
	for {
		select {
		case <-l.doneCh:
			close(l.stoppedIndexCh)
			return
		case indexUpdate := <-l.indexCh:
			stopWatch := metrics.StopWatch("kvledger_indexUpdate")
			allUpdates, indexDeletes := l.kvCacheProvider.PrepareIndexUpdates(indexUpdate.TxOps, indexUpdate.PvtData, indexUpdate.PvtHashData)
			l.txtmgmt.RLock()
			err := l.kvCacheProvider.ApplyIndexUpdates(allUpdates, indexDeletes, l.ledgerID)
			l.txtmgmt.RUnlock()
			if err != nil {
				logger.Errorf("Failed to apply index updates in db for ledger[%s] : %s", l.ledgerID, err)
				stopWatch()
				panic(fmt.Sprintf("%s", err))
			}
			stopWatch()
			if ledgerconfig.IsCommitter() {
				logger.Debug("notifying completion of statekeyindex updates")
				notifyIndexReady <- struct{}{}
			}
		}
	}
}

//updateBlockchainInfo updates local kvledger blockchain info
func (l *kvLedger) updateBlockchainInfo(block *common.Block) {
	hash := block.GetHeader().Hash()
	number := block.GetHeader().GetNumber()
	l.bcInfo = &common.BlockchainInfo{
		Height:            number + 1,
		CurrentBlockHash:  hash,
		PreviousBlockHash: block.Header.PreviousHash,
	}
}

func processNonEndorserTx(txEnv *common.Envelope, txid string, txType common.HeaderType, txmgr txmgr.TxMgr) (*rwset.TxReadWriteSet, error) {
	logger.Debugf("Performing custom processing for transaction [txid=%s], [txType=%s]", txid, txType)
	processor := customtx.GetProcessor(txType)
	logger.Debugf("Processor for custom tx processing:%#v", processor)
	if processor == nil {
		return nil, nil
	}

	var err error
	var sim ledger.TxSimulator
	var simRes *ledger.TxSimulationResults
	if sim, err = txmgr.NewTxSimulator(txid); err != nil {
		return nil, err
	}
	defer sim.Done()
	if err = processor.GenerateSimulationResults(txEnv, sim, false); err != nil {
		return nil, err
	}
	if simRes, err = sim.GetTxSimulationResults(); err != nil {
		return nil, err
	}
	return simRes.PubSimulationResults, nil
}

func getPrivateDataKV(blockNumber uint64, chId string, pvtData map[uint64]*ledger.TxPvtData, txValidationFlags util.TxValidationFlags, btlPolicy pvtdatapolicy.BTLPolicy) ([]kvcache.ValidatedPvtData, []*ledger.TxPvtData, error) {

	pvtKeys := make([]kvcache.ValidatedPvtData, 0)
	validPvtData := make([]*ledger.TxPvtData, 0)
	for _, txPvtdata := range pvtData {
		if txValidationFlags.IsValid(int(txPvtdata.SeqInBlock)) {
			validPvtData = append(validPvtData, txPvtdata)
			pvtRWSet, err := rwsetutil.TxPvtRwSetFromProtoMsg(txPvtdata.WriteSet)
			if err != nil {
				return nil, nil, err
			}
			for _, nsPvtdata := range pvtRWSet.NsPvtRwSet {
				for _, collPvtRwSets := range nsPvtdata.CollPvtRwSets {
					txnum := txPvtdata.SeqInBlock
					ns := nsPvtdata.NameSpace
					coll := collPvtRwSets.CollectionName
					btl, err := btlPolicy.GetBTL(ns, coll)
					if err != nil {
						return nil, nil, err
					}
					for _, write := range collPvtRwSets.KvRwSet.Writes {
						pvtKeys = append(pvtKeys,
							kvcache.ValidatedPvtData{ValidatedTxOp: kvcache.ValidatedTxOp{ValidatedTx: kvcache.ValidatedTx{Key: write.Key, Value: write.Value, BlockNum: blockNumber, IndexInBlock: int(txnum)},
								IsDeleted: write.IsDelete, Namespace: ns, ChId: chId}, Collection: coll,
								Level1ExpiringBlock: getFirstLevelCacheExpiryBlock(blockNumber, btl),
								Level2ExpiringBlock: getSecondLevelCacheExpiryBlock(blockNumber, btl),
								PolicyBTL:           btl})
					}
				}
			}
		}
	}
	return pvtKeys, validPvtData, nil
}

func getPrivateDataCache(chID string) (pvtdatastorage.Store, error) {
	p := mempvtdatacache.NewProvider(ledgerconfig.GetPvtDataCacheSize())
	var err error
	pvtCache, err := p.OpenStore(chID)
	if err != nil {
		return nil, err
	}
	return pvtCache, nil
}

func min(x, y uint64) uint64 {
	if x < y {
		return x
	}
	return y
}

func getFirstLevelCacheExpiryBlock(blockNum, policyBTL uint64) uint64 {

	btl := policyBTL
	if policyBTL == 0 {
		btl = math.MaxUint64
	}

	return blockNum + min(ledgerconfig.GetKVCacheBlocksToLive(), btl) + 1
}

func getSecondLevelCacheExpiryBlock(blockNum, policyBTL uint64) uint64 {

	if policyBTL == 0 || policyBTL == math.MaxUint64 || blockNum > math.MaxUint64-(policyBTL+1) {
		return math.MaxUint64
	}

	return blockNum + policyBTL + 1
}
