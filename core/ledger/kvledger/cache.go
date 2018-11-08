/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package kvledger

import (
	"encoding/base64"

	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/customtx"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/privacyenabledstate"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb/statekeyindex"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/txmgr"
	ledgerUtil "github.com/hyperledger/fabric/core/ledger/util"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/ledger/rwset"
	"github.com/hyperledger/fabric/protos/peer"
	"github.com/hyperledger/fabric/protos/utils"
)

func (l *kvLedger) cacheBlock(pvtdataAndBlock *ledger.BlockAndPvtData) error {
	block := pvtdataAndBlock.Block
	pvtData := pvtdataAndBlock.BlockPvtData

	validatedTxOps, pvtDataHashedKeys, err := l.getKVFromBlock(block)
	if err != nil {
		return err
	}
	pvtDataKeys, err := getPrivateDataKV(block.Header.Number, l.ledgerID, pvtData)
	if err != nil {
		return err
	}
	pvtDataKeys = append(pvtDataKeys, pvtDataHashedKeys...)

	// Update the cache
	statedb.UpdateKVCache(validatedTxOps, pvtDataKeys)

	indexKeys := make([]statedb.CompositeKey, 0)
	deletedIndexKeys := make([]statedb.CompositeKey, 0)
	// Add key index for KV
	for _, v := range validatedTxOps {
		if v.IsDeleted {
			deletedIndexKeys = append(deletedIndexKeys, statedb.CompositeKey{Key: v.Key, Namespace: v.Namespace})
		} else {
			indexKeys = append(indexKeys, statedb.CompositeKey{Key: v.Key, Namespace: v.Namespace})
		}
	}
	// Add key index for pvt
	for _, v := range pvtDataKeys {
		if v.IsDeleted {
			deletedIndexKeys = append(deletedIndexKeys, statedb.CompositeKey{Key: v.Key, Namespace: privacyenabledstate.DerivePvtDataNs(v.Namespace, v.Collection)})
		} else {
			indexKeys = append(indexKeys, statedb.CompositeKey{Key: v.Key, Namespace: privacyenabledstate.DerivePvtDataNs(v.Namespace, v.Collection)})
		}
	}

	// Add key index in leveldb
	if len(indexKeys) > 0 {
		stateKeyIndex, err := statekeyindex.NewProvider().OpenStateKeyIndex(l.ledgerID)
		if err != nil {
			return err
		}
		err = stateKeyIndex.AddIndex(indexKeys)
		if err != nil {
			return err
		}
	}

	// Delete key index in leveldb
	if len(deletedIndexKeys) > 0 {
		stateKeyIndex, err := statekeyindex.NewProvider().OpenStateKeyIndex(l.ledgerID)
		if err != nil {
			return err
		}
		err = stateKeyIndex.DeleteIndex(deletedIndexKeys)
		if err != nil {
			return err
		}
	}

	return nil
}

func (l *kvLedger) getKVFromBlock(block *common.Block) ([]statedb.ValidatedTxOp, []statedb.ValidatedPvtData, error) {
	validatedTxOps := make([]statedb.ValidatedTxOp, 0)
	pvtHashedKeys := make([]statedb.ValidatedPvtData, 0)

	for txIndex, envBytes := range block.Data.Data {
		var env *common.Envelope
		var chdr *common.ChannelHeader
		var err error
		txsFilter := ledgerUtil.TxValidationFlags(block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER])
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
			return nil, nil, err
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
				txsFilter.SetFlag(txIndex, peer.TxValidationCode_INVALID_OTHER_REASON)
				logger.Warningf("Channel [%s]: Block [%d] Transaction index [%d] TxId [%s]"+
					" FromProtoBytes return error will not add to cache. Reason code [%s]",
					chdr.GetChannelId(), block.Header.Number, txIndex, chdr.GetTxId(), err.Error())
				continue
			}
		} else {
			rwsetProto, err := processNonEndorserTx(env, chdr.TxId, txType, l.txtmgmt)
			if _, ok := err.(*customtx.InvalidTxError); ok {
				txsFilter.SetFlag(txIndex, peer.TxValidationCode_INVALID_OTHER_REASON)
				continue
			}
			if err != nil {
				return nil, nil, err
			}
			if rwsetProto != nil {
				if txRWSet, err = rwsetutil.TxRwSetFromProtoMsg(rwsetProto); err != nil {
					return nil, nil, err
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
						statedb.ValidatedTxOp{ValidatedTx: statedb.ValidatedTx{Key: kvwrite.Key, Value: kvwrite.Value, BlockNum: block.Header.Number, IndexInBlock: txIndex},
							IsDeleted: kvwrite.IsDelete, Namespace: nsRwSet.NameSpace, ChId: chdr.ChannelId})
				}
				for _, collHashedRwSets := range nsRwSet.CollHashedRwSets {
					for _, hashedWrite := range collHashedRwSets.HashedRwSet.HashedWrites {
						pvtHashedKeys = append(pvtHashedKeys,
							statedb.ValidatedPvtData{ValidatedTxOp: statedb.ValidatedTxOp{ValidatedTx: statedb.ValidatedTx{Key: base64.StdEncoding.EncodeToString(hashedWrite.KeyHash),
								Value: hashedWrite.ValueHash, BlockNum: block.Header.Number, IndexInBlock: txIndex},
								IsDeleted: hashedWrite.IsDelete, Namespace: nsRwSet.NameSpace, ChId: chdr.ChannelId}, Collection: collHashedRwSets.CollectionName})
					}
				}
			}
		}
	}
	return validatedTxOps, pvtHashedKeys, nil
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

func getPrivateDataKV(blockNumber uint64, chId string, pvtData map[uint64]*ledger.TxPvtData) ([]statedb.ValidatedPvtData, error) {
	pvtKeys := make([]statedb.ValidatedPvtData, 0)
	for _, txPvtdata := range pvtData {
		pvtRWSet, err := rwsetutil.TxPvtRwSetFromProtoMsg(txPvtdata.WriteSet)
		if err != nil {
			return nil, err
		}
		for _, nsPvtdata := range pvtRWSet.NsPvtRwSet {
			for _, collPvtRwSets := range nsPvtdata.CollPvtRwSets {
				txnum := txPvtdata.SeqInBlock
				ns := nsPvtdata.NameSpace
				coll := collPvtRwSets.CollectionName
				for _, write := range collPvtRwSets.KvRwSet.Writes {
					pvtKeys = append(pvtKeys,
						statedb.ValidatedPvtData{ValidatedTxOp: statedb.ValidatedTxOp{ValidatedTx: statedb.ValidatedTx{Key: write.Key, Value: write.Value, BlockNum: blockNumber, IndexInBlock: int(txnum)},
							IsDeleted: write.IsDelete, Namespace: ns, ChId: chId}, Collection: coll})
				}

			}
		}
	}
	return pvtKeys, nil
}
