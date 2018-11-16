/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package kvledger

import (
	"encoding/base64"

	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/customtx"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/txmgr"
	"github.com/hyperledger/fabric/core/ledger/util"
	ledgerUtil "github.com/hyperledger/fabric/core/ledger/util"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/ledger/rwset"
	"github.com/hyperledger/fabric/protos/utils"
)

func (l *kvLedger) cacheBlock(pvtdataAndBlock *ledger.BlockAndPvtData) error {
	block := pvtdataAndBlock.Block
	pvtData := pvtdataAndBlock.BlockPvtData

	validatedTxOps, pvtDataHashedKeys, txValidationFlags, err := l.getKVFromBlock(block)
	if err != nil {
		return err
	}
	pvtDataKeys, err := getPrivateDataKV(block.Header.Number, l.ledgerID, pvtData, txValidationFlags)
	if err != nil {
		return err
	}

	// Update the cache
	err = statedb.UpdateKVCache(validatedTxOps, pvtDataKeys, pvtDataHashedKeys, l.ledgerID)
	if err != nil {
		return err
	}

	return nil
}

func (l *kvLedger) getKVFromBlock(block *common.Block) ([]statedb.ValidatedTxOp, []statedb.ValidatedPvtData, util.TxValidationFlags, error) {
	validatedTxOps := make([]statedb.ValidatedTxOp, 0)
	pvtHashedKeys := make([]statedb.ValidatedPvtData, 0)
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
	return validatedTxOps, pvtHashedKeys, txsFilter, nil
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

func getPrivateDataKV(blockNumber uint64, chId string, pvtData map[uint64]*ledger.TxPvtData, txValidationFlags util.TxValidationFlags) ([]statedb.ValidatedPvtData, error) {
	pvtKeys := make([]statedb.ValidatedPvtData, 0)
	for _, txPvtdata := range pvtData {
		if txValidationFlags.IsValid(int(txPvtdata.SeqInBlock)) {
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
						logger.Infof("*** cache pvt data key key %s blockNumber %d", pvtKeys, blockNumber)
						pvtKeys = append(pvtKeys,
							statedb.ValidatedPvtData{ValidatedTxOp: statedb.ValidatedTxOp{ValidatedTx: statedb.ValidatedTx{Key: write.Key, Value: write.Value, BlockNum: blockNumber, IndexInBlock: int(txnum)},
								IsDeleted: write.IsDelete, Namespace: ns, ChId: chId}, Collection: coll})
					}

				}
			}
		}
	}
	return pvtKeys, nil
}
