/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"strings"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/core/common/ccprovider"
	"github.com/hyperledger/fabric/core/ledger/cceventmgmt"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	cb "github.com/hyperledger/fabric/protos/common"
	fabriccmn "github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/ledger/rwset/kvrwset"
	pb "github.com/hyperledger/fabric/protos/peer"
	utils "github.com/hyperledger/fabric/protos/utils"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

const (
	collectionSeparator              = "~"
	transientBlockRetentionConfigKey = "peer.gossip.pvtData.transientstoreMaxBlockRetention"
	transientBlockRetentionDefault   = 1000
)

// BlockPublisher is used for endorser-only peers to notify all interested
// consumers of the new block
type BlockPublisher interface {
	AddBlock(block *cb.Block) error
	CheckpointBlock(block *cb.Block) error
}
type privateDataPurge interface {
	PurgeByTxids(txids []string) error
	PurgeByHeight(maxBlockNumToRetain uint64) error
}

type publisher struct {
	channelID               string
	bp                      BlockPublisher
	pvtDataPurge            privateDataPurge
	blockNumber             uint64
	transientBlockRetention uint64
	mutex                   sync.RWMutex
}

func newBlockPublisher(channelID string, bp BlockPublisher, pvtDataPurge privateDataPurge, ledgerHeight uint64) *publisher {
	if ledgerHeight == 0 {
		panic("Ledger height must be greater than 0")
	}

	logger.Infof("Initializing ledger height to %d for channel [%s]: %s", ledgerHeight, channelID)

	transientBlockRetention := uint64(viper.GetInt(transientBlockRetentionConfigKey))
	if transientBlockRetention == 0 {
		logger.Warning("Configuration key", transientBlockRetentionConfigKey, "isn't set, defaulting to", transientBlockRetentionDefault)
		transientBlockRetention = transientBlockRetentionDefault
	}

	return &publisher{
		channelID:               channelID,
		bp:                      bp,
		pvtDataPurge:            pvtDataPurge,
		blockNumber:             ledgerHeight - 1,
		transientBlockRetention: transientBlockRetention,
	}
}

// Publish notifies all interested consumers of the new block.
// Note: This function should only be used for endorser-only peers.
func (p *publisher) Publish(block *fabriccmn.Block) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if block.Header.Number != p.blockNumber+1 {
		return errors.Errorf("expecting block %d for channel [%s] but got block %d", p.blockNumber+1, p.channelID, block.Header.Number)
	}

	var pvtDataTxIDs []string
	for i := range block.Data.Data {
		envelope, err := utils.ExtractEnvelope(block, i)
		if err != nil {
			return err
		}
		if txID, hasPvtData, err := p.checkEnvelope(envelope); err != nil {
			logger.Warningf("Error checking envelope at index %d in block %d and channel [%s]: %s", i, block.Header.Number, p.channelID, err)
		} else if hasPvtData {
			logger.Debugf("Adding private data TxID %s for index %d in block %d and channel [%s]", txID, i, block.Header.Number, p.channelID)
			pvtDataTxIDs = append(pvtDataTxIDs, txID)
		}
	}

	err := p.bp.AddBlock(block)
	if err != nil {
		return err
	}

	p.blockNumber = block.Header.Number

	logger.Debugf("[%s] Broadcasting checkpoint for block [%d]", p.channelID, block.Header.Number)
	if err := p.bp.CheckpointBlock(block); err != nil {
		logger.Errorf("[%s] Error setting checkpoint for block [%d]: %s", p.channelID, block.Header.Number, err)
	}

	logger.Infof("Updated ledger height to %d for channel [%s]", p.blockNumber+1, p.channelID)

	if len(pvtDataTxIDs) > 0 || (block.Header.Number%p.transientBlockRetention == 0 && block.Header.Number > p.transientBlockRetention) {
		go p.purgePrivateTransientData(block.Header.Number, pvtDataTxIDs)
	}

	return nil
}

func (p *publisher) purgePrivateTransientData(blockNum uint64, pvtDataTxIDs []string) {
	maxBlockNumToRetain := blockNum - p.transientBlockRetention
	if len(pvtDataTxIDs) > 0 {
		// Purge all transactions in block - valid or not valid.
		logger.Debugf("Purging transient private data for transactions %s ...", pvtDataTxIDs)
		if err := p.pvtDataPurge.PurgeByTxids(pvtDataTxIDs); err != nil {
			logger.Errorf("Purging transient private data for transactions %s failed: %s", pvtDataTxIDs, err)
		} else {
			logger.Debugf("Purging transient private data for transactions %s succeeded", pvtDataTxIDs)
		}
	}

	if blockNum%p.transientBlockRetention == 0 && blockNum > p.transientBlockRetention {
		logger.Debugf("Purging transient private data with maxBlockNumToRetain [%d]...", maxBlockNumToRetain)
		if err := p.pvtDataPurge.PurgeByHeight(maxBlockNumToRetain); err != nil {
			logger.Errorf("Failed purging data from transient store with maxBlockNumToRetain [%d]: %s", maxBlockNumToRetain, err)
		} else {
			logger.Debugf("... finished running PurgeByHeight with maxBlockNumToRetain [%d]", maxBlockNumToRetain)
		}
	}
}

func (p *publisher) getPvtDataTxIDs(block *fabriccmn.Block) ([]string, error) {
	return nil, nil
}

func (p *publisher) LedgerHeight() uint64 {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.blockNumber + 1
}

func (p *publisher) checkEnvelope(envelope *fabriccmn.Envelope) (string, bool, error) {
	payload, err := utils.ExtractPayload(envelope)
	if err != nil {
		return "", false, err
	}

	chdr, err := utils.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return "", false, err
	}

	if fabriccmn.HeaderType(chdr.Type) != fabriccmn.HeaderType_ENDORSER_TRANSACTION {
		return chdr.TxId, false, nil
	}

	hasPvtData, err := p.checkData(payload.Data)
	if err != nil {
		return "", false, err
	}
	return chdr.TxId, hasPvtData, nil
}

func (p *publisher) checkData(data []byte) (bool, error) {
	tx, err := utils.GetTransaction(data)
	if err != nil {
		return false, err
	}
	return p.checkTransaction(tx), nil
}

func (p *publisher) checkTransaction(tx *pb.Transaction) bool {
	var hasPrivateData bool
	for i, action := range tx.Actions {
		hasPvtData, err := p.checkTXAction(action)
		if err != nil {
			logger.Warningf("Error checking TxAction at index %d: %s", i, err)
		} else if hasPvtData {
			hasPrivateData = true
		}
	}
	return hasPrivateData
}

func (p *publisher) checkTXAction(action *pb.TransactionAction) (bool, error) {
	chaPayload, err := utils.GetChaincodeActionPayload(action.Payload)
	if err != nil {
		return false, err
	}
	return p.checkChaincodeActionPayload(chaPayload)
}

func (p *publisher) checkChaincodeActionPayload(chaPayload *pb.ChaincodeActionPayload) (bool, error) {
	cpp := &pb.ChaincodeProposalPayload{}
	err := proto.Unmarshal(chaPayload.ChaincodeProposalPayload, cpp)
	if err != nil {
		return false, err
	}

	return p.checkAction(chaPayload.Action)
}

func (p *publisher) checkAction(action *pb.ChaincodeEndorsedAction) (bool, error) {
	prp := &pb.ProposalResponsePayload{}
	err := proto.Unmarshal(action.ProposalResponsePayload, prp)
	if err != nil {
		return false, err
	}
	return p.checkProposalResponsePayload(prp)
}

func (p *publisher) checkProposalResponsePayload(prp *pb.ProposalResponsePayload) (bool, error) {
	chaincodeAction := &pb.ChaincodeAction{}
	err := proto.Unmarshal(prp.Extension, chaincodeAction)
	if err != nil {
		return false, err
	}
	return p.checkChaincodeAction(chaincodeAction)
}

func (p *publisher) checkChaincodeAction(chaincodeAction *pb.ChaincodeAction) (bool, error) {
	if len(chaincodeAction.Results) == 0 {
		return false, nil
	}
	txRWSet := &rwsetutil.TxRwSet{}
	if err := txRWSet.FromProtoBytes(chaincodeAction.Results); err != nil {
		return false, err
	}
	return p.checkTxReadWriteSet(txRWSet), nil
}

func (p *publisher) checkTxReadWriteSet(txRWSet *rwsetutil.TxRwSet) bool {
	var hasPrivateData bool
	for _, nsRWSet := range txRWSet.NsRwSets {
		if p.checkNsReadWriteSet(nsRWSet) {
			hasPrivateData = true
		}
	}
	return hasPrivateData
}

func (p *publisher) checkNsReadWriteSet(nsRWSet *rwsetutil.NsRwSet) bool {
	var hasPrivateData bool
	for _, w := range nsRWSet.KvRwSet.Writes {
		if nsRWSet.NameSpace == "lscc" {
			if err := p.handleStateUpdate(w); err != nil {
				logger.Warningf("Error handling state update for key [%s]: %s", w.Key, err)
			}
		}
		if len(nsRWSet.CollHashedRwSets) > 0 {
			hasPrivateData = true
		}
	}
	return hasPrivateData
}

func (p *publisher) handleStateUpdate(kvWrite *kvrwset.KVWrite) error {
	// There are LSCC entries for the chaincode and for the chaincode collections.
	// We need to ignore changes to chaincode collections, and handle changes to chaincode
	// We can detect collections based on the presence of a CollectionSeparator, which never exists in chaincode names
	if isCollectionConfigKey(kvWrite.Key) {
		return nil
	}
	// Ignore delete events
	if kvWrite.IsDelete {
		return nil
	}

	// Chaincode instantiate/upgrade is not logged on committing peer anywhere else.  This is a good place to log it.
	logger.Debugf("Handling LSCC state update for chaincode [%s] on channel [%s]", kvWrite.Key, p.channelID)
	chaincodeData := &ccprovider.ChaincodeData{}
	if err := proto.Unmarshal(kvWrite.Value, chaincodeData); err != nil {
		return errors.Errorf("Unmarshalling ChaincodeQueryResponse failed, error %s", err)
	}

	chaincodeDefs := []*cceventmgmt.ChaincodeDefinition{}
	chaincodeDefs = append(chaincodeDefs, &cceventmgmt.ChaincodeDefinition{Name: chaincodeData.CCName(), Version: chaincodeData.CCVersion(), Hash: chaincodeData.Hash()})

	err := cceventmgmt.GetMgr().HandleChaincodeDeploy(p.channelID, chaincodeDefs)
	if err != nil {
		return err
	}

	cceventmgmt.GetMgr().ChaincodeDeployDone(p.channelID)

	return nil
}

// isCollectionConfigKey detects if a key is a collection key
func isCollectionConfigKey(key string) bool {
	return strings.Contains(key, collectionSeparator)
}
