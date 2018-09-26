/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/core/common/ccprovider"
	"github.com/hyperledger/fabric/core/ledger/cceventmgmt"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	fabriccmn "github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/ledger/rwset/kvrwset"
	pb "github.com/hyperledger/fabric/protos/peer"
	utils "github.com/hyperledger/fabric/protos/utils"
	"github.com/pkg/errors"
)

const (
	collectionSeparator = "~"
)

// TODO: Notify block iterator of the new block
func publishBlock(block *fabriccmn.Block) error {
	for i := range block.Data.Data {
		envelope, err := utils.ExtractEnvelope(block, i)
		if err != nil {
			return err
		}
		if err := checkEnvelope(envelope); err != nil {
			logger.Warningf("Error checking envelope at index %d: %s", i, err)
		}
	}
	return nil
}

func checkEnvelope(envelope *fabriccmn.Envelope) error {
	payload, err := utils.ExtractPayload(envelope)
	if err != nil {
		return err
	}
	chdr, err := utils.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return err
	}

	if fabriccmn.HeaderType(chdr.Type) == fabriccmn.HeaderType_ENDORSER_TRANSACTION {
		chUpdater := &publisher{channelID: chdr.ChannelId}
		if err := chUpdater.checkData(payload.Data); err != nil {
			return err
		}
	}
	return nil
}

type publisher struct {
	channelID string
}

func (u *publisher) checkData(data []byte) error {
	tx, err := utils.GetTransaction(data)
	if err != nil {
		return err
	}
	u.checkTransaction(tx)
	return nil
}

func (u *publisher) checkTransaction(tx *pb.Transaction) {
	for i, action := range tx.Actions {
		if err := u.checkTXAction(action); err != nil {
			logger.Warningf("Error checking TxAction at index %d: %s", i, err)
		}
	}
}

func (u *publisher) checkTXAction(action *pb.TransactionAction) error {
	chaPayload, err := utils.GetChaincodeActionPayload(action.Payload)
	if err != nil {
		return err
	}
	return u.checkChaincodeActionPayload(chaPayload)
}

func (u *publisher) checkChaincodeActionPayload(chaPayload *pb.ChaincodeActionPayload) error {
	cpp := &pb.ChaincodeProposalPayload{}
	err := proto.Unmarshal(chaPayload.ChaincodeProposalPayload, cpp)
	if err != nil {
		return err
	}

	return u.checkAction(chaPayload.Action)
}

func (u *publisher) checkAction(action *pb.ChaincodeEndorsedAction) error {
	prp := &pb.ProposalResponsePayload{}
	err := proto.Unmarshal(action.ProposalResponsePayload, prp)
	if err != nil {
		return err
	}
	return u.checkProposalResponsePayload(prp)
}

func (u *publisher) checkProposalResponsePayload(prp *pb.ProposalResponsePayload) error {
	chaincodeAction := &pb.ChaincodeAction{}
	err := proto.Unmarshal(prp.Extension, chaincodeAction)
	if err != nil {
		return err
	}
	return u.checkChaincodeAction(chaincodeAction)
}

func (u *publisher) checkChaincodeAction(chaincodeAction *pb.ChaincodeAction) error {
	if len(chaincodeAction.Results) > 0 {
		txRWSet := &rwsetutil.TxRwSet{}
		if err := txRWSet.FromProtoBytes(chaincodeAction.Results); err != nil {
			return err
		}
		u.checkTxReadWriteSet(txRWSet)
	}
	return nil
}

func (u *publisher) checkTxReadWriteSet(txRWSet *rwsetutil.TxRwSet) {
	for _, nsRWSet := range txRWSet.NsRwSets {
		u.checkNsReadWriteSet(nsRWSet)
	}
}

func (u *publisher) checkNsReadWriteSet(nsRWSet *rwsetutil.NsRwSet) {
	for _, w := range nsRWSet.KvRwSet.Writes {
		if nsRWSet.NameSpace == "lscc" {
			if err := u.handleStateUpdate(u.channelID, w); err != nil {
				logger.Warningf("Error handling state update for key [%s]: %s", w.Key, err)
			}
		}
	}
}

func (u *publisher) handleStateUpdate(channelName string, kvWrite *kvrwset.KVWrite) error {
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
	// FIXME: Change to Debugf
	logger.Infof("Handling LSCC state update for chaincode [%s] on channel [%s]", kvWrite.Key, channelName)
	chaincodeData := &ccprovider.ChaincodeData{}
	if err := proto.Unmarshal(kvWrite.Value, chaincodeData); err != nil {
		return errors.Errorf("Unmarshalling ChaincodeQueryResponse failed, error %s", err)
	}

	chaincodeDefs := []*cceventmgmt.ChaincodeDefinition{}
	chaincodeDefs = append(chaincodeDefs, &cceventmgmt.ChaincodeDefinition{Name: chaincodeData.CCName(), Version: chaincodeData.CCVersion(), Hash: chaincodeData.Hash()})

	// FIXME: Change to Debugf
	logger.Infof("Calling manager.HandleChaincodeDeploy for chaincode [%s] on channel [%s]", chaincodeData.Name, channelName)
	err := cceventmgmt.GetMgr().HandleChaincodeDeploy(channelName, chaincodeDefs)
	if err != nil {
		return err
	}

	// FIXME: Change to Debugf
	logger.Infof("Calling manager.ChaincodeDeployDone for chaincode [%s] on channel [%s]", chaincodeData.Name, channelName)
	cceventmgmt.GetMgr().ChaincodeDeployDone(channelName)

	return nil
}

// isCollectionConfigKey detects if a key is a collection key
func isCollectionConfigKey(key string) bool {
	return strings.Contains(key, collectionSeparator)
}
