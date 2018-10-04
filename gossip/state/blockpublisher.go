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
)

const (
	collectionSeparator = "~"
)

// BlockPublisher is used for endorser-only peers to notify all interested
// consumers of the new block
type BlockPublisher interface {
	AddBlock(block *cb.Block) error
}

type publisher struct {
	channelID   string
	bp          BlockPublisher
	blockNumber uint64
	mutex       sync.RWMutex
}

func newBlockPublisher(channelID string, bp BlockPublisher, ledgerHeight uint64) *publisher {
	if ledgerHeight == 0 {
		panic("Ledger height must be greater than 0")
	}

	logger.Infof("Initializing ledger height to %d for channel [%s]: %s", ledgerHeight, channelID)

	return &publisher{
		channelID:   channelID,
		bp:          bp,
		blockNumber: ledgerHeight - 1,
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

	for i := range block.Data.Data {
		envelope, err := utils.ExtractEnvelope(block, i)
		if err != nil {
			return err
		}
		if err := p.checkEnvelope(envelope); err != nil {
			logger.Warningf("Error checking envelope at index %d for channel [%s]: %s", i, p.channelID, err)
		}
	}

	err := p.bp.AddBlock(block)
	if err != nil {
		return err
	}

	p.blockNumber = block.Header.Number

	logger.Infof("Updated ledger height to %d for channel [%s]", p.blockNumber+1, p.channelID)

	return nil
}

func (p *publisher) LedgerHeight() uint64 {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.blockNumber + 1
}

func (p *publisher) checkEnvelope(envelope *fabriccmn.Envelope) error {
	payload, err := utils.ExtractPayload(envelope)
	if err != nil {
		return err
	}
	chdr, err := utils.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return err
	}

	if fabriccmn.HeaderType(chdr.Type) == fabriccmn.HeaderType_ENDORSER_TRANSACTION {
		if err := p.checkData(payload.Data); err != nil {
			return err
		}
	}
	return nil
}

func (p *publisher) checkData(data []byte) error {
	tx, err := utils.GetTransaction(data)
	if err != nil {
		return err
	}
	p.checkTransaction(tx)
	return nil
}

func (p *publisher) checkTransaction(tx *pb.Transaction) {
	for i, action := range tx.Actions {
		if err := p.checkTXAction(action); err != nil {
			logger.Warningf("Error checking TxAction at index %d: %s", i, err)
		}
	}
}

func (p *publisher) checkTXAction(action *pb.TransactionAction) error {
	chaPayload, err := utils.GetChaincodeActionPayload(action.Payload)
	if err != nil {
		return err
	}
	return p.checkChaincodeActionPayload(chaPayload)
}

func (p *publisher) checkChaincodeActionPayload(chaPayload *pb.ChaincodeActionPayload) error {
	cpp := &pb.ChaincodeProposalPayload{}
	err := proto.Unmarshal(chaPayload.ChaincodeProposalPayload, cpp)
	if err != nil {
		return err
	}

	return p.checkAction(chaPayload.Action)
}

func (p *publisher) checkAction(action *pb.ChaincodeEndorsedAction) error {
	prp := &pb.ProposalResponsePayload{}
	err := proto.Unmarshal(action.ProposalResponsePayload, prp)
	if err != nil {
		return err
	}
	return p.checkProposalResponsePayload(prp)
}

func (p *publisher) checkProposalResponsePayload(prp *pb.ProposalResponsePayload) error {
	chaincodeAction := &pb.ChaincodeAction{}
	err := proto.Unmarshal(prp.Extension, chaincodeAction)
	if err != nil {
		return err
	}
	return p.checkChaincodeAction(chaincodeAction)
}

func (p *publisher) checkChaincodeAction(chaincodeAction *pb.ChaincodeAction) error {
	if len(chaincodeAction.Results) > 0 {
		txRWSet := &rwsetutil.TxRwSet{}
		if err := txRWSet.FromProtoBytes(chaincodeAction.Results); err != nil {
			return err
		}
		p.checkTxReadWriteSet(txRWSet)
	}
	return nil
}

func (p *publisher) checkTxReadWriteSet(txRWSet *rwsetutil.TxRwSet) {
	for _, nsRWSet := range txRWSet.NsRwSets {
		p.checkNsReadWriteSet(nsRWSet)
	}
}

func (p *publisher) checkNsReadWriteSet(nsRWSet *rwsetutil.NsRwSet) {
	for _, w := range nsRWSet.KvRwSet.Writes {
		if nsRWSet.NameSpace == "lscc" {
			if err := p.handleStateUpdate(w); err != nil {
				logger.Warningf("Error handling state update for key [%s]: %s", w.Key, err)
			}
		}
	}
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
