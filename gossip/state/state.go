/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/golang/protobuf/proto"
	vsccErrors "github.com/hyperledger/fabric/common/errors"
	"github.com/hyperledger/fabric/common/metrics"
	"github.com/hyperledger/fabric/common/util/retry"
	"github.com/hyperledger/fabric/core/committer/txvalidator/validationpolicy"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	ledgerUtil "github.com/hyperledger/fabric/core/ledger/util"
	"github.com/hyperledger/fabric/gossip/api"
	"github.com/hyperledger/fabric/gossip/comm"
	common2 "github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/gossip/state/validationctx"

	gossipimpl "github.com/hyperledger/fabric/gossip/gossip"
	privdata2 "github.com/hyperledger/fabric/gossip/privdata"
	"github.com/hyperledger/fabric/gossip/util"

	"github.com/hyperledger/fabric/gossip/discovery"
	"github.com/hyperledger/fabric/gossip/roleutil"
	"github.com/hyperledger/fabric/protos/common"
	proto "github.com/hyperledger/fabric/protos/gossip"
	"github.com/hyperledger/fabric/protos/ledger/rwset"
	"github.com/hyperledger/fabric/protos/peer"
	"github.com/hyperledger/fabric/protos/transientstore"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"golang.org/x/net/context"
)

// GossipStateProvider is the interface to acquire sequences of the ledger blocks
// capable to full fill missing blocks by running state replication and
// sending request to get missing block to other nodes
type GossipStateProvider interface {
	AddPayload(payload *proto.Payload) error

	// Stop terminates state transfer object
	Stop()
}

const (
	defAntiEntropyInterval             = 10 * time.Second
	defAntiEntropyStateResponseTimeout = 3 * time.Second
	defAntiEntropyBatchSize            = 10

	defChannelBufferSize     = 100
	defAntiEntropyMaxRetries = 3

	defMaxBlockDistance = 100

	blocking    = true
	nonBlocking = false

	enqueueRetryInterval = time.Millisecond * 100
)

// GossipAdapter defines gossip/communication required interface for state provider
type GossipAdapter interface {
	// Send sends a message to remote peers
	Send(msg *proto.GossipMessage, peers ...*comm.RemotePeer)

	// Accept returns a dedicated read-only channel for messages sent by other nodes that match a certain predicate.
	// If passThrough is false, the messages are processed by the gossip layer beforehand.
	// If passThrough is true, the gossip layer doesn't intervene and the messages
	// can be used to send a reply back to the sender
	Accept(acceptor common2.MessageAcceptor, passThrough bool) (<-chan *proto.GossipMessage, <-chan proto.ReceivedMessage)

	// UpdateLedgerHeight updates the ledger height the peer
	// publishes to other peers in the channel
	UpdateLedgerHeight(height uint64, chainID common2.ChainID)

	// PeersOfChannel returns the NetworkMembers considered alive
	// and also subscribed to the channel given
	PeersOfChannel(common2.ChainID) []discovery.NetworkMember

	// Gossip sends a message to other peers to the network
	Gossip(msg *proto.GossipMessage)

	// IdentityInfo returns information known peer identities
	IdentityInfo() api.PeerIdentitySet

	// SelfMembershipInfo returns the peer's membership information
	SelfMembershipInfo() discovery.NetworkMember
}

// MCSAdapter adapter of message crypto service interface to bound
// specific APIs required by state transfer service
type MCSAdapter interface {
	// VerifyBlock returns nil if the block is properly signed, and the claimed seqNum is the
	// sequence number that the block's header contains.
	// else returns error
	VerifyBlock(chainID common2.ChainID, seqNum uint64, signedBlock []byte) error

	// VerifyByChannel checks that signature is a valid signature of message
	// under a peer's verification key, but also in the context of a specific channel.
	// If the verification succeeded, Verify returns nil meaning no error occurred.
	// If peerIdentity is nil, then the verification fails.
	VerifyByChannel(chainID common2.ChainID, peerIdentity api.PeerIdentityType, signature, message []byte) error
}

// ledgerResources defines abilities that the ledger provides
type ledgerResources interface {
	// StoreBlock deliver new block with underlined private data
	// returns missing transaction ids (this version commits the transaction).
	StoreBlock(*ledger.BlockAndPvtData, []string) error

	// PublishBlock deliver new block with underlined private data
	// returns missing transaction ids (this version adds the validated block
	// into local caches and indexes (for a peer that does endorsement).
	PublishBlock(*ledger.BlockAndPvtData, []string) error

	// ValidateBlock validate block
	ValidateBlock(block *common.Block, privateDataSets util.PvtDataCollections, validationResponseChan chan *validationpolicy.ValidationResults) (*ledger.BlockAndPvtData, []string, error)

	ValidatePartialBlock(ctx context.Context, block *common.Block)

	// StorePvtData used to persist private date into transient store
	StorePvtData(txid string, privData *transientstore.TxPvtReadWriteSetWithConfigInfo, blckHeight uint64) error

	// GetPvtDataAndBlockByNum get block by number and returns also all related private data
	// the order of private data in slice of PvtDataCollections doesn't imply the order of
	// transactions in the block related to these private data, to get the correct placement
	// need to read TxPvtData.SeqInBlock field
	GetPvtDataAndBlockByNum(seqNum uint64, peerAuthInfo common.SignedData) (*common.Block, util.PvtDataCollections, error)

	// Get recent block sequence number
	LedgerHeight() (uint64, error)

	// Close ledgerResources
	Close()
}

// ServicesMediator aggregated adapter to compound all mediator
// required by state transfer into single struct
type ServicesMediator struct {
	GossipAdapter
	MCSAdapter
}

// GossipStateProviderImpl the implementation of the GossipStateProvider interface
// the struct to handle in memory sliding window of
// new ledger block to be acquired by hyper ledger
type GossipStateProviderImpl struct {
	// Chain id
	chainID string

	mediator *ServicesMediator

	// Channel to read gossip messages from
	gossipChan <-chan *proto.GossipMessage

	commChan <-chan proto.ReceivedMessage

	// Queue of payloads which wasn't acquired yet
	payloads          PayloadsBuffer
	validationReqChan chan *common.Block

	ledger ledgerResources

	stateResponseCh chan proto.ReceivedMessage

	stateRequestCh chan proto.ReceivedMessage

	stopCh chan struct{}

	validationResponseChan chan *validationpolicy.ValidationResults

	pendingValidations *blockCache

	done sync.WaitGroup

	once sync.Once

	stateTransferActive int32

	peerLedger ledger.PeerLedger

	blockPublisher *publisher

	ctxProvider *validationctx.Provider

	roleUtil *roleutil.RoleUtil
}

var logger = util.GetLogger(util.LoggingStateModule, "")

// NewGossipStateProvider creates state provider with coordinator instance
// to orchestrate arrival of private rwsets and blocks before committing them into the ledger.
func NewGossipStateProvider(chainID string, services *ServicesMediator, ledger ledgerResources, peerLedger ledger.PeerLedger, transientStore privdata2.TransientStore) GossipStateProvider {

	gossipChan, _ := services.Accept(func(message interface{}) bool {
		// Get only data messages
		return message.(*proto.GossipMessage).IsDataMsg() &&
			bytes.Equal(message.(*proto.GossipMessage).Channel, []byte(chainID))
	}, false)

	remoteStateMsgFilter := func(message interface{}) bool {
		receivedMsg := message.(proto.ReceivedMessage)
		msg := receivedMsg.GetGossipMessage()
		if !(msg.IsRemoteStateMessage() || msg.GetPrivateData() != nil || msg.IsValidationResultsMsg() || msg.IsValidationReqMsg()) {
			return false
		}

		// Ensure we deal only with messages that belong to this channel
		if !bytes.Equal(msg.Channel, []byte(chainID)) {
			return false
		}
		connInfo := receivedMsg.GetConnectionInfo()
		authErr := services.VerifyByChannel(msg.Channel, connInfo.Identity, connInfo.Auth.Signature, connInfo.Auth.SignedData)
		if authErr != nil {
			logger.Warning("Got unauthorized request from", string(connInfo.Identity))
			return false
		}
		return true
	}

	// Filter message which are only relevant for nodeMetastate transfer
	_, commChan := services.Accept(remoteStateMsgFilter, true)

	height, err := ledger.LedgerHeight()
	if height == 0 {
		// Panic here since this is an indication of invalid situation which should not happen in normal
		// code path.
		logger.Panic("Committer height cannot be zero, ledger should include at least one block (genesis).")
	}

	if err != nil {
		logger.Error("Could not read ledger info to obtain current ledger height due to: ", errors.WithStack(err))
		// Exiting as without ledger it will be impossible
		// to deliver new blocks
		return nil
	}

	s := &GossipStateProviderImpl{
		// MessageCryptoService
		mediator: services,

		// Chain ID
		chainID: chainID,

		// Channel to read new messages from
		gossipChan: gossipChan,

		// Channel to read direct messages from other peers
		commChan: commChan,

		validationResponseChan: make(chan *validationpolicy.ValidationResults, 10),

		pendingValidations: newBlockCache(),

		// Create a queue for payload received
		payloads:          NewPayloadsBuffer(height),
		validationReqChan: make(chan *common.Block, 10),

		ledger: ledger,

		stateResponseCh: make(chan proto.ReceivedMessage, defChannelBufferSize),

		stateRequestCh: make(chan proto.ReceivedMessage, defChannelBufferSize),

		stopCh: make(chan struct{}, 1),

		stateTransferActive: 0,

		once: sync.Once{},

		peerLedger: peerLedger,

		blockPublisher: newBlockPublisher(chainID, ledger, height),

		ctxProvider: validationctx.NewProvider(),

		roleUtil: roleutil.New(chainID, services),
	}

	logger.Infof("Updating metadata information, "+
		"current ledger sequence is at = %d, next expected block is = %d", height-1, s.payloads.Next())
	logger.Debug("Updating gossip ledger height to", height)
	services.UpdateLedgerHeight(height, common2.ChainID(s.chainID))

	s.done.Add(5)

	// Listen for incoming communication
	go s.listen()
	// Deliver in order messages into the incoming channel
	go s.deliverPayloads()
	// Execute anti entropy to fill missing gaps
	go s.antiEntropy()
	// Taking care of state request messages
	go s.processStateRequests()
	// Process validation request messages
	go s.processValidationRequests()

	return s
}

func (s *GossipStateProviderImpl) listen() {
	defer s.done.Done()

	for {
		select {
		case msg := <-s.gossipChan:
			logger.Debug("Received new message via gossip channel")
			go s.queueNewMessage(msg)
		case msg := <-s.commChan:
			logger.Debug("Dispatching a message", msg)
			go s.dispatch(msg)
		case <-s.stopCh:
			s.stopCh <- struct{}{}
			logger.Debug("Stop listening for new messages")
			return
		}
	}
}
func (s *GossipStateProviderImpl) dispatch(msg proto.ReceivedMessage) {
	// Check type of the message
	if msg.GetGossipMessage().IsRemoteStateMessage() {
		logger.Debug("Handling direct state transfer message")
		// Got state transfer request response
		s.directMessage(msg)
	} else if msg.GetGossipMessage().GetPrivateData() != nil {
		logger.Debug("Handling private data collection message")
		// Handling private data replication message
		s.privateDataMessage(msg)
	} else if msg.GetGossipMessage().IsValidationResultsMsg() {
		logger.Debug("Handling validation results message")
		s.validationResultsMessage(msg)
	} else if msg.GetGossipMessage().IsValidationReqMsg() {
		logger.Debug("Handling validation request message")
		s.validationRequestMessage(msg)
	}
}

func (s *GossipStateProviderImpl) privateDataMessage(msg proto.ReceivedMessage) {
	if !bytes.Equal(msg.GetGossipMessage().Channel, []byte(s.chainID)) {
		logger.Warning("Received state transfer request for channel",
			string(msg.GetGossipMessage().Channel), "while expecting channel", s.chainID, "skipping request...")
		return
	}

	gossipMsg := msg.GetGossipMessage()
	pvtDataMsg := gossipMsg.GetPrivateData()

	if pvtDataMsg.Payload == nil {
		logger.Warning("Malformed private data message, no payload provided")
		return
	}

	collectionName := pvtDataMsg.Payload.CollectionName
	txID := pvtDataMsg.Payload.TxId
	pvtRwSet := pvtDataMsg.Payload.PrivateRwset

	if len(pvtRwSet) == 0 {
		logger.Warning("Malformed private data message, no rwset provided, collection name = ", collectionName)
		return
	}

	txPvtRwSet := &rwset.TxPvtReadWriteSet{
		DataModel: rwset.TxReadWriteSet_KV,
		NsPvtRwset: []*rwset.NsPvtReadWriteSet{{
			Namespace: pvtDataMsg.Payload.Namespace,
			CollectionPvtRwset: []*rwset.CollectionPvtReadWriteSet{{
				CollectionName: collectionName,
				Rwset:          pvtRwSet,
			}}},
		},
	}

	txPvtRwSetWithConfig := &transientstore.TxPvtReadWriteSetWithConfigInfo{
		PvtRwset: txPvtRwSet,
		CollectionConfigs: map[string]*common.CollectionConfigPackage{
			pvtDataMsg.Payload.Namespace: pvtDataMsg.Payload.CollectionConfigs,
		},
	}

	if err := s.ledger.StorePvtData(txID, txPvtRwSetWithConfig, pvtDataMsg.Payload.PrivateSimHeight); err != nil {
		logger.Errorf("Wasn't able to persist private data for collection %s, due to %s", collectionName, err)
		msg.Ack(err) // Sending NACK to indicate failure of storing collection
	}

	msg.Ack(nil)
	logger.Debug("Private data for collection", collectionName, "has been stored")
}

func (s *GossipStateProviderImpl) directMessage(msg proto.ReceivedMessage) {
	logger.Debug("[ENTER] -> directMessage")
	defer logger.Debug("[EXIT] ->  directMessage")

	if msg == nil {
		logger.Error("Got nil message via end-to-end channel, should not happen!")
		return
	}

	if !bytes.Equal(msg.GetGossipMessage().Channel, []byte(s.chainID)) {
		logger.Warning("Received state transfer request for channel",
			string(msg.GetGossipMessage().Channel), "while expecting channel", s.chainID, "skipping request...")
		return
	}

	incoming := msg.GetGossipMessage()

	if incoming.GetStateRequest() != nil {
		if len(s.stateRequestCh) < defChannelBufferSize {
			// Forward state request to the channel, if there are too
			// many message of state request ignore to avoid flooding.
			s.stateRequestCh <- msg
		}
	} else if incoming.GetStateResponse() != nil {
		// If no state transfer procedure activate there is
		// no reason to process the message
		if atomic.LoadInt32(&s.stateTransferActive) == 1 {
			// Send signal of state response message
			s.stateResponseCh <- msg
		}
	}
}

func (s *GossipStateProviderImpl) validationResultsMessage(msg proto.ReceivedMessage) {
	logger.Debug("[ENTER] -> validationResultsMessage")
	defer logger.Debug("[EXIT] ->  validationResultsMessage")

	validationResultsMsg := msg.GetGossipMessage().GetValidationResultsMsg()
	if !ledgerconfig.IsCommitter() {
		logger.Warningf("[%s] Validation Results message received on non-committer for block [%d]. Ignoring.", s.chainID, validationResultsMsg.SeqNum)
		return
	}

	logger.Debugf("[%s] Validation Results message for block [%d] received - sending to response channel", s.chainID, validationResultsMsg.SeqNum)
	mspID, ok := s.roleUtil.GetMSPID(msg.GetConnectionInfo().ID)
	if !ok {
		logger.Errorf("Unable to get MSP ID from PKI ID")
		return
	}

	s.validationResponseChan <- &validationpolicy.ValidationResults{
		BlockNumber: validationResultsMsg.SeqNum,
		TxFlags:     validationResultsMsg.TxFlags,
		Signature:   validationResultsMsg.Signature,
		Identity:    validationResultsMsg.Identity,
		Endpoint:    msg.GetConnectionInfo().Endpoint,
		MSPID:       mspID,
	}
}

func (s *GossipStateProviderImpl) validationRequestMessage(msg proto.ReceivedMessage) {
	logger.Debugf("[ENTER] -> validationRequestMessage")
	defer logger.Debug("[EXIT] ->  validationRequestMessage")

	if ledgerconfig.IsCommitter() || !ledgerconfig.IsValidator() {
		logger.Warningf("Committer and non-validator should not be receiving validation request messages")
		return
	}

	validationRequest := msg.GetGossipMessage().GetValidationReqMsg()
	if validationRequest.GetPayload() == nil {
		logger.Warning("Got nil payload in ValidationRequestMsg")
		return
	}

	block, err := createBlockFromPayload(validationRequest.GetPayload())
	if err != nil {
		logger.Warning("Got invalid block for seq number: %s", validationRequest.GetPayload().SeqNum, err)
		return
	}

	// FIXME: Change to Debug
	logger.Infof("[%s] Submitting unvalidated block %d for validation.", s.chainID, block.Header.Number)
	s.validationReqChan <- block
}

func (s *GossipStateProviderImpl) processStateRequests() {
	defer s.done.Done()

	for {
		select {
		case msg := <-s.stateRequestCh:
			s.handleStateRequest(msg)
		case <-s.stopCh:
			s.stopCh <- struct{}{}
			return
		}
	}
}

// Handle state request message, validate batch size, read current leader state to
// obtain required blocks, build response message and send it back
func (s *GossipStateProviderImpl) handleStateRequest(msg proto.ReceivedMessage) {
	if msg == nil {
		return
	}
	request := msg.GetGossipMessage().GetStateRequest()

	if !ledgerconfig.IsEndorser() {
		logger.Debugf("I'm not an endorser so ignoring state request for blocks in range [%d,%d]", request.StartSeqNum, request.EndSeqNum)
		return
	}

	batchSize := request.EndSeqNum - request.StartSeqNum
	if batchSize > defAntiEntropyBatchSize {
		logger.Errorf("Requesting blocks batchSize size (%d) greater than configured allowed"+
			" (%d) batching for anti-entropy. Ignoring request...", batchSize, defAntiEntropyBatchSize)
		return
	}

	if request.StartSeqNum > request.EndSeqNum {
		logger.Errorf("Invalid sequence interval [%d...%d], ignoring request...", request.StartSeqNum, request.EndSeqNum)
		return
	}

	currentHeight, err := s.ledger.LedgerHeight()
	if err != nil {
		logger.Errorf("Cannot access to current ledger height, due to %+v", errors.WithStack(err))
		return
	}
	if currentHeight < request.EndSeqNum {
		logger.Warningf("Received state request to transfer blocks with sequence numbers higher  [%d...%d] "+
			"than available in ledger (%d)", request.StartSeqNum, request.StartSeqNum, currentHeight)
	}

	endSeqNum := min(currentHeight, request.EndSeqNum)

	response := &proto.RemoteStateResponse{Payloads: make([]*proto.Payload, 0)}
	for seqNum := request.StartSeqNum; seqNum <= endSeqNum; seqNum++ {
		logger.Debug("Reading block ", seqNum, " with private data from the coordinator service")
		connInfo := msg.GetConnectionInfo()
		peerAuthInfo := common.SignedData{
			Data:      connInfo.Auth.SignedData,
			Signature: connInfo.Auth.Signature,
			Identity:  connInfo.Identity,
		}
		block, pvtData, err := s.ledger.GetPvtDataAndBlockByNum(seqNum, peerAuthInfo)

		if err != nil {
			logger.Errorf("cannot read block number %d from ledger, because %+v, skipping...", seqNum, err)
			continue
		}

		if block == nil {
			logger.Errorf("Wasn't able to read block with sequence number %d from ledger, skipping....", seqNum)
			continue
		}

		blockBytes, err := pb.Marshal(block)

		if err != nil {
			logger.Errorf("Could not marshal block: %+v", errors.WithStack(err))
			continue
		}

		var pvtBytes [][]byte
		if pvtData != nil {
			// Marshal private data
			pvtBytes, err = pvtData.Marshal()
			if err != nil {
				logger.Errorf("Failed to marshal private rwset for block %d due to %+v", seqNum, errors.WithStack(err))
				continue
			}
		}

		// Appending result to the response
		response.Payloads = append(response.Payloads, &proto.Payload{
			SeqNum:      seqNum,
			Data:        blockBytes,
			PrivateData: pvtBytes,
		})
	}
	// Sending back response with missing blocks
	msg.Respond(&proto.GossipMessage{
		// Copy nonce field from the request, so it will be possible to match response
		Nonce:   msg.GetGossipMessage().Nonce,
		Tag:     proto.GossipMessage_CHAN_OR_ORG,
		Channel: []byte(s.chainID),
		Content: &proto.GossipMessage_StateResponse{StateResponse: response},
	})
}

func (s *GossipStateProviderImpl) handleStateResponse(msg proto.ReceivedMessage) (uint64, error) {
	max := uint64(0)
	// Send signal that response for given nonce has been received
	response := msg.GetGossipMessage().GetStateResponse()
	// Extract payloads, verify and push into buffer
	if len(response.GetPayloads()) == 0 {
		return uint64(0), errors.New("Received state transfer response without payload")
	}
	for _, payload := range response.GetPayloads() {

		if metrics.IsDebug() {
			metrics.RootScope.Gauge(fmt.Sprintf("gossip_state_%s_handleStateResponse_block_number", metrics.FilterMetricName(s.chainID))).Update(float64(payload.SeqNum))
		}
		logger.Debugf("Received payload with sequence number %d.", payload.SeqNum)
		if err := s.mediator.VerifyBlock(common2.ChainID(s.chainID), payload.SeqNum, payload.Data); err != nil {
			err = errors.WithStack(err)
			logger.Warningf("Error verifying block with sequence number %d, due to %+v", payload.SeqNum, err)
			return uint64(0), err
		}
		if max < payload.SeqNum {
			max = payload.SeqNum
		}

		err := s.addPayload(payload, blocking)
		if err != nil {
			logger.Warningf("Block [%d] received from block transfer wasn't added to payload buffer: %v", payload.SeqNum, err)
		}
	}
	return max, nil
}

// Stop function send halting signal to all go routines
func (s *GossipStateProviderImpl) Stop() {
	// Make sure stop won't be executed twice
	// and stop channel won't be used again
	s.once.Do(func() {
		s.stopCh <- struct{}{}
		// Make sure all go-routines has finished
		s.done.Wait()
		// Close all resources
		s.ledger.Close()
		close(s.stateRequestCh)
		close(s.stateResponseCh)
		close(s.validationReqChan)
		close(s.validationResponseChan)
		close(s.stopCh)
	})
}

// New message notification/handler
func (s *GossipStateProviderImpl) queueNewMessage(msg *proto.GossipMessage) {
	if !bytes.Equal(msg.Channel, []byte(s.chainID)) {
		logger.Warning("Received enqueue for channel",
			string(msg.Channel), "while expecting channel", s.chainID, "ignoring enqueue")
		return
	}

	dataMsg := msg.GetDataMsg()
	if dataMsg == nil {
		logger.Info("Gossip message received is not of data message type, usually this should not happen.")
		return
	}

	logger.Debugf("[%s] Adding block [%d] to payload buffer", s.chainID, dataMsg.Payload.SeqNum)
	if err := s.addPayload(dataMsg.Payload, nonBlocking); err != nil {
		logger.Warningf("[%s] Block [%d] received from gossip wasn't added to payload buffer: %v", s.chainID, dataMsg.Payload.SeqNum, err)
	}
}

func (s *GossipStateProviderImpl) deliverPayloads() {
	defer s.done.Done()

	for {
		if !s.waitForBufferReady() {
			break
		}

		for payload := s.payloads.Pop(); payload != nil; payload = s.payloads.Pop() {
			// KEEP EVEN WHEN metrics.debug IS OFF
			metrics.RootScope.Gauge(fmt.Sprintf("gossip_state_%s_next_sequence_arrived", metrics.FilterMetricName(s.chainID))).Update(float64(payload.GetSeqNum()))

			rawBlock := &common.Block{}
			if err := pb.Unmarshal(payload.Data, rawBlock); err != nil {
				logger.Errorf("Error getting block with seqNum = %d due to (%+v)...dropping block", payload.SeqNum, errors.WithStack(err))
				continue
			}
			if rawBlock.Data == nil || rawBlock.Header == nil {
				logger.Errorf("Block with claimed sequence %d has no header (%v) or data (%v)",
					payload.SeqNum, rawBlock.Header, rawBlock.Data)
				continue
			}

			if ledgerconfig.IsValidator() && !ledgerconfig.IsCommitter() {
				// Cancel any outstanding validation for the current block being committed
				s.ctxProvider.Cancel(rawBlock.Header.Number)
			}

			logger.Debugf("[%s] Transferring block [%d] with %d transaction(s) to the ledger", s.chainID, payload.SeqNum, len(rawBlock.Data.Data))

			// Read all private data into slice
			var p util.PvtDataCollections
			if payload.PrivateData != nil {
				err := p.Unmarshal(payload.PrivateData)
				if err != nil {
					logger.Errorf("Wasn't able to unmarshal private data for block seqNum = %d due to (%+v)...dropping block", payload.SeqNum, errors.WithStack(err))
					continue
				}
			}
			if err := s.commitBlock(rawBlock, p); err != nil {
				if executionErr, isExecutionErr := err.(*vsccErrors.VSCCExecutionFailureError); isExecutionErr {
					logger.Errorf("Failed executing VSCC due to %v. Aborting chain processing", executionErr)
					return
				}
				logger.Panicf("Cannot commit block to the ledger due to %+v", errors.WithStack(err))
			}

			if ledgerconfig.IsValidator() && !ledgerconfig.IsCommitter() {
				unvalidatedBlock := s.pendingValidations.Remove(rawBlock.Header.Number + 1)
				if unvalidatedBlock != nil {
					logger.Debugf("[%s] Validating pending block [%d] with %d transaction(s)", s.chainID, payload.SeqNum, len(unvalidatedBlock.Data.Data))
					s.ledger.ValidatePartialBlock(s.ctxProvider.Create(unvalidatedBlock.Header.Number), unvalidatedBlock)
				}
			}

		}
	}
}

func (s *GossipStateProviderImpl) waitForBufferReady() bool {
	stopWatch := metrics.StopWatch(fmt.Sprintf("gossip_state_%s_wait_for_buffer_ready", metrics.FilterMetricName(s.chainID)))
	defer stopWatch()

	ready, readySig := s.payloads.Ready()
	if ready {
		return true
	}

	select {
	// Wait for notification that next seq has arrived
	case <-readySig:
		if metrics.IsDebug() {
			metrics.RootScope.Gauge(fmt.Sprintf("gossip_state_%s_next_sequence_ready", metrics.FilterMetricName(s.chainID))).Update(float64(s.payloads.Next()))
		}
		logger.Debugf("[%s] Ready to transfer payloads (blocks) to the ledger, next block number is = [%d]", s.chainID, s.payloads.Next())
		// Collect all subsequent payloads
	case <-s.stopCh:
		s.stopCh <- struct{}{}
		logger.Debug("State provider has been stopped, finishing to push new blocks.")
		return false
	}

	return true
}

func (s *GossipStateProviderImpl) processValidationRequests() {
	defer s.done.Done()

	for {
		select {
		case block := <-s.validationReqChan:
			// FIXME: Change to Debug
			logger.Infof("[%s] Received validation request for block %d", s.chainID, block.Header.Number)

			currentHeight, err := s.ledger.LedgerHeight()
			if err != nil {
				logger.Errorf("Error getting height from DB for channel [%s]: %s", s.chainID, errors.WithStack(err))
			}
			if block.Header.Number == currentHeight {
				logger.Infof("[%s] Validating block [%d] with %d transaction(s)", s.chainID, block.Header.Number, len(block.Data.Data))
				s.ledger.ValidatePartialBlock(s.ctxProvider.Create(block.Header.Number), block)
			} else if block.Header.Number > currentHeight {
				logger.Infof("[%s] Block [%d] with %d transaction(s) cannot be validated yet since our ledger height is %d. Adding to cache.", s.chainID, block.Header.Number, len(block.Data.Data), currentHeight)
				s.pendingValidations.Add(block)
			} else {
				logger.Infof("[%s] Block [%d] will not be validated since the block has already been committed. Our ledger height is %d.", s.chainID, block.Header.Number, currentHeight)
			}
		case <-s.stopCh:
			s.stopCh <- struct{}{}
			return
		}
	}
}

func (s *GossipStateProviderImpl) antiEntropy() {
	defer s.done.Done()
	defer logger.Debug("State Provider stopped, stopping anti entropy procedure.")

	for {
		select {
		case <-s.stopCh:
			s.stopCh <- struct{}{}
			return
		case <-time.After(defAntiEntropyInterval):
			ourHeight, err := s.ledger.LedgerHeight()
			if err != nil {
				// Unable to read from ledger continue to the next round
				logger.Errorf("Cannot obtain ledger height, due to %+v", errors.WithStack(err))
				continue
			}
			if ourHeight == 0 {
				logger.Error("Ledger reported block height of 0 but this should be impossible")
				continue
			}

			if ledgerconfig.IsCommitter() {
				maxHeight := s.maxAvailableLedgerHeight()
				if ourHeight >= maxHeight {
					continue
				}
				logger.Debugf("I am a committer. Requesting blocks in range [%d:%d] for channel [%s]...", ourHeight-1, uint64(maxHeight)-1, s.chainID)
				s.requestBlocksInRange(uint64(ourHeight), uint64(maxHeight)-1)
			} else {
				// No need to request blocks from other peers since we just need to make sure that our in-memory
				// block-height is caught up with the block height in the DB
				ledgerHeight, err := s.ledger.LedgerHeight()
				if err != nil {
					logger.Errorf("Error getting height from DB for channel [%s]: %s", s.chainID, errors.WithStack(err))
					continue
				}

				logger.Debugf("Endorser height [%d], ledger height [%d] for channel [%s]", ourHeight, ledgerHeight, s.chainID)
				if ourHeight >= ledgerHeight {
					logger.Debugf("Endorser height [%d], ledger height [%d] for channel [%s]. No need to load blocks from DB.", ourHeight, ledgerHeight, s.chainID)
					continue
				}

				payloads, err := s.loadBlocksInRange(ourHeight, ledgerHeight-1)
				if err != nil {
					logger.Errorf("Error loading blocks for channel [%s]: %s", s.chainID, err)
					continue
				}

				if err := s.addPayloads(payloads); err != nil {
					logger.Errorf("Error adding payloads for channel [%s]: %s", s.chainID, err)
					continue
				}
			}
		}
	}
}

// Iterate over all available peers and check advertised meta state to
// find maximum available ledger height across peers
func (s *GossipStateProviderImpl) maxAvailableLedgerHeight() uint64 {
	max := uint64(0)
	for _, p := range s.mediator.PeersOfChannel(common2.ChainID(s.chainID)) {
		if p.Properties == nil {
			logger.Debug("Peer", p.PreferredEndpoint(), "doesn't have properties, skipping it")
			continue
		}
		peerHeight := p.Properties.LedgerHeight
		if max < peerHeight {
			max = peerHeight
		}
	}
	return max
}

// GetBlocksInRange capable to acquire blocks with sequence
// numbers in the range [start...end).
func (s *GossipStateProviderImpl) requestBlocksInRange(start uint64, end uint64) {
	atomic.StoreInt32(&s.stateTransferActive, 1)
	defer atomic.StoreInt32(&s.stateTransferActive, 0)

	for prev := start; prev <= end; {
		next := min(end, prev+defAntiEntropyBatchSize)

		gossipMsg := s.stateRequestMessage(prev, next)

		responseReceived := false
		tryCounts := 0

		for !responseReceived {
			if tryCounts > defAntiEntropyMaxRetries {
				logger.Warningf("Wasn't  able to get blocks in range [%d...%d), after %d retries",
					prev, next, tryCounts)
				return
			}
			// Select peers to ask for blocks
			peer, err := s.selectPeerToRequestFrom(next)
			if err != nil {
				logger.Warningf("Cannot send state request for blocks in range [%d...%d), due to %+v",
					prev, next, errors.WithStack(err))
				return
			}

			logger.Debugf("State transfer, with peer %s, requesting blocks in range [%d...%d), "+
				"for chainID %s", peer.Endpoint, prev, next, s.chainID)

			s.mediator.Send(gossipMsg, peer)
			tryCounts++

			// Wait until timeout or response arrival
			select {
			case msg := <-s.stateResponseCh:
				if msg.GetGossipMessage().Nonce != gossipMsg.Nonce {
					continue
				}
				// Got corresponding response for state request, can continue
				index, err := s.handleStateResponse(msg)
				if err != nil {
					logger.Warningf("Wasn't able to process state response for "+
						"blocks [%d...%d], due to %+v", prev, next, errors.WithStack(err))
					continue
				}
				prev = index + 1
				responseReceived = true
			case <-time.After(defAntiEntropyStateResponseTimeout):
			case <-s.stopCh:
				s.stopCh <- struct{}{}
				return
			}
		}
	}
}

func (s *GossipStateProviderImpl) loadBlocksInRange(fromBlock, toBlock uint64) ([]*proto.Payload, error) {
	logger.Debugf("Loading blocks in range %d to %d for channel [%s]", fromBlock, toBlock, s.chainID)

	var payloads []*proto.Payload

	for num := fromBlock; num <= toBlock; num++ {
		// Don't need to load the private data since we don't actually do anything with it on the endorser.
		logger.Debugf("Getting block %d for channel [%s]...", num, s.chainID)
		block, err := s.peerLedger.GetBlockByNumber(num)
		if err != nil {
			return nil, errors.WithMessage(err, fmt.Sprintf("Error reading block and private data for block %d", num))
		}

		blockBytes, err := pb.Marshal(block)
		if err != nil {
			logger.Errorf("Could not marshal block: %+v", errors.WithStack(err))
			return nil, errors.WithMessage(err, fmt.Sprintf("Error marshalling block %d", num))
		}

		payloads = append(payloads,
			&proto.Payload{
				SeqNum: num,
				Data:   blockBytes,
			},
		)
	}

	return payloads, nil
}

// Generate state request message for given blocks in range [beginSeq...endSeq]
func (s *GossipStateProviderImpl) stateRequestMessage(beginSeq uint64, endSeq uint64) *proto.GossipMessage {
	return &proto.GossipMessage{
		Nonce:   util.RandomUInt64(),
		Tag:     proto.GossipMessage_CHAN_OR_ORG,
		Channel: []byte(s.chainID),
		Content: &proto.GossipMessage_StateRequest{
			StateRequest: &proto.RemoteStateRequest{
				StartSeqNum: beginSeq,
				EndSeqNum:   endSeq,
			},
		},
	}
}

// Select peer which has required blocks to ask missing blocks from
func (s *GossipStateProviderImpl) selectPeerToRequestFrom(height uint64) (*comm.RemotePeer, error) {
	// Filter peers which posses required range of missing blocks
	peers := s.filterPeers(s.hasRequiredHeight(height))

	n := len(peers)
	if n == 0 {
		return nil, errors.New("there are no peers to ask for missing blocks from")
	}

	// Select peer to ask for blocks
	return peers[util.RandomInt(n)], nil
}

// filterPeers return list of peers which aligns the predicate provided
func (s *GossipStateProviderImpl) filterPeers(predicate func(peer discovery.NetworkMember) bool) []*comm.RemotePeer {
	var peers []*comm.RemotePeer

	for _, member := range s.mediator.PeersOfChannel(common2.ChainID(s.chainID)) {
		if member.Properties != nil {
			roles := gossipimpl.Roles(member.Properties.Roles)
			if !roles.HasRole(ledgerconfig.EndorserRole) {
				logger.Debugf("Not choosing [%s] since it's not an endorser", member.Endpoint)
				continue
			}
		}
		if predicate(member) {
			logger.Debugf("Choosing [%s] since it's an endorser", member.Endpoint)
			peers = append(peers, &comm.RemotePeer{Endpoint: member.PreferredEndpoint(), PKIID: member.PKIid})
		}
	}

	return peers
}

// hasRequiredHeight returns predicate which is capable to filter peers with ledger height above than indicated
// by provided input parameter
func (s *GossipStateProviderImpl) hasRequiredHeight(height uint64) func(peer discovery.NetworkMember) bool {
	return func(peer discovery.NetworkMember) bool {
		if peer.Properties != nil {
			return peer.Properties.LedgerHeight >= height
		}
		logger.Debug(peer.PreferredEndpoint(), "doesn't have properties")
		return false
	}
}

// AddPayload add new payload into state.
func (s *GossipStateProviderImpl) AddPayload(payload *proto.Payload) error {
	if !ledgerconfig.IsCommitter() {
		// Only the committer processes the payload from the orderer.
		// Other roles receive the block via gossip.
		return nil
	}

	// Gossip the unvalidated block to other validators (if any)
	// so that they can perform validation on the block.

	// FIXME: Should only gossip the block to the validators that will be involved in validating the block
	validators := s.roleUtil.Validators(false)
	if len(validators) > 0 {
		gossipMsg := createValidationRequestGossipMsg(s.chainID, payload)
		logger.Debugf("[%s] Gossiping block [%d] to [%d] validator(s)", s.chainID, payload.SeqNum, len(validators))
		s.mediator.Send(gossipMsg, asRemotePeers(validators)...)
	} else {
		logger.Debugf("[%s] Not gossiping block [%d] since no other validators were found", s.chainID, payload.SeqNum)
	}

	blockingMode := blocking
	if viper.GetBool("peer.gossip.nonBlockingCommitMode") {
		blockingMode = false
	}

	return s.addPayload(payload, blockingMode)
}

// addPayload add new payload into state. It may (or may not) block according to the
// given parameter. If it gets a block while in blocking mode - it would wait until
// the block is sent into the payloads buffer.
// Else - it may drop the block, if the payload buffer is too full.
func (s *GossipStateProviderImpl) addPayload(payload *proto.Payload, blockingMode bool) error {
	if payload == nil {
		return errors.New("Given payload is nil")
	}

	logger.Debugf("[%s] adding payload to local buffer [%d]", s.chainID, payload.SeqNum)
	height, err := s.ledger.LedgerHeight()
	if err != nil {
		return errors.Wrap(err, "Failed obtaining ledger height")
	}

	if !blockingMode && payload.SeqNum-height >= defMaxBlockDistance {
		return errors.Errorf("Ledger height is at %d, cannot enqueue block with sequence of %d", height, payload.SeqNum)
	}

	for blockingMode && s.payloads.Size() > defMaxBlockDistance*2 {
		logger.Infof("[%s] waiting for buffer to drain [seqNum=%d, size=%d]", s.chainID, payload.SeqNum, s.payloads.Size())
		time.Sleep(enqueueRetryInterval)
	}

	if s.payloads.Push(payload) {
		metrics.RootScope.Gauge(fmt.Sprintf("payloadbuffer_%s_push_block_number", metrics.FilterMetricName(s.chainID))).Update(float64(payload.SeqNum))
		metrics.RootScope.Gauge(fmt.Sprintf("payloadbuffer_%s_length", metrics.FilterMetricName(s.chainID))).Update(float64(s.payloads.Size()))

		block, _ := createBlockFromPayload(payload)
		txnCount := 0
		if block != nil {
			txnCount = len(block.Data.Data)
		}
		metrics.RootScope.Gauge(fmt.Sprintf("block_%s_size_txns", metrics.FilterMetricName(s.chainID))).Update(float64(txnCount))

		// TODO - make the following Debug if it turns out to be not useful.
		logger.Infof("[%s] payload added to buffer [%d] txn [%d]", s.chainID, payload.SeqNum, txnCount)
	}

	return nil
}

func createBlockFromPayload(payload *proto.Payload) (*common.Block, error) {
	block := &common.Block{}
	if err := pb.Unmarshal(payload.Data, block); err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("block has incorrect structure [%d]", payload.SeqNum))
	}
	if block.Data == nil || block.Header == nil {
		return nil, errors.Errorf("block has no header or data [%d]", payload.SeqNum)
	}

	// validate private data format prior to entering the payload buffer.
	var p util.PvtDataCollections
	if payload.PrivateData != nil {
		err := p.Unmarshal(payload.PrivateData)
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("block has incorrect structure [%d]", payload.SeqNum))
		}
	}

	return block, nil
}

func isBlockValidated(block *common.Block) bool {
	validated, _, _ := getValidationStatus(block)
	return validated
}

func getValidationStatus(block *common.Block) (validated, partiallyValidated, unvalidated bool) {
	blockData := block.GetData()
	envelopes := blockData.GetData()
	envelopesLen := len(envelopes)

	blockMetadata := block.GetMetadata()
	if blockMetadata == nil || blockMetadata.GetMetadata() == nil {
		return false, false, true
	}

	txValidationFlags := ledgerUtil.TxValidationFlags(blockMetadata.GetMetadata()[common.BlockMetadataIndex_TRANSACTIONS_FILTER])
	flagsLen := len(txValidationFlags)

	if envelopesLen != flagsLen {
		return false, false, true
	}

	atLeastOneValidated := false
	atLeastOneUnvalidated := false
	for _, flag := range txValidationFlags {
		if peer.TxValidationCode(flag) == peer.TxValidationCode_NOT_VALIDATED {
			atLeastOneUnvalidated = true
		} else {
			atLeastOneValidated = true
		}
	}

	validated = !atLeastOneUnvalidated
	partiallyValidated = atLeastOneValidated && atLeastOneUnvalidated
	unvalidated = !atLeastOneValidated

	return
}

func (s *GossipStateProviderImpl) addPayloads(payloads []*proto.Payload) error {
	for _, payload := range payloads {
		logger.Debugf("Adding payload for block %d and channel [%s]...", payload.SeqNum, s.chainID)
		if err := s.AddPayload(payload); err != nil {
			return errors.WithMessage(err, fmt.Sprintf("Error adding payload for block %d", payload.SeqNum))
		}
		logger.Debugf("Payload for block %d in channel [%s] was successfully added", payload.SeqNum, s.chainID)
	}
	return nil
}

func (s *GossipStateProviderImpl) commitBlock(block *common.Block, pvtData util.PvtDataCollections) error {
	if metrics.IsDebug() {
		metrics.RootScope.Gauge(fmt.Sprintf("gossip_state_%s_about_to_store_block_number", metrics.FilterMetricName(s.chainID))).Update(float64(block.Header.Number))
	}

	if !ledgerconfig.IsCommitter() {
		if !isBlockValidated(block) {
			logger.Warningf("Non-committer got unvalidated block %d. Ignoring.", block.Header.Number)
			return nil
		}

		// If not the committer than publish the block instead of committing.
		err := s.publishBlock(block, pvtData)
		if err != nil {
			logger.Errorf("Error publishing block: %s", err)
			// Don't return the error since it will cause a panic
			return nil
		}
		return nil
	}

	stopWatch := metrics.StopWatch(fmt.Sprintf("committer_%s_commitblock_duration", metrics.FilterMetricName(s.chainID)))
	defer stopWatch()

	stopWatch = metrics.StopWatch(fmt.Sprintf("committer_%s_validateblock_duration", metrics.FilterMetricName(s.chainID)))
	blockAndPvtData, pvtTxns, err := s.ledger.ValidateBlock(block, pvtData, s.validationResponseChan)
	if err != nil {
		logger.Errorf("Got error while validating block: %s", err)
		stopWatch()
		return err
	}
	stopWatch()

	// KEEP EVEN WHEN metrics.debug IS OFF
	metrics.RootScope.Gauge(fmt.Sprintf("gossip_state_%s_validated_block_number", metrics.FilterMetricName(s.chainID))).Update(float64(block.Header.Number))

	stopWatch = metrics.StopWatch(fmt.Sprintf("committer_%s_gossipblock_duration", metrics.FilterMetricName(s.chainID)))
	// Gossip messages with other nodes in my org
	s.gossipBlock(block, blockAndPvtData.BlockPvtData)
	stopWatch()

	stopWatch = metrics.StopWatch(fmt.Sprintf("committer_%s_storeblock_duration", metrics.FilterMetricName(s.chainID)))
	// Commit block with available private transactions
	err = s.ledger.StoreBlock(blockAndPvtData, pvtTxns)
	if err != nil {
		logger.Errorf("Got error while committing(%+v)", errors.WithStack(err))
		stopWatch()
		return err
	}
	stopWatch()

	stopWatch = metrics.StopWatch(fmt.Sprintf("committer_%s_updateledgerheight_duration", metrics.FilterMetricName(s.chainID)))
	// Update ledger height
	s.mediator.UpdateLedgerHeight(block.Header.Number+1, common2.ChainID(s.chainID))
	stopWatch()

	logger.Debugf("[%s] Committed block [%d] with %d transaction(s)",
		s.chainID, block.Header.Number, len(block.Data.Data))

	return nil
}

func (s *GossipStateProviderImpl) gossipBlock(block *common.Block, blockPvtData map[uint64]*ledger.TxPvtData) {
	blockNum := block.Header.Number

	marshaledBlock, err := pb.Marshal(block)
	if err != nil {
		logger.Errorf("[%s] Error serializing block with sequence number %d, due to %s", s.chainID, blockNum, err)
	}
	if err := s.mediator.VerifyBlock(common2.ChainID(s.chainID), blockNum, marshaledBlock); err != nil {
		logger.Errorf("[%s] Error verifying block with sequnce number %d, due to %s", s.chainID, blockNum, err)
	}

	pvtDataCollections := make(util.PvtDataCollections, 0)
	for _, value := range blockPvtData {
		pvtDataCollections = append(pvtDataCollections, &ledger.TxPvtData{SeqInBlock: value.SeqInBlock, WriteSet: value.WriteSet})
	}
	marshaledPvt, err := pvtDataCollections.Marshal()
	if err != nil {
		logger.Errorf("[%s] Error serializing pvtDataCollections with sequence number %d, due to %s", s.chainID, blockNum, err)
	}

	// Create payload with a block received
	payload := createPayload(blockNum, marshaledBlock, marshaledPvt)
	// Use payload to create gossip message
	gossipMsg := createGossipMsg(s.chainID, payload)

	logger.Debugf("[%s] Gossiping block %d", s.chainID, blockNum)
	s.mediator.GossipAdapter.Gossip(gossipMsg)
}

func createGossipMsg(chainID string, payload *proto.Payload) *proto.GossipMessage {
	gossipMsg := &proto.GossipMessage{
		Nonce:   0,
		Tag:     proto.GossipMessage_CHAN_AND_ORG,
		Channel: []byte(chainID),
		Content: &proto.GossipMessage_DataMsg{
			DataMsg: &proto.DataMessage{
				Payload: payload,
			},
		},
	}
	return gossipMsg
}

func createPayload(seqNum uint64, marshaledBlock []byte, privateData [][]byte) *proto.Payload {
	return &proto.Payload{
		Data:        marshaledBlock,
		SeqNum:      seqNum,
		PrivateData: privateData,
	}
}

func (s *GossipStateProviderImpl) publishBlock(block *common.Block, pvtData util.PvtDataCollections) error {
	if block == nil {
		return errors.New("cannot publish nil block")
	}

	if block.Header == nil {
		return errors.New("cannot publish block with nil header")
	}

	committedBlock, err := s.getCommittedBlock(block)
	if err != nil {
		return err
	}

	currentHeight, err := s.ledger.LedgerHeight()
	if err != nil {
		logger.Errorf("Error getting height from DB for channel [%s]: %s", s.chainID, errors.WithStack(err))
	}
	if block.Header.Number < currentHeight-1 {
		return errors.Errorf("received block %d but ledger height is already at %d", block.Header.Number, currentHeight)
	}

	bpd := createBlockAndPvtData(committedBlock, pvtData)
	if err := s.blockPublisher.AddBlock(bpd); err != nil {
		return errors.Wrapf(err, "error updating block number %d: %s", block.Header.Number, err)
	}

	logger.Debugf("Updating ledger height for channel [%s] to %d", s.chainID, block.Header.Number+1)
	s.mediator.UpdateLedgerHeight(block.Header.Number+1, common2.ChainID(s.chainID))

	return nil
}

func createBlockAndPvtData(block *common.Block, pvtData util.PvtDataCollections) *ledger.BlockAndPvtData {
	bpd := ledger.BlockAndPvtData{
		Block:        block,
		BlockPvtData: make(map[uint64]*ledger.TxPvtData),
	}

	for _, txPvtData := range pvtData {
		bpd.BlockPvtData[txPvtData.SeqInBlock] = txPvtData
	}

	return &bpd
}

func (s *GossipStateProviderImpl) getCommittedBlock(block *common.Block) (*common.Block, error) {
	// Examine the block to see if it was a committed block (i.e. already loaded from the ledger) or
	// if it's an uncommitted block coming from the orderer. (The uncommitted block will have nil set the the TxValidationFlags.)
	// This will save the extra call to the DB.
	blockMetadata := block.GetMetadata()
	txValidationFlags := ledgerUtil.TxValidationFlags(blockMetadata.GetMetadata()[common.BlockMetadataIndex_TRANSACTIONS_FILTER])

	if txValidationFlags == nil {
		logger.Warningf("[%s] Block %d was received in uncommitted state. Getting block from ledger.", s.chainID, block.Header.Number)
		committedBlock, err := s.getBlockFromLedger(block.Header.Number)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to get block number %d from ledger: %s", block.Header.Number, err)
		}

		if committedBlock == nil {
			return nil, errors.Errorf("nil block retrieved from ledger for block number %d", block.Header.Number)
		}

		logger.Debugf("block %d was received in uncommitted state", block.Header.Number)
		return committedBlock, nil
	}
	logger.Debugf("[%s] Block %d was received in committed state", s.chainID, block.Header.Number)
	return block, nil
}

func (s *GossipStateProviderImpl) getBlockFromLedger(number uint64) (*common.Block, error) {
	// TODO: Make configurable
	maxAttempts := 20
	block, err := retry.Invoke(
		func() (interface{}, error) {
			// Get the height from the ledger to see if the committer has finished committing. The ledger height is retrieved
			// from the checkpoint info, and checkpoint info is committed after the block, state, and pvt data.
			ledgerHeight, err := s.ledger.LedgerHeight()
			if err != nil {
				logger.Errorf("Error getting height from DB for channel [%s]: %s", s.chainID, errors.WithStack(err))
				return nil, errors.WithMessage(err, "Unable to get block height from ledger")
			}
			if ledgerHeight-1 < number {
				logger.Debugf("Block %d for channel [%s] hasn't been committed yet. Last block committed in DB is %d", number, s.chainID, ledgerHeight-1)
				return nil, errors.Errorf("Block %d for channel [%s] hasn't been committed yet", number, s.chainID)
			}

			logger.Debugf("Getting block %d for channel [%s] from ledger", number, s.chainID)
			return s.peerLedger.GetBlockByNumber(number)
		},
		retry.WithMaxAttempts(maxAttempts),
		retry.WithBeforeRetry(func(err error, attempt int, backoff time.Duration) bool {
			logger.Debugf("Got error on attempt #%d: %s. Retrying in %s.", attempt, err, backoff)
			return true
		}),
	)
	if err != nil {
		logger.Errorf("Unable to get block number %d from ledger after %d attempts: %s", number, maxAttempts, err)
		return nil, errors.Wrapf(err, "Unable to get block number %d from ledger", number)
	}

	if block == nil {
		logger.Errorf("Got nil block for number %d from ledger after %d attempts", number, maxAttempts)
		return nil, errors.Errorf("Got nil block for number %d", number)
	}

	return block.(*common.Block), nil
}

func min(a uint64, b uint64) uint64 {
	return b ^ ((a ^ b) & (-(uint64(a-b) >> 63)))
}

func createValidationRequestGossipMsg(chainID string, payload *proto.Payload) *proto.GossipMessage {
	gossipMsg := &proto.GossipMessage{
		Nonce:   0,
		Tag:     proto.GossipMessage_CHAN_AND_ORG,
		Channel: []byte(chainID),
		Content: &proto.GossipMessage_ValidationReqMsg{
			ValidationReqMsg: &proto.DataMessage{
				Payload: payload,
			},
		},
	}
	return gossipMsg
}

func asRemotePeers(members []*roleutil.Member) []*comm.RemotePeer {
	var peers []*comm.RemotePeer
	for _, m := range members {
		peers = append(peers, &comm.RemotePeer{
			Endpoint: m.Endpoint,
			PKIID:    m.PKIid,
		})
	}
	return peers
}
