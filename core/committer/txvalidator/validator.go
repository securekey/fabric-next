/*
Copyright IBM Corp. 2016 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package txvalidator

import (
	"fmt"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"sort"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/configtx"
	commonerrors "github.com/hyperledger/fabric/common/errors"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/common/sysccprovider"
	"github.com/hyperledger/fabric/core/common/validation"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/util"
	ledgerUtil "github.com/hyperledger/fabric/core/ledger/util"
	gossipcommon "github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/gossip/discovery"
	gossip2 "github.com/hyperledger/fabric/gossip/gossip"
	gossipimpl "github.com/hyperledger/fabric/gossip/gossip"
	"github.com/hyperledger/fabric/msp"
	"github.com/hyperledger/fabric/protos/common"
	gossipproto "github.com/hyperledger/fabric/protos/gossip"
	mspprotos "github.com/hyperledger/fabric/protos/msp"
	"github.com/hyperledger/fabric/protos/peer"
	"github.com/hyperledger/fabric/protos/utils"
	"github.com/op/go-logging"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

// Support provides all of the needed to evaluate the VSCC
type Support interface {
	// Acquire implements semaphore-like acquire semantics
	Acquire(ctx context.Context, n int64) error

	// Release implements semaphore-like release semantics
	Release(n int64)

	// Ledger returns the ledger associated with this validator
	Ledger() ledger.PeerLedger

	// MSPManager returns the MSP manager for this channel
	MSPManager() msp.MSPManager

	// Apply attempts to apply a configtx to become the new config
	Apply(configtx *common.ConfigEnvelope) error

	// GetMSPIDs returns the IDs for the application MSPs
	// that have been defined in the channel
	GetMSPIDs(cid string) []string

	// Capabilities defines the capabilities for the application portion of this channel
	Capabilities() channelconfig.ApplicationCapabilities
}

//Validator interface which defines API to validate block transactions
// and return the bit array mask indicating invalid transactions which
// didn't pass validation.
type Validator interface {
	Validate(block *common.Block, resultsChan chan *ValidationResults) error
	ValidatePartial(block *common.Block)
}

// private interface to decouple tx validator
// and vscc execution, in order to increase
// testability of TxValidator
type vsccValidator interface {
	VSCCValidateTx(seq int, payload *common.Payload, envBytes []byte, block *common.Block) (error, peer.TxValidationCode)
}

type mvccValidator interface {
	ValidateMVCC(block *common.Block, txFlags util.TxValidationFlags, filter util.TxFilter) error
}

// implementation of Validator interface, keeps
// reference to the ledger to enable tx simulation
// and execution of vscc
type TxValidator struct {
	ChainID       string
	Support       Support
	Vscc          vsccValidator
	gossip        gossip2.Gossip
	mvccValidator mvccValidator
}

var logger *logging.Logger // package-level logger

func init() {
	// Init logger with module name
	logger = flogging.MustGetLogger("committer/txvalidator")
}

type blockValidationRequest struct {
	block *common.Block
	d     []byte
	tIdx  int
}

type blockValidationResult struct {
	tIdx                 int
	validationCode       peer.TxValidationCode
	txsChaincodeName     *sysccprovider.ChaincodeInstance
	txsUpgradedChaincode *sysccprovider.ChaincodeInstance
	err                  error
	txid                 string
}

// NewTxValidator creates new transactions validator
func NewTxValidator(chainID string, support Support, sccp sysccprovider.SystemChaincodeProvider, pm PluginMapper, gossip gossip2.Gossip, mvccValidator mvccValidator) *TxValidator {
	// Encapsulates interface implementation
	pluginValidator := NewPluginValidator(pm, support.Ledger(), &dynamicDeserializer{support: support}, &dynamicCapabilities{support: support})
	return &TxValidator{
		ChainID:       chainID,
		Support:       support,
		Vscc:          newVSCCValidator(chainID, support, sccp, pluginValidator),
		gossip:        gossip,
		mvccValidator: mvccValidator,
	}
}

func (v *TxValidator) chainExists(chain string) bool {
	// TODO: implement this function!
	return true
}

// ValidationResults contains the validation flags for the given block number.
type ValidationResults struct {
	BlockNumber uint64
	TxFlags     ledgerUtil.TxValidationFlags
	Err         error
}

// Validate performs the validation of a block. The validation
// of each transaction in the block is performed in parallel.
// The approach is as follows: the committer thread starts the
// tx validation function in a goroutine (using a semaphore to cap
// the number of concurrent validating goroutines). The committer
// thread then reads results of validation (in orderer of completion
// of the goroutines) from the results channel. The goroutines
// perform the validation of the txs in the block and enqueue the
// validation result in the results channel. A few note-worthy facts:
// 1) to keep the approach simple, the committer thread enqueues
//    all transactions in the block and then moves on to reading the
//    results.
// 2) for parallel validation to work, it is important that the
//    validation function does not change the state of the system.
//    Otherwise the order in which validation is perform matters
//    and we have to resort to sequential validation (or some locking).
//    This is currently true, because the only function that affects
//    state is when a config transaction is received, but they are
//    guaranteed to be alone in the block. If/when this assumption
//    is violated, this code must be changed.
//
// NOTE: This function should only be called by committers and not validators.
func (v *TxValidator) Validate(block *common.Block, resultsChan chan *ValidationResults) error {
	startValidation := time.Now() // timer to log Validate block duration
	logger.Debugf("[%s] START Block Validation for block [%d]", v.ChainID, block.Header.Number)

	// Initialize trans as valid here, then set invalidation reason code upon invalidation below
	txsfltr := ledgerUtil.NewTxValidationFlags(len(block.Data.Data))
	// txsChaincodeNames records all the invoked chaincodes by tx in a block
	txsChaincodeNames := make(map[int]*sysccprovider.ChaincodeInstance)
	// upgradedChaincodes records all the chaincodes that are upgraded in a block
	txsUpgradedChaincodes := make(map[int]*sysccprovider.ChaincodeInstance)
	// array of txids
	txidArray := make([]string, len(block.Data.Data))

	txFlags, numValidated, err := v.validate(block, v.getTxFilter())
	if err == nil {
		mergeValidationFlags(block.Header.Number, txsfltr, txFlags)

		done := allValidated(txsfltr)
		if done {
			logger.Debugf("[%s] Committer has validated all %d transactions in block %d", v.ChainID, len(block.Data.Data), block.Header.Number)
		} else {
			err = v.waitForValidationResults(block.Header.Number, txsfltr, resultsChan, getValidationWaitTime(numValidated))
			if err != nil {
				logger.Warningf("[%s] Got error in validation response for block %d: %s", v.ChainID, block.Header.Number, err)
			}
		}

		if err == nil {
			notValidated := make(map[int]struct{})
			for i, flag := range txsfltr {
				if peer.TxValidationCode(flag) == peer.TxValidationCode_NOT_VALIDATED {
					notValidated[i] = struct{}{}
				}
			}

			if len(notValidated) > 0 {
				// Haven't received results for some of the transactions. Validate the remaining ones.
				go v.validateRemaining(block, notValidated, resultsChan)

				// Wait forever for a response
				err = v.waitForValidationResults(block.Header.Number, txsfltr, resultsChan, time.Hour)
				if err != nil {
					logger.Warningf("[%s] Got error validating remaining transactions in block %d: %s", v.ChainID, block.Header.Number, err)
				}
			}
		}
	}

	// if we're here, all workers have completed the validation.
	// If there was an error we return the error from the first
	// tx in this block that returned an error
	if err != nil {
		logger.Infof("[%s] Got error validating transactions in block %d: %s", v.ChainID, block.Header.Number, err)
		return err
	}

	if !allValidated(txsfltr) {
		logger.Errorf("[%s] Not all transactions in block %d were validated", v.ChainID, block.Header.Number)
		return errors.Errorf("Not all transactions in block %d were validated", block.Header.Number)
	}

	// if we operate with this capability, we mark invalid any transaction that has a txid
	// which is equal to that of a previous tx in this block
	if v.Support.Capabilities().ForbidDuplicateTXIdInBlock() {
		markTXIdDuplicates(txidArray, txsfltr)
	}

	// if we're here, all workers have completed validation and
	// no error was reported; we set the tx filter and return
	// success
	v.invalidTXsForUpgradeCC(txsChaincodeNames, txsUpgradedChaincodes, txsfltr)

	// make sure no transaction has skipped validation
	if !allValidated(txsfltr) {
		return errors.Errorf("not all transactions in block %d were validated", block.Header.Number)
	}

	// Initialize metadata structure
	utils.InitBlockMetadata(block)
	block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER] = txsfltr

	elapsedValidation := time.Since(startValidation) / time.Millisecond // duration in ms
	logger.Infof("[%s] Validated block [%d] in %dms", v.ChainID, block.Header.Number, elapsedValidation)

	return nil
}

// ValidatePartial partially validates the block and sends the validation results over Gossip
// NOTE: This function should only be called by validators and not committers.
func (v *TxValidator) ValidatePartial(block *common.Block) {
	txFlags, numValidated, err := v.validate(block, v.getTxFilter())
	if err != nil {
		// Error while validating. Don't send the result over Gossip - in this case the committer will
		// revalidate the unvalidated transactions.
		logger.Warningf("[%s] Got error in validation of block %d: %s", v.ChainID, block.Header.Number, err)
		return
	}

	if numValidated == 0 {
		logger.Debugf("[%s] No transactions were validated for block %d", v.ChainID, block.Header.Number)
		return
	}

	logger.Debugf("[%s] ... finished validating %d transactions in block %d. Error: %v", v.ChainID, numValidated, block.Header.Number, err)

	// Gossip the results to the committer
	msg, err := v.createValidationResponseGossipMsg(block, txFlags)
	if err != nil {
		logger.Errorf("[%s] Got error creating validation response for block %d: %s", v.ChainID, block.Header.Number, err)
		return
	}

	logger.Debugf("[%s] ... gossiping validation response for %d transactions in block %d", v.ChainID, numValidated, block.Header.Number)
	v.gossip.Gossip(msg)
}

func (v *TxValidator) validateRemaining(block *common.Block, notValidated map[int]struct{}, resultsChan chan *ValidationResults) {
	// FIXME: Change to Debug
	logger.Infof("[%s] Validating %d transactions in block %d that were not validated ...", v.ChainID, len(notValidated), block.Header.Number)
	txFlags, numValidated, err := v.validate(block,
		func(txIdx int) bool {
			_, ok := notValidated[txIdx]
			return ok
		},
	)

	// FIXME: Change to Debug
	logger.Infof("[%s] ... finished validating %d transactions in block %d that were not validated. Err: %v", v.ChainID, numValidated, block.Header.Number, err)

	resultsChan <- &ValidationResults{
		BlockNumber: block.Header.Number,
		TxFlags:     txFlags,
		Err:         err,
	}
}

func (v *TxValidator) waitForValidationResults(blockNumber uint64, txsfltr ledgerUtil.TxValidationFlags, resultsChan chan *ValidationResults, timeout time.Duration) error {
	// FIXME: Change to Debug
	logger.Infof("[%s] Waiting up to %s for validation responses for block %d ...", v.ChainID, timeout, blockNumber)

	start := time.Now()

	for {
		select {
		case result := <-resultsChan:
			// FIXME: Change to Debug
			logger.Infof("[%s] Got results for block %d", v.ChainID, result.BlockNumber)
			if result.BlockNumber < blockNumber {
				// FIXME: Change to Debug
				logger.Infof("[%s] Discarding validation results for block %d since we're waiting on block %d", v.ChainID, result.BlockNumber, blockNumber)
			} else if result.BlockNumber > blockNumber {
				// This shouldn't be possible
				logger.Warningf("[%s] Discarding validation results for block %d since we're waiting on block %d", v.ChainID, result.BlockNumber, blockNumber)
			} else {
				if result.Err != nil {
					logger.Infof("[%s] Received error in validation results for block %d: %s", v.ChainID, result.BlockNumber, result.Err)
					return result.Err
				}

				mergeValidationFlags(blockNumber, txsfltr, result.TxFlags)
				if allValidated(txsfltr) {
					// FIXME: Change to Debug
					logger.Infof("[%s] Block %d is all validated. Done waiting %s for responses.", v.ChainID, blockNumber, time.Since(start))
					return nil
				}
			}
		case <-time.After(timeout):
			// FIXME: Change to Debug
			logger.Infof("[%s] Timed out after %s waiting for validation response for block %d", v.ChainID, timeout, blockNumber)
			return nil
		}
	}
}

func mergeValidationFlags(blockNumber uint64, target, source ledgerUtil.TxValidationFlags) {
	for i, flag := range source {
		if peer.TxValidationCode(flag) == peer.TxValidationCode_NOT_VALIDATED {
			continue
		}
		currentFlag := target.Flag(i)
		if currentFlag == peer.TxValidationCode_NOT_VALIDATED {
			target.SetFlag(i, peer.TxValidationCode(flag))
		} else {
			logger.Warningf("TxValidation flag at index [%d] for block number %d is already set to %s and attempting to set it to %s. The flag will not be changed.", i, blockNumber, currentFlag, peer.TxValidationCode(flag))
		}
	}
}

func (v *TxValidator) validate(block *common.Block, shouldValidate util.TxFilter) (ledgerUtil.TxValidationFlags, int, error) {
	// First phase validation includes validating the block for proper structure, no duplicate transactions, signatures.
	logger.Debugf("[%s] Starting phase 1 validation of block %d ...", v.ChainID, block.Header.Number)
	txFlags, numValidated, err := v.validateBlock(block, shouldValidate)
	if logger.IsEnabledFor(logging.DEBUG) {
		logger.Debugf("[%s] ... finished phase 1 validation of block %d. Flags: %s", v.ChainID, block.Header.Number, flagsToString(txFlags))
	}
	if err != nil {
		logger.Errorf("[%s] Got error in phase1 validation of block %d: %s", v.ChainID, block.Header.Number, err)
		return nil, 0, err
	}

	// Second phase validation validates the transactions for MVCC conflicts against committed data.
	logger.Debugf("[%s] Starting phase 2 validation of block %d ...", v.ChainID, block.Header.Number)
	err = v.mvccValidator.ValidateMVCC(block, txFlags, shouldValidate)
	logger.Debugf("[%s] ... finished validation of block %d.", v.ChainID, block.Header.Number)
	if err != nil {
		logger.Errorf("[%s] Got error in phase 2 validation of block %d: %s", v.ChainID, block.Header.Number, err)
		return nil, 0, err
	}

	// FIXME: Change to debug
	if logger.IsEnabledFor(logging.DEBUG) {
		logger.Debugf("[%s] Returning flags from phase1 validation of block %d: %s", v.ChainID, block.Header.Number, flagsToString(txFlags))
	}

	return txFlags, numValidated, nil
}

func (v *TxValidator) validateBlock(block *common.Block, shouldValidate util.TxFilter) (ledgerUtil.TxValidationFlags, int, error) {
	logger.Debugf("[%s] Validating block %d ...", v.ChainID, block.Header.Number)

	var err error
	var errPos int

	// Initialize trans as valid here, then set invalidation reason code upon invalidation below
	txsfltr := ledgerUtil.NewTxValidationFlags(len(block.Data.Data))
	// txsChaincodeNames records all the invoked chaincodes by tx in a block
	txsChaincodeNames := make(map[int]*sysccprovider.ChaincodeInstance)
	// upgradedChaincodes records all the chaincodes that are upgraded in a block
	txsUpgradedChaincodes := make(map[int]*sysccprovider.ChaincodeInstance)
	// array of txids
	txidArray := make([]string, len(block.Data.Data))

	transactions := make(map[int]struct{})
	for tIdx := range block.Data.Data {
		if shouldValidate(tIdx) {
			transactions[tIdx] = struct{}{}
		}
	}

	results := make(chan *blockValidationResult)

	go func() {
		for tIdx, d := range block.Data.Data {
			_, ok := transactions[tIdx]
			if !ok {
				continue
			}

			// ensure that we don't have too many concurrent validation workers
			v.Support.Acquire(context.Background(), 1)
			logger.Debugf("[%s] Validating tx index [%d] in block %d ...", v.ChainID, tIdx, block.Header.Number)
			go func(index int, data []byte) {
				defer v.Support.Release(1)

				v.validateTx(&blockValidationRequest{
					d:     data,
					block: block,
					tIdx:  index,
				}, results)
				logger.Debugf("[%s] ... finished validating tx index [%d] in block %d", v.ChainID, index, block.Header.Number)
			}(tIdx, d)
		}
	}()

	logger.Debugf("[%s] expecting %d block validation responses", v.ChainID, len(transactions))

	// now we read responses in the order in which they come back
	for i := 0; i < len(transactions); i++ {
		res := <-results

		if res.err != nil {
			// if there is an error, we buffer its value, wait for
			// all workers to complete validation and then return
			// the error from the first tx in this block that returned an error
			logger.Debugf("got terminal error %s for idx %d", res.err, res.tIdx)

			if err == nil || res.tIdx < errPos {
				err = res.err
				errPos = res.tIdx
			}
		} else {
			// if there was no error, we set the txsfltr and we set the
			// txsChaincodeNames and txsUpgradedChaincodes maps
			logger.Debugf("got result for idx %d, code %d", res.tIdx, res.validationCode)

			txsfltr.SetFlag(res.tIdx, res.validationCode)

			if res.validationCode == peer.TxValidationCode_VALID {
				if res.txsChaincodeName != nil {
					txsChaincodeNames[res.tIdx] = res.txsChaincodeName
				}
				if res.txsUpgradedChaincode != nil {
					txsUpgradedChaincodes[res.tIdx] = res.txsUpgradedChaincode
				}
				txidArray[res.tIdx] = res.txid
			}
		}
	}

	return txsfltr, len(transactions), err
}

// allValidated returns false if some of the validation flags have not been set
// during validation
func allValidated(txsfltr ledgerUtil.TxValidationFlags) bool {
	for _, f := range txsfltr {
		if peer.TxValidationCode(f) == peer.TxValidationCode_NOT_VALIDATED {
			return false
		}
	}
	return true
}

func (v *TxValidator) createValidationResponseGossipMsg(block *common.Block, txFlags ledgerUtil.TxValidationFlags) (*gossipproto.GossipMessage, error) {
	data, err := proto.Marshal(newValidationResponse(block, txFlags))
	if err != nil {
		logger.Errorf("[%s] Error serializing validation response with sequence number %d: %s", v.ChainID, block.Header.Number, err)
		return nil, err
	}

	return &gossipproto.GossipMessage{
		Nonce:   0,
		Tag:     gossipproto.GossipMessage_CHAN_AND_ORG,
		Channel: []byte(v.ChainID),
		Content: &gossipproto.GossipMessage_DataMsg{
			DataMsg: &gossipproto.DataMessage{
				Payload: &gossipproto.Payload{
					Data:   data,
					SeqNum: block.Header.Number,
				},
			},
		},
	}, nil
}

func newValidationResponse(block *common.Block, txFlags ledgerUtil.TxValidationFlags) *common.Block {
	utils.InitBlockMetadata(block)
	block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER] = txFlags
	return block
}

type member struct {
	discovery.NetworkMember
	mspID string
}

func (v *TxValidator) getValidators() []*member {
	selfMember := v.getSelf()
	localMSPID := selfMember.mspID
	identityInfo := v.gossip.IdentityInfo()
	mapByID := identityInfo.ByID()

	var peers []*member
	for _, m := range v.gossip.PeersOfChannel(gossipcommon.ChainID(v.ChainID)) {
		if m.Properties == nil {
			logger.Debugf("[%s] Not adding peer [%s] as a validator since it does not have any properties", v.ChainID, m.Endpoint)
			continue
		}

		roles := gossipimpl.Roles(m.Properties.Roles)
		if !roles.HasRole(ledgerconfig.ValidatorRole) && !roles.HasRole(ledgerconfig.CommitterRole) {
			logger.Debugf("[%s] Not adding peer [%s] as a validator since it does not have the committer nor the validator role", v.ChainID, m.Endpoint)
			continue
		}

		identity, ok := mapByID[string(m.PKIid)]
		if !ok {
			logger.Warningf("[%s] Not adding peer [%s] as a validator since unable to determine MSP ID from PKIID for [%s]", v.ChainID, m.Endpoint)
			continue
		}

		mspID := string(identity.Organization)
		if mspID != localMSPID {
			logger.Debugf("[%s] Not adding peer [%s] from MSP [%s] as a validator since it is not part of the local msp [%s]", v.ChainID, m.Endpoint, mspID, localMSPID)
			continue
		}

		peers = append(peers, &member{
			NetworkMember: m,
			mspID:         mspID,
		})
	}

	if ledgerconfig.IsCommitter() || ledgerconfig.IsValidator() {
		peers = append(peers, v.getSelf())
	}

	sortedValidators := validators(peers)
	sort.Sort(sortedValidators)

	return sortedValidators
}

func (v *TxValidator) getSelf() *member {
	self := v.gossip.SelfMembershipInfo()
	self.Properties = &gossipproto.Properties{
		Roles: ledgerconfig.RolesAsString(),
	}

	identityInfo := v.gossip.IdentityInfo()
	mapByID := identityInfo.ByID()

	var mspID string
	selfIdentity, ok := mapByID[string(self.PKIid)]
	if ok {
		mspID = string(selfIdentity.Organization)
	} else {
		logger.Warningf("[%s] Unable to determine MSP ID from PKIID for self", v.ChainID)
	}

	return &member{
		NetworkMember: self,
		mspID:         mspID,
	}
}

func (v *TxValidator) getTxFilter() util.TxFilter {
	validators := v.getValidators()

	if logger.IsEnabledFor(logging.DEBUG) {
		logger.Debugf("[%s] All validators:", v.ChainID)
		for _, m := range validators {
			logger.Debugf("- [%s], MSPID [%s], - Roles: %s", m.Endpoint, m.mspID, m.Properties.Roles)
		}
	}

	selfMember := v.getSelf()

	return func(txIdx int) bool {
		validatorForTx := validators[txIdx%len(validators)]
		logger.Debugf("[%s] Validator for TxIdx [%d] is [%s]", v.ChainID, txIdx, validatorForTx.Endpoint)
		return validatorForTx.Endpoint == selfMember.Endpoint
	}
}

func markTXIdDuplicates(txids []string, txsfltr ledgerUtil.TxValidationFlags) {
	txidMap := make(map[string]struct{})

	for id, txid := range txids {
		if txid == "" {
			continue
		}

		_, in := txidMap[txid]
		if in {
			logger.Error("Duplicate txid", txid, "found, skipping")
			txsfltr.SetFlag(id, peer.TxValidationCode_DUPLICATE_TXID)
		} else {
			txidMap[txid] = struct{}{}
		}
	}
}

func (v *TxValidator) validateTx(req *blockValidationRequest, results chan<- *blockValidationResult) {
	block := req.block
	d := req.d
	tIdx := req.tIdx
	txID := ""

	if d == nil {
		results <- &blockValidationResult{
			tIdx: tIdx,
		}
		return
	}

	if env, err := utils.GetEnvelopeFromBlock(d); err != nil {
		logger.Warningf("Error getting tx from block: %+v", err)
		results <- &blockValidationResult{
			tIdx:           tIdx,
			validationCode: peer.TxValidationCode_INVALID_OTHER_REASON,
		}
		return
	} else if env != nil {
		// validate the transaction: here we check that the transaction
		// is properly formed, properly signed and that the security
		// chain binding proposal to endorsements to tx holds. We do
		// NOT check the validity of endorsements, though. That's a
		// job for VSCC below
		logger.Debugf("[%s] validateTx starts for block %p env %p txn %d", v.ChainID, block, env, tIdx)
		defer logger.Debugf("[%s] validateTx completes for block %p env %p txn %d", v.ChainID, block, env, tIdx)
		var payload *common.Payload
		var err error
		var txResult peer.TxValidationCode
		var txsChaincodeName *sysccprovider.ChaincodeInstance
		var txsUpgradedChaincode *sysccprovider.ChaincodeInstance

		if payload, txResult = validation.ValidateTransaction(env, v.Support.Capabilities()); txResult != peer.TxValidationCode_VALID {
			logger.Errorf("Invalid transaction with index %d", tIdx)
			results <- &blockValidationResult{
				tIdx:           tIdx,
				validationCode: txResult,
			}
			return
		}

		chdr, err := utils.UnmarshalChannelHeader(payload.Header.ChannelHeader)
		if err != nil {
			logger.Warningf("Could not unmarshal channel header, err %s, skipping", err)
			results <- &blockValidationResult{
				tIdx:           tIdx,
				validationCode: peer.TxValidationCode_INVALID_OTHER_REASON,
			}
			return
		}

		channel := chdr.ChannelId
		logger.Debugf("Transaction is for channel %s", channel)

		if !v.chainExists(channel) {
			logger.Errorf("Dropping transaction for non-existent channel %s", channel)
			results <- &blockValidationResult{
				tIdx:           tIdx,
				validationCode: peer.TxValidationCode_TARGET_CHAIN_NOT_FOUND,
			}
			return
		}

		if common.HeaderType(chdr.Type) == common.HeaderType_ENDORSER_TRANSACTION {
			// Check duplicate transactions
			txID = chdr.TxId
			// GetTransactionByID will return:
			_, err := v.Support.Ledger().GetTransactionByID(txID)
			// 1) err == nil => there is already a tx in the ledger with the supplied id
			if err == nil {
				logger.Error("Duplicate transaction found, ", txID, ", skipping")
				results <- &blockValidationResult{
					tIdx:           tIdx,
					validationCode: peer.TxValidationCode_DUPLICATE_TXID,
				}
				return
			}
			// 2) err is not of type blkstorage.NotFoundInIndexErr => we could not verify whether a tx with the supplied id is in the ledger
			if _, isNotFoundInIndexErrType := err.(ledger.NotFoundInIndexErr); !isNotFoundInIndexErrType {
				logger.Errorf("Ledger failure while attempting to detect duplicate status for txid %s, err '%s'. Aborting", txID, err)
				results <- &blockValidationResult{
					tIdx: tIdx,
					err:  err,
				}
				return
			}
			// 3) err is of type blkstorage.NotFoundInIndexErr => there is no tx with the supplied id in the ledger

			// Validate tx with vscc and policy
			logger.Debug("Validating transaction vscc tx validate")
			err, cde := v.Vscc.VSCCValidateTx(tIdx, payload, d, block)
			if err != nil {
				logger.Errorf("VSCCValidateTx for transaction txId = %s returned error: %s", txID, err)
				switch err.(type) {
				case *commonerrors.VSCCExecutionFailureError:
					results <- &blockValidationResult{
						tIdx: tIdx,
						err:  err,
					}
					return
				case *commonerrors.VSCCInfoLookupFailureError:
					results <- &blockValidationResult{
						tIdx: tIdx,
						err:  err,
					}
					return
				default:
					results <- &blockValidationResult{
						tIdx:           tIdx,
						validationCode: cde,
					}
					return
				}
			}

			invokeCC, upgradeCC, err := v.getTxCCInstance(payload)
			if err != nil {
				logger.Errorf("Get chaincode instance from transaction txId = %s returned error: %+v", txID, err)
				results <- &blockValidationResult{
					tIdx:           tIdx,
					validationCode: peer.TxValidationCode_INVALID_OTHER_REASON,
				}
				return
			}
			txsChaincodeName = invokeCC
			if upgradeCC != nil {
				logger.Infof("Find chaincode upgrade transaction for chaincode %s on channel %s with new version %s", upgradeCC.ChaincodeName, upgradeCC.ChainID, upgradeCC.ChaincodeVersion)
				txsUpgradedChaincode = upgradeCC
			}
		} else if common.HeaderType(chdr.Type) == common.HeaderType_CONFIG {
			configEnvelope, err := configtx.UnmarshalConfigEnvelope(payload.Data)
			if err != nil {
				err = errors.WithMessage(err, "error unmarshalling config which passed initial validity checks")
				logger.Criticalf("%+v", err)
				results <- &blockValidationResult{
					tIdx: tIdx,
					err:  err,
				}
				return
			}

			if err := v.Support.Apply(configEnvelope); err != nil {
				err = errors.WithMessage(err, "error validating config which passed initial validity checks")
				logger.Criticalf("%+v", err)
				results <- &blockValidationResult{
					tIdx: tIdx,
					err:  err,
				}
				return
			}
			logger.Debugf("config transaction received for chain %s", channel)
		} else {
			logger.Warningf("Unknown transaction type [%s] in block number [%d] transaction index [%d]",
				common.HeaderType(chdr.Type), block.Header.Number, tIdx)
			results <- &blockValidationResult{
				tIdx:           tIdx,
				validationCode: peer.TxValidationCode_UNKNOWN_TX_TYPE,
			}
			return
		}

		if _, err := proto.Marshal(env); err != nil {
			logger.Warningf("Cannot marshal transaction: %s", err)
			results <- &blockValidationResult{
				tIdx:           tIdx,
				validationCode: peer.TxValidationCode_MARSHAL_TX_ERROR,
			}
			return
		}
		// Succeeded to pass down here, transaction is valid
		results <- &blockValidationResult{
			tIdx:                 tIdx,
			txsChaincodeName:     txsChaincodeName,
			txsUpgradedChaincode: txsUpgradedChaincode,
			validationCode:       peer.TxValidationCode_VALID,
			txid:                 txID,
		}
		return
	} else {
		logger.Warning("Nil tx from block")
		results <- &blockValidationResult{
			tIdx:           tIdx,
			validationCode: peer.TxValidationCode_NIL_ENVELOPE,
		}
		return
	}
}

// generateCCKey generates a unique identifier for chaincode in specific channel
func (v *TxValidator) generateCCKey(ccName, chainID string) string {
	return fmt.Sprintf("%s/%s", ccName, chainID)
}

// invalidTXsForUpgradeCC invalid all txs that should be invalided because of chaincode upgrade txs
func (v *TxValidator) invalidTXsForUpgradeCC(txsChaincodeNames map[int]*sysccprovider.ChaincodeInstance, txsUpgradedChaincodes map[int]*sysccprovider.ChaincodeInstance, txsfltr ledgerUtil.TxValidationFlags) {
	if len(txsUpgradedChaincodes) == 0 {
		return
	}

	// Invalid former cc upgrade txs if there're two or more txs upgrade the same cc
	finalValidUpgradeTXs := make(map[string]int)
	upgradedChaincodes := make(map[string]*sysccprovider.ChaincodeInstance)
	for tIdx, cc := range txsUpgradedChaincodes {
		if cc == nil {
			continue
		}
		upgradedCCKey := v.generateCCKey(cc.ChaincodeName, cc.ChainID)

		if finalIdx, exist := finalValidUpgradeTXs[upgradedCCKey]; !exist {
			finalValidUpgradeTXs[upgradedCCKey] = tIdx
			upgradedChaincodes[upgradedCCKey] = cc
		} else if finalIdx < tIdx {
			logger.Infof("Invalid transaction with index %d: chaincode was upgraded by latter tx", finalIdx)
			txsfltr.SetFlag(finalIdx, peer.TxValidationCode_CHAINCODE_VERSION_CONFLICT)

			// record latter cc upgrade tx info
			finalValidUpgradeTXs[upgradedCCKey] = tIdx
			upgradedChaincodes[upgradedCCKey] = cc
		} else {
			logger.Infof("Invalid transaction with index %d: chaincode was upgraded by latter tx", tIdx)
			txsfltr.SetFlag(tIdx, peer.TxValidationCode_CHAINCODE_VERSION_CONFLICT)
		}
	}

	// invalid txs which invoke the upgraded chaincodes
	for tIdx, cc := range txsChaincodeNames {
		if cc == nil {
			continue
		}
		ccKey := v.generateCCKey(cc.ChaincodeName, cc.ChainID)
		if _, exist := upgradedChaincodes[ccKey]; exist {
			if txsfltr.IsValid(tIdx) {
				logger.Infof("Invalid transaction with index %d: chaincode was upgraded in the same block", tIdx)
				txsfltr.SetFlag(tIdx, peer.TxValidationCode_CHAINCODE_VERSION_CONFLICT)
			}
		}
	}
}

func (v *TxValidator) getTxCCInstance(payload *common.Payload) (invokeCCIns, upgradeCCIns *sysccprovider.ChaincodeInstance, err error) {
	// This is duplicated unpacking work, but make test easier.
	chdr, err := utils.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return nil, nil, err
	}

	// Chain ID
	chainID := chdr.ChannelId // it is guaranteed to be an existing channel by now

	// ChaincodeID
	hdrExt, err := utils.GetChaincodeHeaderExtension(payload.Header)
	if err != nil {
		return nil, nil, err
	}
	invokeCC := hdrExt.ChaincodeId
	invokeIns := &sysccprovider.ChaincodeInstance{ChainID: chainID, ChaincodeName: invokeCC.Name, ChaincodeVersion: invokeCC.Version}

	// Transaction
	tx, err := utils.GetTransaction(payload.Data)
	if err != nil {
		logger.Errorf("GetTransaction failed: %+v", err)
		return invokeIns, nil, nil
	}

	// ChaincodeActionPayload
	cap, err := utils.GetChaincodeActionPayload(tx.Actions[0].Payload)
	if err != nil {
		logger.Errorf("GetChaincodeActionPayload failed: %+v", err)
		return invokeIns, nil, nil
	}

	// ChaincodeProposalPayload
	cpp, err := utils.GetChaincodeProposalPayload(cap.ChaincodeProposalPayload)
	if err != nil {
		logger.Errorf("GetChaincodeProposalPayload failed: %+v", err)
		return invokeIns, nil, nil
	}

	// ChaincodeInvocationSpec
	cis := &peer.ChaincodeInvocationSpec{}
	err = proto.Unmarshal(cpp.Input, cis)
	if err != nil {
		logger.Errorf("GetChaincodeInvokeSpec failed: %+v", err)
		return invokeIns, nil, nil
	}

	if invokeCC.Name == "lscc" {
		if string(cis.ChaincodeSpec.Input.Args[0]) == "upgrade" {
			upgradeIns, err := v.getUpgradeTxInstance(chainID, cis.ChaincodeSpec.Input.Args[2])
			if err != nil {
				return invokeIns, nil, nil
			}
			return invokeIns, upgradeIns, nil
		}
	}

	return invokeIns, nil, nil
}

func (v *TxValidator) getUpgradeTxInstance(chainID string, cdsBytes []byte) (*sysccprovider.ChaincodeInstance, error) {
	cds, err := utils.GetChaincodeDeploymentSpec(cdsBytes)
	if err != nil {
		return nil, err
	}

	return &sysccprovider.ChaincodeInstance{
		ChainID:          chainID,
		ChaincodeName:    cds.ChaincodeSpec.ChaincodeId.Name,
		ChaincodeVersion: cds.ChaincodeSpec.ChaincodeId.Version,
	}, nil
}

type dynamicDeserializer struct {
	support Support
}

func (ds *dynamicDeserializer) DeserializeIdentity(serializedIdentity []byte) (msp.Identity, error) {
	return ds.support.MSPManager().DeserializeIdentity(serializedIdentity)
}

func (ds *dynamicDeserializer) IsWellFormed(identity *mspprotos.SerializedIdentity) error {
	return ds.support.MSPManager().IsWellFormed(identity)
}

type dynamicCapabilities struct {
	support Support
}

func (ds *dynamicCapabilities) ACLs() bool {
	return ds.support.Capabilities().ACLs()
}

func (ds *dynamicCapabilities) CollectionUpgrade() bool {
	return ds.support.Capabilities().CollectionUpgrade()
}

func (ds *dynamicCapabilities) ForbidDuplicateTXIdInBlock() bool {
	return ds.support.Capabilities().ForbidDuplicateTXIdInBlock()
}

func (ds *dynamicCapabilities) MetadataLifecycle() bool {
	return ds.support.Capabilities().MetadataLifecycle()
}

func (ds *dynamicCapabilities) PrivateChannelData() bool {
	return ds.support.Capabilities().PrivateChannelData()
}

func (ds *dynamicCapabilities) Supported() error {
	return ds.support.Capabilities().Supported()
}

func (ds *dynamicCapabilities) V1_1Validation() bool {
	return ds.support.Capabilities().V1_1Validation()
}

func (ds *dynamicCapabilities) V1_2Validation() bool {
	return ds.support.Capabilities().V1_2Validation()
}

type validators []*member

func (p validators) Len() int {
	return len(p)
}

func (p validators) Less(i, j int) bool {
	// Committers should always come first
	if p.isCommiter(i) {
		return true
	}
	if p.isCommiter(j) {
		return false
	}
	return p[i].Endpoint < p[j].Endpoint
}

func (p validators) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func (p validators) isCommiter(i int) bool {
	if p[i].Properties == nil {
		return false
	}
	roles := gossipimpl.Roles(p[i].Properties.Roles)
	return roles.HasRole(ledgerconfig.CommitterRole)
}

func getValidationWaitTime(numTransactions int) time.Duration {
	minWaitTime := ledgerconfig.GetValidationMinWaitTime()
	waitTime := time.Duration(numTransactions) * ledgerconfig.GetValidationWaitTimePerTx()
	if waitTime < minWaitTime {
		return minWaitTime
	}
	return waitTime
}

// flagsToString used in debugging
func flagsToString(flags ledgerUtil.TxValidationFlags) string {
	str := "["
	for i := range flags {
		str += fmt.Sprintf("[%d]=[%s]", i, flags.Flag(i))
		if i+1 < len(flags) {
			str += ","
		}
	}
	return str + "]"
}
