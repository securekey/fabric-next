/*
Copyright IBM Corp. 2016 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package txvalidator

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/common/configtx"
	commonerrors "github.com/hyperledger/fabric/common/errors"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/chaincode/platforms"
	"github.com/hyperledger/fabric/core/chaincode/platforms/golang"
	"github.com/hyperledger/fabric/core/common/sysccprovider"
	"github.com/hyperledger/fabric/core/common/validation"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/util"
	ledgerUtil "github.com/hyperledger/fabric/core/ledger/util"
	"github.com/hyperledger/fabric/gossip/comm"
	gossip2 "github.com/hyperledger/fabric/gossip/gossip"
	gossipimpl "github.com/hyperledger/fabric/gossip/gossip"
	"github.com/hyperledger/fabric/gossip/roleutil"
	"github.com/hyperledger/fabric/msp"
	"github.com/hyperledger/fabric/protos/common"
	gossipproto "github.com/hyperledger/fabric/protos/gossip"
	mspprotos "github.com/hyperledger/fabric/protos/msp"
	"github.com/hyperledger/fabric/protos/peer"
	"github.com/hyperledger/fabric/protos/utils"
	"github.com/pkg/errors"
	"sync"
	"sort"
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
	ValidatePartial(ctx context.Context, block *common.Block)
}

// private interface to decouple tx validator
// and vscc execution, in order to increase
// testability of TxValidator
type vsccValidator interface {
	VSCCValidateTx(seq int, payload *common.Payload, envBytes []byte, block *common.Block) (error, peer.TxValidationCode)
}

type mvccValidator interface {
	ValidateMVCC(ctx context.Context, block *common.Block, txFlags util.TxValidationFlags, filter util.TxFilter) error
}
// implementation of Validator interface, keeps
// reference to the ledger to enable tx simulation
// and execution of vscc
type TxValidator struct {
	ChainID string
	Support Support
	Vscc    vsccValidator
	gossip        gossip2.Gossip
	mvccValidator mvccValidator
	roleUtil      *roleutil.RoleUtil
}

var logger = flogging.MustGetLogger("committer.txvalidator")
// ignoreCancel is a cancel function that does nothing
var ignoreCancel = func() {}

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
		roleUtil:      roleutil.NewRoleUtil(chainID, gossip),
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
	// Endpoint is the endpoint of the peer that provided the results.
	// Empty means local peer.
	Endpoint string
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

	txFlags, numValidated, err := v.validate(context.Background(), block, v.getTxFilter())
	if err == nil {
		flags := newTxFlags(block.Header.Number, txsfltr)
		done := flags.merge(txFlags)
		if done {
			logger.Debugf("[%s] Committer has validated all %d transactions in block %d", v.ChainID, len(block.Data.Data), block.Header.Number)
		} else {
			err = v.waitForValidationResults(ignoreCancel, block.Header.Number, flags, resultsChan, getValidationWaitTime(numValidated))
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
				ctx, cancel := context.WithCancel(context.Background())

				// Haven't received results for some of the transactions. Validate the remaining ones.
				go v.validateRemaining(ctx, block, notValidated, resultsChan)

				// Wait forever for a response
				err = v.waitForValidationResults(cancel, block.Header.Number, flags, resultsChan, time.Hour)
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
func (v *TxValidator) ValidatePartial(ctx context.Context, block *common.Block) {
	committer, err := v.roleUtil.Committer(false)
	if err != nil {
		logger.Errorf("[%s] Unable to get the committing peer to send the validation response to: %s", v.ChainID, err)
		return
	}

	txFlags, numValidated, err := v.validate(ctx, block, v.getTxFilter())
	if err != nil {
		// Error while validating. Don't send the result over Gossip - in this case the committer will
		// revalidate the unvalidated transactions.
		logger.Infof("[%s] Got error in validation of block %d: %s", v.ChainID, block.Header.Number, err)
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

	logger.Debugf("[%s] ... gossiping validation response for %d transactions in block %d to the committer: [%s]", v.ChainID, numValidated, block.Header.Number, committer.Endpoint)

	v.gossip.Send(msg, &comm.RemotePeer{
		Endpoint: committer.Endpoint,
		PKIID:    committer.PKIid,
	})
}

func (v *TxValidator) validateRemaining(ctx context.Context, block *common.Block, notValidated map[int]struct{}, resultsChan chan *ValidationResults) {
	txFlags, numValidated, err := v.validate(ctx, block,
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

func (v *TxValidator) waitForValidationResults(cancel context.CancelFunc, blockNumber uint64, flags *txFlags, resultsChan chan *ValidationResults, timeout time.Duration) error {
	logger.Debugf("[%s] Waiting up to %s for validation responses for block %d ...", v.ChainID, timeout, blockNumber)

	start := time.Now()
	timeoutChan := time.After(timeout)

	for {
		select {
		case result := <-resultsChan:
			// FIXME: Change to Debug
			logger.Infof("[%s] Got results from [%s] for block %d after %s", v.ChainID, result.Endpoint, result.BlockNumber, time.Since(start))

			done, err := v.handleResults(blockNumber, flags, result)
			if err != nil {
				logger.Infof("[%s] Received error in validation results from [%s] peer for block %d: %s", v.ChainID, result.Endpoint, result.BlockNumber, err)
				return err
			}

			if done {
				// Cancel any background validations
				cancel()

				// FIXME: Change to Debug
				logger.Infof("[%s] Block %d is all validated. Done waiting %s for responses.", v.ChainID, blockNumber, time.Since(start))
				return nil
			}
		case <-timeoutChan:
			// FIXME: Change to Debug
			logger.Infof("[%s] Timed out after %s waiting for validation response for block %d", v.ChainID, timeout, blockNumber)
			return nil
		}
	}
}

func (v *TxValidator) handleResults(blockNumber uint64, flags *txFlags, result *ValidationResults) (done bool, err error) {
	if result.BlockNumber < blockNumber {
		logger.Debugf("[%s] Discarding validation results from [%s] peer for block %d since we're waiting on block %d", v.ChainID, result.Endpoint, result.BlockNumber, blockNumber)
		return false, nil
	}

	if result.BlockNumber > blockNumber {
		// This shouldn't be possible
		logger.Warningf("[%s] Discarding validation results from [%s] peer for block %d since we're waiting on block %d", v.ChainID, result.Endpoint, result.BlockNumber, blockNumber)
		return false, nil
	}

	if result.Err != nil {
		if result.Err == context.Canceled {
			// Ignore this error
			logger.Debugf("[%s] Validation was canceled in [%s] peer for block %d", v.ChainID, result.Endpoint, result.BlockNumber)
			return false, nil
		}
		return true, result.Err
	}

	return flags.merge(result.TxFlags), nil
}

type txFlags struct {
	mutex       sync.Mutex
	flags       ledgerUtil.TxValidationFlags
	blockNumber uint64
}

func newTxFlags(blockNumber uint64, flags ledgerUtil.TxValidationFlags) *txFlags {
	return &txFlags{blockNumber: blockNumber, flags: flags}
}

// merge merges the given flags and returns true if all of the flags have been validated
func (f *txFlags) merge(source ledgerUtil.TxValidationFlags) bool {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	for i, flag := range source {
		if peer.TxValidationCode(flag) == peer.TxValidationCode_NOT_VALIDATED {
			continue
		}
		currentFlag := f.flags.Flag(i)
		if currentFlag == peer.TxValidationCode_NOT_VALIDATED {
			f.flags.SetFlag(i, peer.TxValidationCode(flag))
		} else {
			logger.Debugf("TxValidation flag at index [%d] for block number %d is already set to %s and attempting to set it to %s. The flag will not be changed.", i, f.blockNumber, currentFlag, peer.TxValidationCode(flag))
		}
	}
	return allValidated(f.flags)
}

func (v *TxValidator) validate(ctx context.Context, block *common.Block, shouldValidate util.TxFilter) (ledgerUtil.TxValidationFlags, int, error) {
	// First phase validation includes validating the block for proper structure, no duplicate transactions, signatures.
	logger.Debugf("[%s] Starting phase 1 validation of block %d ...", v.ChainID, block.Header.Number)
	txFlags, numValidated, err := v.validateBlock(ctx, block, shouldValidate)
	if logger.IsEnabledFor(logging.DEBUG) {
		logger.Debugf("[%s] ... finished phase 1 validation of block %d. Flags: %s", v.ChainID, block.Header.Number, flagsToString(txFlags))
	}
	if err != nil {
		return nil, 0, err
	}

	// Second phase validation validates the transactions for MVCC conflicts against committed data.
	logger.Debugf("[%s] Starting phase 2 validation of block %d ...", v.ChainID, block.Header.Number)
	err = v.mvccValidator.ValidateMVCC(ctx, block, txFlags, shouldValidate)
	logger.Debugf("[%s] ... finished validation of block %d.", v.ChainID, block.Header.Number)
	if err != nil {
		return nil, 0, err
	}

	if logger.IsEnabledFor(logging.DEBUG) {
		logger.Debugf("[%s] Returning flags from phase1 validation of block %d: %s", v.ChainID, block.Header.Number, flagsToString(txFlags))
	}

	return txFlags, numValidated, nil
}

func (v *TxValidator) validateBlock(ctx context.Context, block *common.Block, shouldValidate util.TxFilter) (ledgerUtil.TxValidationFlags, int, error) {
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

	results := make(chan *blockValidationResult, 10)

	go func() {
		n := 0
		for tIdx, d := range block.Data.Data {
			_, ok := transactions[tIdx]
			if !ok {
				continue
			}

			// ensure that we don't have too many concurrent validation workers
			err := v.Support.Acquire(ctx, 1)
			if err != nil {
				// Probably canceled
				// FIXME: Change to Debug
				logger.Infof("Unable to acquire semaphore after submitting %d of %d validation requests for block %d: %s", n, len(transactions), block.Header.Number, err)

				// Send error responses for the remaining transactions
				for ; n < len(transactions); n++ {
					results <- &blockValidationResult{err: err}
				}
				return
			}

			n++

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
			if err == nil || res.tIdx < errPos {
				err = res.err
				errPos = res.tIdx

				if err == context.Canceled {
					// FIXME: Change to Debug
					logger.Infof("Validation of block %d was canceled", block.Header.Number)
				} else {
					logger.Warningf("Got error %s for idx %d", err, res.tIdx)
				}
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
	return &gossipproto.GossipMessage{
		Nonce:   0,
		Tag:     gossipproto.GossipMessage_CHAN_AND_ORG,
		Channel: []byte(v.ChainID),
		Content: &gossipproto.GossipMessage_ValidationResultsMsg{
			ValidationResultsMsg: &gossipproto.ValidationResultsMessage{
				SeqNum:  block.Header.Number,
				TxFlags: txFlags,
			},
		},
	}, nil
}

func (v *TxValidator) getTxFilter() util.TxFilter {
	sortedValidators := validators(v.roleUtil.Validators(true))
	sort.Sort(sortedValidators)

	if logger.IsEnabledFor(logging.DEBUG) {
		logger.Debugf("[%s] All validators:", v.ChainID)
		for _, m := range sortedValidators {
			logger.Debugf("- [%s], MSPID [%s], - Roles: %s", m.Endpoint, m.MSPID, m.Properties.Roles)
		}
	}

	return func(txIdx int) bool {
		validatorForTx := sortedValidators[txIdx%len(sortedValidators)]
		logger.Debugf("[%s] Validator for TxIdx [%d] is [%s]", v.ChainID, txIdx, validatorForTx.Endpoint)
		return validatorForTx.Local
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

			txID = chdr.TxId

			// Check duplicate transactions
			erroneousResultEntry := v.checkTxIdDupsLedger(tIdx, chdr, v.Support.Ledger())
			if erroneousResultEntry != nil {
				results <- erroneousResultEntry
				return
			}

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
			// FAB-12971 comment out below block before v1.4 cut. Will uncomment after v1.4.
			/*
				} else if common.HeaderType(chdr.Type) == common.HeaderType_TOKEN_TRANSACTION {

					txID = chdr.TxId
					if !v.Support.Capabilities().FabToken() {
						logger.Errorf("FabToken capability is not enabled. Unsupported transaction type [%s] in block [%d] transaction [%d]",
							common.HeaderType(chdr.Type), block.Header.Number, tIdx)
						results <- &blockValidationResult{
							tIdx:           tIdx,
							validationCode: peer.TxValidationCode_UNSUPPORTED_TX_PAYLOAD,
						}
						return
					}

					// Check if there is a duplicate of such transaction in the ledger and
					// obtain the corresponding result that acknowledges the error type
					erroneousResultEntry := v.checkTxIdDupsLedger(tIdx, chdr, v.Support.Ledger())
					if erroneousResultEntry != nil {
						results <- erroneousResultEntry
						return
					}

					// Set the namespace of the invocation field
					txsChaincodeName = &sysccprovider.ChaincodeInstance{
						ChainID:          channel,
						ChaincodeName:    "Token",
						ChaincodeVersion: ""}
			*/
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

// CheckTxIdDupsLedger returns a vlockValidationResult enhanced with the respective
// error codes if and only if there is transaction with the same transaction identifier
// in the ledger or no decision can be made for whether such transaction exists;
// the function returns nil if it has ensured that there is no such duplicate, such
// that its consumer can proceed with the transaction processing
func (v *TxValidator) checkTxIdDupsLedger(tIdx int, chdr *common.ChannelHeader, ldgr ledger.PeerLedger) (errorTuple *blockValidationResult) {

	// Retrieve the transaction identifier of the input header
	txID := chdr.TxId

	// Look for a transaction with the same identifier inside the ledger
	_, err := ldgr.GetTransactionByID(txID)

	// if returned error is nil, it means that there is already a tx in
	// the ledger with the supplied id
	if err == nil {
		logger.Error("Duplicate transaction found, ", txID, ", skipping")
		return &blockValidationResult{
			tIdx:           tIdx,
			validationCode: peer.TxValidationCode_DUPLICATE_TXID,
		}
	}

	// if returned error is not of type blkstorage.NotFoundInIndexErr, it means
	// we could not verify whether a tx with the supplied id is in the ledger
	if _, isNotFoundInIndexErrType := err.(ledger.NotFoundInIndexErr); !isNotFoundInIndexErrType {
		logger.Errorf("Ledger failure while attempting to detect duplicate status for "+
			"txid %s, err '%s'. Aborting", txID, err)
		return &blockValidationResult{
			tIdx: tIdx,
			err:  err,
		}
	}

	// it otherwise means that there is no transaction with the same identifier
	// residing in the ledger
	return nil
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
	cds, err := utils.GetChaincodeDeploymentSpec(cdsBytes, platforms.NewRegistry(&golang.Platform{}))
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

// FabToken returns true if fabric token function is supported.
func (ds *dynamicCapabilities) FabToken() bool {
	return ds.support.Capabilities().FabToken()
}

func (ds *dynamicCapabilities) ForbidDuplicateTXIdInBlock() bool {
	return ds.support.Capabilities().ForbidDuplicateTXIdInBlock()
}

func (ds *dynamicCapabilities) KeyLevelEndorsement() bool {
	return ds.support.Capabilities().KeyLevelEndorsement()
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

func (ds *dynamicCapabilities) V1_3Validation() bool {
	return ds.support.Capabilities().V1_3Validation()
}
