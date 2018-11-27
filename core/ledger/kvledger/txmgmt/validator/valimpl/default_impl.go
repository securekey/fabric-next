/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package valimpl

import (
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/privacyenabledstate"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/txmgr"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/validator"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/validator/statebasedval"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/validator/valinternal"
	"github.com/hyperledger/fabric/core/ledger/util"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
)

var logger = flogging.MustGetLogger("valimpl")

// DefaultImpl implements the interface validator.Validator
// This performs the common tasks that are independent of a particular scheme of validation
// and for actual validation of the public rwset, it encloses an internal validator (that implements interface
// valinternal.InternalValidator) such as statebased validator
type DefaultImpl struct {
	txmgr txmgr.TxMgr
	db    privacyenabledstate.DB
	valinternal.InternalValidator
}

// NewStatebasedValidator constructs a validator that internally manages statebased validator and in addition
// handles the tasks that are agnostic to a particular validation scheme such as parsing the block and handling the pvt data
func NewStatebasedValidator(channelID string, txmgr txmgr.TxMgr, db privacyenabledstate.DB) validator.Validator {
	return &DefaultImpl{txmgr, db, statebasedval.NewValidator(channelID, db)}
}

// ValidateMVCC validates block for MVCC conflicts and phantom reads against committed data
func (impl *DefaultImpl) ValidateMVCC(block *common.Block, txsFilter util.TxValidationFlags, doMVCCValidation bool, acceptTx util.TxFilter) error {
	logger.Debugf("ValidateMVCC - Block number = [%d]", block.Header.Number)

	internalBlock, err := preprocessProtoBlock(impl.txmgr, impl.db.ValidateKeyValue, block, doMVCCValidation, txsFilter)
	if err != nil {
		return err
	}

	for txIndex := range block.Data.Data {
		if txsFilter.IsValid(txIndex) {
			// Mark the transaction as not validated so that we know that the first phase (distributed) validation
			// has completed validating all transactions and that none are missed
			txsFilter.SetFlag(txIndex, peer.TxValidationCode_NOT_VALIDATED)
		}
	}

	if err = impl.InternalValidator.ValidateMVCC(internalBlock, txsFilter, doMVCCValidation, acceptTx); err != nil {
		return err
	}

	postprocessProtoBlock(block, txsFilter, internalBlock, acceptTx)
	logger.Debugf("ValidateMVCC completed for block %d", block.Header.Number)

	return nil
}

// ValidateAndPrepareBatch implements the function in interface validator.Validator
func (impl *DefaultImpl) ValidateAndPrepareBatch(blockAndPvtdata *ledger.BlockAndPvtData,
	doMVCCValidation bool) (*privacyenabledstate.UpdateBatch, error) {
	block := blockAndPvtdata.Block
	logger.Debugf("ValidateAndPrepareBatch() for block number = [%d]", block.Header.Number)
	var internalBlock *valinternal.Block
	var pubAndHashUpdates *valinternal.PubAndHashUpdates
	var pvtUpdates *privacyenabledstate.PvtUpdateBatch
	var err error

	txsFilter := util.TxValidationFlags(block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER])

	logger.Debugf("preprocessing ProtoBlock for block %d...", block.Header.Number)
	if internalBlock, err = preprocessProtoBlock(impl.txmgr, impl.db.ValidateKeyValue, block, doMVCCValidation, txsFilter); err != nil {
		return nil, err
	}

	if pubAndHashUpdates, err = impl.InternalValidator.ValidateAndPrepareBatch(internalBlock, doMVCCValidation, blockAndPvtdata.BlockPvtData); err != nil {
		return nil, err
	}
	logger.Debugf("validating rwset for block %d...", block.Header.Number)
	if pvtUpdates, err = validateAndPreparePvtBatch(internalBlock, blockAndPvtdata.BlockPvtData); err != nil {
		return nil, err
	}
	logger.Debugf("postprocessing ProtoBlock for block %d...", block.Header.Number)
	postprocessProtoBlock(block, txsFilter, internalBlock, util.TxFilterAcceptAll)
	logger.Debugf("ValidateAndPrepareBatch() for block %d complete", block.Header.Number)
	return &privacyenabledstate.UpdateBatch{
		PubUpdates:  pubAndHashUpdates.PubUpdates,
		HashUpdates: pubAndHashUpdates.HashUpdates,
		PvtUpdates:  pvtUpdates,
	}, nil
}
