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
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/validator/internal"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/validator/statebasedval"
	"github.com/hyperledger/fabric/core/ledger/util"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
	"golang.org/x/net/context"
)

var logger = flogging.MustGetLogger("valimpl")

// DefaultImpl implements the interface validator.Validator
// This performs the common tasks that are independent of a particular scheme of validation
// and for actual validation of the public rwset, it encloses an internal validator (that implements interface
// internal.InternalValidator) such as statebased validator
type DefaultImpl struct {
	txmgr txmgr.TxMgr
	db    privacyenabledstate.DB
	internal.Validator
}

// NewStatebasedValidator constructs a validator that internally manages statebased validator and in addition
// handles the tasks that are agnostic to a particular validation scheme such as parsing the block and handling the pvt data
func NewStatebasedValidator(channelID string, txmgr txmgr.TxMgr, db privacyenabledstate.DB) validator.Validator {
	return &DefaultImpl{txmgr, db, statebasedval.NewValidator(channelID, db)}
}

// ValidateMVCC validates block for MVCC conflicts and phantom reads against committed data
func (impl *DefaultImpl) ValidateMVCC(ctx context.Context, block *common.Block, txsFilter util.TxValidationFlags, acceptTx util.TxFilter) error {
	logger.Debugf("ValidateMVCC - Block number = [%d]", block.Header.Number)

	internalBlock, _, err := preprocessProtoBlock(impl.txmgr, impl.db.ValidateKeyValue, block, true)
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

	if err = impl.Validator.ValidateMVCC(ctx, internalBlock, txsFilter, acceptTx); err != nil {
		return err
	}

	postprocessProtoBlock(block, internalBlock)
	logger.Debugf("ValidateMVCC completed for block %d", block.Header.Number)

	return nil
}

// ValidateAndPrepareBatch implements the function in interface validator.Validator
func (impl *DefaultImpl) ValidateAndPrepareBatch(blockAndPvtdata *ledger.BlockAndPvtData, doMVCCValidation bool) (*privacyenabledstate.UpdateBatch, []*txmgr.TxStatInfo, error) {

	block := blockAndPvtdata.Block
	logger.Debugf("ValidateAndPrepareBatch() for block number = [%d]", block.Header.Number)
	var internalBlock *internal.Block
	var txsStatInfo []*txmgr.TxStatInfo
	var pubAndHashUpdates *internal.PubAndHashUpdates
	var pvtUpdates *privacyenabledstate.PvtUpdateBatch
	var err error

	logger.Debug("preprocessing ProtoBlock...")
	if internalBlock, txsStatInfo, err = preprocessProtoBlock(impl.txmgr, impl.db.ValidateKeyValue, block, doMVCCValidation); err != nil {
		return nil, nil, err
	}

	if pubAndHashUpdates, err = impl.Validator.ValidateAndPrepareBatch(internalBlock, doMVCCValidation, blockAndPvtdata.PvtData); err != nil {
		return nil, nil, err
	}
	logger.Debug("validating rwset...")
	if pvtUpdates, err = validateAndPreparePvtBatch(internalBlock, impl.db, pubAndHashUpdates, blockAndPvtdata.PvtData); err != nil {
		return nil, nil, err
	}
	logger.Debug("postprocessing ProtoBlock...")
	postprocessProtoBlock(block, internalBlock)
	logger.Debug("ValidateAndPrepareBatch() complete")

	txsFilter := util.TxValidationFlags(block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER])
	for i := range txsFilter {
		txsStatInfo[i].ValidationCode = txsFilter.Flag(i)
	}
	return &privacyenabledstate.UpdateBatch{
		PubUpdates:  pubAndHashUpdates.PubUpdates,
		HashUpdates: pubAndHashUpdates.HashUpdates,
		PvtUpdates:  pvtUpdates,
	}, txsStatInfo, nil
}
