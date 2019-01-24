/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package txvalidator

import (
	"sync"

	"github.com/hyperledger/fabric/core/ledger/util"
	ledgerUtil "github.com/hyperledger/fabric/core/ledger/util"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
)

type txFlags struct {
	mutex       sync.RWMutex
	flags       util.TxValidationFlags
	blockNumber uint64
}

func newTxFlags(block *common.Block) *txFlags {
	// Copy the current flags from the block
	flags := ledgerUtil.NewTxValidationFlags(len(block.Data.Data))
	currentFlags := util.TxValidationFlags(block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER])
	for i := range block.Data.Data {
		flags.SetFlag(i, currentFlags.Flag(i))
	}
	return &txFlags{blockNumber: block.Header.Number, flags: flags}
}

// merge merges the given flags and returns true if all of the flags have been validated
func (f *txFlags) merge(source util.TxValidationFlags) bool {
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
			logger.Debugf("Not setting TxValidation flag at index [%d] for block number %d since it is already set to %s.", i, f.blockNumber, currentFlag)
		}
	}

	return f.allValidatedNoLock()
}

// markTXIdDuplicates marks a transaction that has a duplicate ID from a previous transaction in the block as invalid
func (f *txFlags) markTXIdDuplicates(txids []string) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	txidMap := make(map[string]struct{})
	for id, txid := range txids {
		if txid == "" {
			continue
		}

		_, in := txidMap[txid]
		if in {
			logger.Error("Duplicate txid", txid, "found, skipping")
			f.flags.SetFlag(id, peer.TxValidationCode_DUPLICATE_TXID)
		} else {
			txidMap[txid] = struct{}{}
		}
	}
}

// unvalidatedMap returns a map of TX indexes of the transaction that are not yet validated
func (f *txFlags) unvalidatedMap() map[int]struct{} {
	f.mutex.RLock()
	defer f.mutex.RUnlock()

	notValidated := make(map[int]struct{})
	for i, flag := range f.flags {
		if peer.TxValidationCode(flag) == peer.TxValidationCode_NOT_VALIDATED {
			notValidated[i] = struct{}{}
		}
	}
	return notValidated
}

// allValidated returns true if none of the flags is set to NOT_VALIDATED
func (f *txFlags) allValidated() bool {
	f.mutex.RLock()
	defer f.mutex.RUnlock()
	return f.allValidatedNoLock()
}

// value returns the validation flags
func (f *txFlags) value() util.TxValidationFlags {
	return f.flags
}

// allValidatedNoLock returns true if none of the flags is set to NOT_VALIDATED
func (f *txFlags) allValidatedNoLock() bool {
	for i, txStatus := range f.flags {
		if peer.TxValidationCode(txStatus) == peer.TxValidationCode_NOT_VALIDATED {
			// FIXME: Change to Debug
			logger.Infof("Still waiting for validation results for block %d, TxIdx [%d]", f.blockNumber, i)
			return false
		}
	}
	// FIXME: Change to Debug
	logger.Infof("Block %d is all validated", f.blockNumber)
	return true
}
