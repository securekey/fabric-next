/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package txmgr

import (
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/core/ledger/util"
	"github.com/hyperledger/fabric/protos/common"
	"golang.org/x/net/context"
)

// TxMgr - an interface that a transaction manager should implement
type TxMgr interface {
	NewQueryExecutor(txid string) (ledger.QueryExecutor, error)
	NewTxSimulator(txid string) (ledger.TxSimulator, error)
	ValidateMVCC(ctx context.Context, block *common.Block, txFlags util.TxValidationFlags, filter util.TxFilter) error
	ValidateAndPrepare(blockAndPvtdata *ledger.BlockAndPvtData, doMVCCValidation bool) error
	GetLastSavepoint() (*version.Height, error)
	ShouldRecover(lastAvailableBlock uint64) (bool, uint64, error)
	CommitLostBlock(blockAndPvtdata *ledger.BlockAndPvtData) error
	Commit(blockAndPvtdata *ledger.BlockAndPvtData) error
	Rollback(blockAndPvtdata *ledger.BlockAndPvtData)
	Shutdown()
}

//LockBasedTxMgr - an extension of TxMgr interface which allows to lock/unlock txmgr rwlock
type LockBasedTxMgr interface {
	TxMgr
	RLock()
	RUnlock()
	Lock()
	Unlock()
}

// ErrUnsupportedTransaction is expected to be thrown if a unsupported query is performed in an update transaction
type ErrUnsupportedTransaction struct {
	Msg string
}

func (e *ErrUnsupportedTransaction) Error() string {
	return e.Msg
}

// ErrPvtdataNotAvailable is to be thrown when an application seeks a private data item
// during simulation and the simulator is not capable of returning the version of the
// private data item consistent with the snapshopt exposed to the simulation
type ErrPvtdataNotAvailable struct {
	Msg string
}

func (e *ErrPvtdataNotAvailable) Error() string {
	return e.Msg
}
