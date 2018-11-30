/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/
package fileledger

import (
	"github.com/hyperledger/fabric/common/ledger/blkstorage"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
)

// TODO: Refactor index out of fsblockstorage and then use that extracted package.

type noopIndexProvider struct {}
type noopIndex struct {}
type noopTxLoc struct {}

func (p *noopIndexProvider) OpenBlockIndex(ledgerid string) (blkstorage.BlockIndex, error) {
	return &noopIndex{}, nil
}
func (p *noopIndexProvider) Exists(ledgerid string) (bool, error) {
	return true, nil
}
func (p *noopIndexProvider) List() ([]string, error) {
	return []string{}, nil
}
func (p *noopIndexProvider) Close() {
}

func (p *noopIndex) AddBlock(block *common.Block) error {
	return nil
}
func (i *noopIndex) Shutdown() {
}

func (i *noopIndex) RetrieveTxLoc(txID string) (blkstorage.TxLoc, error) {
	return &noopTxLoc{}, nil
}
func (i *noopIndex) RetrieveTxValidationCodeByTxID(txID string) (peer.TxValidationCode, error) {
	return peer.TxValidationCode_INVALID_OTHER_REASON, nil
}

func (t *noopTxLoc) BlockNumber() uint64 {
	return 0
}
func (t *noopTxLoc) TxNumber() uint64 {
	return 0
}