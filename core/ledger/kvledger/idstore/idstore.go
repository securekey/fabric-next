/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package idstore

import "github.com/hyperledger/fabric/protos/common"

type IDStore interface {
	SetUnderConstructionFlag(string) error
	UnsetUnderConstructionFlag() error
	GetUnderConstructionFlag() (string, error)
	CreateLedgerID(ledgerID string, gb *common.Block) error
	LedgerIDExists(ledgerID string) (bool, error)
	GetAllLedgerIds() ([]string, error)
	GetLedgeIDValue(ledgerID string) ([]byte, error)
	Close()
}

// not implemented for Fabric 1.4.1
func OpenIDStore(path string) IDStore {
	return nil
}
