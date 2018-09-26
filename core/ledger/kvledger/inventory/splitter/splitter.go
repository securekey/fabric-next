/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package splitter

import (
	"github.com/hyperledger/fabric/core/ledger/kvledger/inventory/cdbid"
	"github.com/hyperledger/fabric/core/ledger/kvledger/inventory/leveldbid"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/protos/common"
)

type idStore interface {
	SetUnderConstructionFlag(string) error
	UnsetUnderConstructionFlag() error
	GetUnderConstructionFlag() (string, error)
	CreateLedgerID(ledgerID string, gb *common.Block) error
	LedgerIDExists(ledgerID string) (bool, error)
	GetAllLedgerIds() ([]string, error)
	Close()
}

type Store struct {
	sa idStore
	sb idStore
}

func OpenStore() (*Store, error) {
	path := ledgerconfig.GetLedgerProviderPath()

	sa := leveldbid.OpenStore(path)
	sb, _ := cdbid.OpenStore()

	return &Store{sa, sb}, nil
}

func (s *Store) SetUnderConstructionFlag(ledgerID string) error {
	s.sb.SetUnderConstructionFlag(ledgerID)
	return s.sa.SetUnderConstructionFlag(ledgerID)
}

func (s *Store) UnsetUnderConstructionFlag() error {
	s.sb.UnsetUnderConstructionFlag()
	return s.sa.UnsetUnderConstructionFlag()
}

func (s *Store) GetUnderConstructionFlag() (string, error) {
	s.sb.GetUnderConstructionFlag()
	return s.sa.GetUnderConstructionFlag()
}

func (s *Store) CreateLedgerID(ledgerID string, gb *common.Block) error {
	s.sb.CreateLedgerID(ledgerID, gb)
	return s.sa.CreateLedgerID(ledgerID, gb)
}

func (s *Store) LedgerIDExists(ledgerID string) (bool, error) {
	s.sb.LedgerIDExists(ledgerID)
	return s.sa.LedgerIDExists(ledgerID)
}

func (s *Store) GetAllLedgerIds() ([]string, error) {
	s.sb.GetAllLedgerIds()
	return s.sa.GetAllLedgerIds()
}

func (s *Store) Close() {
	s.sa.Close()
	s.sb.Close()
}