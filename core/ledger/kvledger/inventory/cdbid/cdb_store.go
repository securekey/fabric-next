/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbid

import (
	"github.com/hyperledger/fabric/protos/common"
	"github.com/pkg/errors"
)

type Store struct {
}

func OpenStore() (*Store, error) {
	return nil, errors.New("not implemented")
}

func (s *Store) SetUnderConstructionFlag(ledgerID string) error {
	return errors.New("not implemented")
}

func (s *Store) UnsetUnderConstructionFlag() error {
	return errors.New("not implemented")
}

func (s *Store) GetUnderConstructionFlag() (string, error) {
	return "", errors.New("not implemented")
}

func (s *Store) CreateLedgerID(ledgerID string, gb *common.Block) error {
	return errors.New("not implemented")
}

func (s *Store) LedgerIDExists(ledgerID string) (bool, error) {
	return false, errors.New("not implemented")
}

func (s *Store) GetAllLedgerIds() ([]string, error) {
	return nil, errors.New("not implemented")
}

func (s *Store) Close() {

}