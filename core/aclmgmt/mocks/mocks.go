/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mocks

import (
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/stretchr/testify/mock"
)

type MockACLProvider struct {
	mock.Mock
}

func (m *MockACLProvider) CheckACL(resName string, channelID string, idinfo interface{}) error {
	args := m.Called(resName, channelID, idinfo)
	return args.Error(0)
}

func (e *MockACLProvider) GenerateSimulationResults(txEnvelop *common.Envelope, simulator ledger.TxSimulator) error {
	return nil
}

type MockACLProvider2 struct {
	RetErr error
}

func (m *MockACLProvider2) CheckACL(resName string, channelID string, idinfo interface{}) error {
	return m.RetErr
}

func (e *MockACLProvider2) GenerateSimulationResults(txEnvelop *common.Envelope, simulator ledger.TxSimulator) error {
	return nil
}
