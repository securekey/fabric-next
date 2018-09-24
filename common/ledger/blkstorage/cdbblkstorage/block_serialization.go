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
/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/


package cdbblkstorage

import (
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/utils"
	"github.com/pkg/errors"
)

func extractTxID(txEnvelopBytes []byte) (string, error) {
	txEnvelope, err := utils.GetEnvelopeFromBlock(txEnvelopBytes)
	if err != nil {
		return "", err
	}
	txPayload, err := utils.GetPayload(txEnvelope)
	if err != nil {
		return "", nil
	}
	chdr, err := utils.UnmarshalChannelHeader(txPayload.Header.ChannelHeader)
	if err != nil {
		return "", err
	}
	return chdr.TxId, nil
}

func extractTxnEnvelopeFromBlock(block *common.Block, txID string) (*common.Envelope, error) {
	blockData := block.GetData()
	for _, txEnvelopeBytes := range blockData.GetData() {
		id, err := extractTxID(txEnvelopeBytes)
		if err != nil {
			return nil, err
		}
		if id != txID {
			continue
		}

		txEnvelope, err := utils.GetEnvelopeFromBlock(txEnvelopeBytes)
		if err != nil {
			return nil, err
		}

		return txEnvelope, nil
	}

	return nil, errors.Errorf("transaction not found [%s]", txID)
}