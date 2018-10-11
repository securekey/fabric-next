/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mocks

import (
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/peer"
)

// Creates a new mock block.
func NewBlock(blockNum uint64, channelHeader *common.ChannelHeader, trxValidationCodes []peer.TxValidationCode, transactions ...*peer.Transaction) *common.Block {
	var data [][]byte
	for _, trx := range transactions {
		envBytes, err := proto.Marshal(NewEnvelope(channelHeader, trx))
		if err != nil {
			panic(err)
		}
		data = append(data, envBytes)
	}
	txValidationFlags := make([]uint8, len(trxValidationCodes))
	for i, code := range trxValidationCodes {
		txValidationFlags[i] = uint8(code)
	}
	blockMetaData := make([][]byte, 4)
	blockMetaData[common.BlockMetadataIndex_TRANSACTIONS_FILTER] = txValidationFlags
	return &common.Block{
		Header:   &common.BlockHeader{Number: blockNum},
		Metadata: &common.BlockMetadata{Metadata: blockMetaData},
		Data:     &common.BlockData{Data: data},
	}
}

// Creates a new mock transaction.
func NewTransaction(rwSets ...*rwsetutil.NsRwSet) *peer.Transaction {
	protoBytes, err := (&rwsetutil.TxRwSet{NsRwSets: rwSets}).ToProtoBytes()
	panicIfErr(err)
	extBytes, err := proto.Marshal(
		&peer.ChaincodeAction{
			Results: protoBytes,
		},
	)
	panicIfErr(err)
	prpBytes, err := proto.Marshal(
		&peer.ProposalResponsePayload{
			Extension: extBytes,
		},
	)
	panicIfErr(err)
	payloadBytes, err := proto.Marshal(
		&peer.ChaincodeActionPayload{
			Action: &peer.ChaincodeEndorsedAction{
				ProposalResponsePayload: prpBytes,
			},
		},
	)
	panicIfErr(err)
	return &peer.Transaction{
		Actions: []*peer.TransactionAction{
			{
				Payload: payloadBytes,
				Header:  nil,
			},
		},
	}
}

// Creates a new mock transaction envelope.
func NewEnvelope(channelHeader *common.ChannelHeader, trx *peer.Transaction) *common.Envelope {
	txBytes, err := proto.Marshal(trx)
	panicIfErr(err)
	channelHeaderBytes, err := proto.Marshal(channelHeader)
	panicIfErr(err)
	payloadBytes, err := proto.Marshal(
		&common.Payload{
			Header: &common.Header{
				ChannelHeader: channelHeaderBytes,
			},
			Data: txBytes,
		},
	)
	panicIfErr(err)
	return &common.Envelope{
		Payload: payloadBytes,
	}
}

func panicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}
