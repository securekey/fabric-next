/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mocks

import (
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/ledger/rwset/kvrwset"
	"github.com/hyperledger/fabric/protos/peer"
)

// Creates a new mock block.
func CreateBlock(blockNum uint64, txns ...*Transaction) *common.Block {
	var envelopes []*common.Envelope
	var validationCodes []peer.TxValidationCode

	for _, txn := range txns {
		envelopes = append(envelopes, CreateEnvelope(txn.ChannelHeader, txn.Transaction))
		validationCodes = append(validationCodes, txn.ValidationCode)
	}

	b := createBlock(
		blockNum,
		validationCodes,
		envelopes,
	)

	return b
}

func createBlock(blockNum uint64, trxValidationCodes []peer.TxValidationCode, envelopes []*common.Envelope) *common.Block {
	var data [][]byte
	for _, env := range envelopes {
		envBytes, err := proto.Marshal(env)
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
func CreateTransaction(rwSets ...*rwsetutil.NsRwSet) *peer.Transaction {
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
func CreateEnvelope(channelHeader *common.ChannelHeader, trx *peer.Transaction) *common.Envelope {
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

func CreateSimpleMockBlock(blockNum uint64) *common.Block {
	const key = "test_key"
	const txID = "12345"

	return CreateBlock(blockNum, NewTransactionWithMockKey(txID, key, peer.TxValidationCode_VALID))
}

type Transaction struct {
	ChannelHeader  *common.ChannelHeader
	ValidationCode peer.TxValidationCode
	Transaction    *peer.Transaction
}

func NewTransactionWithMockKey(txID string, key string, validationCode peer.TxValidationCode) *Transaction {
	const namespace = "test_namespace"
	writes := []*kvrwset.KVWrite{{Key: key, IsDelete: false, Value: []byte("some_value")}}

	hdr := common.ChannelHeader{
		ChannelId: "some_channel",
		TxId:      txID,
		Type:      int32(common.HeaderType_ENDORSER_TRANSACTION),
	}

	txn := 	CreateTransaction(
		&rwsetutil.NsRwSet{
			NameSpace: namespace,
			KvRwSet: &kvrwset.KVRWSet{
				Writes: writes,
			},
		})

	return &Transaction{
		ChannelHeader:  &hdr,
		ValidationCode: validationCode,
		Transaction:    txn,
	}
}