/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package mocks

import (
	cb "github.com/hyperledger/fabric/protos/common"
)

type BlockBuilder struct {
	number   uint64
	dataHash []byte
}

func NewBlockBuilder() *BlockBuilder {
	return &BlockBuilder{}
}

func (b *BlockBuilder) Number(number uint64) *BlockBuilder {
	b.number = number
	return b
}

func (b *BlockBuilder) DataHash(hash []byte) *BlockBuilder {
	b.dataHash = hash
	return b
}

func (b *BlockBuilder) Build() *cb.Block {
	return &cb.Block{
		Header: &cb.BlockHeader{
			Number:   b.number,
			DataHash: b.dataHash,
		},
	}
}
