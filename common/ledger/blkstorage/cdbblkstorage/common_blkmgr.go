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
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/hyperledger/fabric/protos/common"
)

func createBlockchainInfo(blockStore *couchdb.CouchDatabase, cpInfo *checkpointInfo) (*common.BlockchainInfo, error) {
	// init BlockchainInfo for external API's
	bi := common.BlockchainInfo{
		Height:            0,
		CurrentBlockHash:  nil,
		PreviousBlockHash: nil}

	if !cpInfo.isChainEmpty {
		//If start up is a restart of an existing storage, sync the index from block storage and update BlockchainInfo for external API's
		lastBlock, err := retrieveBlockByNumber(blockStore, cpInfo.lastBlockNumber)
		if err != nil {
			return nil, err
		}
		lastBlockHeader := lastBlock.GetHeader()
		lastBlockHash := lastBlockHeader.Hash()
		previousBlockHash := lastBlockHeader.PreviousHash
		bi = common.BlockchainInfo{
			Height:            cpInfo.lastBlockNumber + 1,
			CurrentBlockHash:  lastBlockHash,
			PreviousBlockHash: previousBlockHash}
	}
	return &bi, nil
}

func updateBlockchainInfo(currentBCInfo *common.BlockchainInfo, latestBlock *common.Block) *common.BlockchainInfo {
	latestBlockHash := latestBlock.GetHeader().Hash()
	newBCInfo := common.BlockchainInfo{
		Height:            currentBCInfo.Height + 1,
		CurrentBlockHash:  latestBlockHash,
		PreviousBlockHash: latestBlock.Header.PreviousHash}
	return &newBCInfo
}
