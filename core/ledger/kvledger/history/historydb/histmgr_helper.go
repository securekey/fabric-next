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

package historydb

import (
	"bytes"

	"github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/protos/ledger/queryresult"
	"github.com/pkg/errors"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/util"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/rwsetutil"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	coreledgerutil "github.com/hyperledger/fabric/core/ledger/util"
	"github.com/hyperledger/fabric/protos/common"
	putils "github.com/hyperledger/fabric/protos/utils"
)

var logger = flogging.MustGetLogger("historydb")

// CompositeKeySep is a nil byte used as a separator between different components of a composite key
var CompositeKeySep = []byte{0x00}

var savePointKey = []byte{0x00}

//ConstructCompositeHistoryKey builds the History Key of namespace~key~blocknum~trannum
// using an order preserving encoding so that history query results are ordered by height
func ConstructCompositeHistoryKey(ns string, key string, blocknum uint64, trannum uint64) []byte {

	var compositeKey []byte
	compositeKey = append(compositeKey, []byte(ns)...)
	compositeKey = append(compositeKey, CompositeKeySep...)
	compositeKey = append(compositeKey, []byte(key)...)
	compositeKey = append(compositeKey, CompositeKeySep...)
	compositeKey = append(compositeKey, util.EncodeOrderPreservingVarUint64(blocknum)...)
	compositeKey = append(compositeKey, util.EncodeOrderPreservingVarUint64(trannum)...)

	return compositeKey
}

//ConstructPartialCompositeHistoryKey builds a partial History Key namespace~key~
// for use in history key range queries
func ConstructPartialCompositeHistoryKey(ns string, key string, endkey bool) []byte {
	var compositeKey []byte
	compositeKey = append(compositeKey, []byte(ns)...)
	compositeKey = append(compositeKey, CompositeKeySep...)
	compositeKey = append(compositeKey, []byte(key)...)
	compositeKey = append(compositeKey, CompositeKeySep...)
	if endkey {
		compositeKey = append(compositeKey, []byte{0xff}...)
	}
	return compositeKey
}

//SplitCompositeHistoryKey splits the key bytes using a separator
func SplitCompositeHistoryKey(bytesToSplit []byte, separator []byte) ([]byte, []byte) {
	split := bytes.SplitN(bytesToSplit, separator, 2)
	return split[0], split[1]
}

// ConstructHistoryBatch takes a block and constructs a map of (key, value) pairs
// to be commited to a history store as a batch
// Returns:
// [][]byte composite history keys in the form ns~key~blockNo~tranNo (each key is []byte)
// []byte save point
// error
func ConstructHistoryBatch(channel string, block *common.Block) ([][]byte, *version.Height, error) {

	keys := [][]byte{{}}

	blockNo := block.Header.Number
	//Set the starting tranNo to 0
	var tranNo uint64

	logger.Debugf("Channel [%s]: Updating history database for blockNo [%v] with [%d] transactions",
		channel, blockNo, len(block.Data.Data))

	// Get the invalidation byte array for the block
	txsFilter := coreledgerutil.TxValidationFlags(block.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER])

	// write each tran's write set to history db
	for _, envBytes := range block.Data.Data {

		// If the tran is marked as invalid, skip it
		if txsFilter.IsInvalid(int(tranNo)) {
			logger.Debugf("Channel [%s]: Skipping history write for invalid transaction number %d",
				channel, tranNo)
			tranNo++
			continue
		}

		env, err := putils.GetEnvelopeFromBlock(envBytes)
		if err != nil {
			return nil, nil, err
		}

		payload, err := putils.GetPayload(env)
		if err != nil {
			return nil, nil, err
		}

		chdr, err := putils.UnmarshalChannelHeader(payload.Header.ChannelHeader)
		if err != nil {
			return nil, nil, err
		}

		if common.HeaderType(chdr.Type) == common.HeaderType_ENDORSER_TRANSACTION {

			// extract actions from the envelope message
			respPayload, err := putils.GetActionFromEnvelope(envBytes)
			if err != nil {
				return nil, nil, err
			}

			//preparation for extracting RWSet from transaction
			txRWSet := &rwsetutil.TxRwSet{}

			// Get the Result from the Action and then Unmarshal
			// it into a TxReadWriteSet using custom unmarshalling
			if err = txRWSet.FromProtoBytes(respPayload.Results); err != nil {
				return nil, nil, err
			}
			// for each transaction, loop through the namespaces and writesets
			// and add a history record for each write
			for _, nsRWSet := range txRWSet.NsRwSets {
				ns := nsRWSet.NameSpace

				for _, kvWrite := range nsRWSet.KvRwSet.Writes {
					writeKey := kvWrite.Key

					//composite key for history records is in the form ns~key~blockNo~tranNo
					compositeHistoryKey := ConstructCompositeHistoryKey(ns, writeKey, blockNo, tranNo)

					// No value is required, write an empty byte array (emptyValue) since Put() of nil is not allowed
					keys = append(keys, compositeHistoryKey)
				}
			}

		} else {
			logger.Debugf("Skipping transaction [%d] since it is not an endorsement transaction\n", tranNo)
		}
		tranNo++
	}

	// savepoint for recovery purpose
	height := version.NewHeight(blockNo, tranNo)

	return keys, height, nil
}

func SavePointKey() []byte {
	return savePointKey
}

// GetKeyModificationFromTran inspects a transaction for writes to a given key
func GetKeyModificationFromTran(tranEnvelope *common.Envelope, namespace string, key string) (ledger.QueryResult, error) {
	logger.Debugf("Entering getKeyModificationFromTran()\n", namespace, key)

	// extract action from the envelope
	payload, err := putils.GetPayload(tranEnvelope)
	if err != nil {
		return nil, err
	}

	tx, err := putils.GetTransaction(payload.Data)
	if err != nil {
		return nil, err
	}

	_, respPayload, err := putils.GetPayloads(tx.Actions[0])
	if err != nil {
		return nil, err
	}

	chdr, err := putils.UnmarshalChannelHeader(payload.Header.ChannelHeader)
	if err != nil {
		return nil, err
	}

	txID := chdr.TxId
	timestamp := chdr.Timestamp

	txRWSet := &rwsetutil.TxRwSet{}

	// Get the Result from the Action and then Unmarshal
	// it into a TxReadWriteSet using custom unmarshalling
	if err = txRWSet.FromProtoBytes(respPayload.Results); err != nil {
		return nil, err
	}

	// look for the namespace and key by looping through the transaction's ReadWriteSets
	for _, nsRWSet := range txRWSet.NsRwSets {
		if nsRWSet.NameSpace == namespace {
			// got the correct namespace, now find the key write
			for _, kvWrite := range nsRWSet.KvRwSet.Writes {
				if kvWrite.Key == key {
					return &queryresult.KeyModification{TxId: txID, Value: kvWrite.Value,
						Timestamp: timestamp, IsDelete: kvWrite.IsDelete}, nil
				}
			} // end keys loop
			return nil, errors.New("Key not found in namespace's writeset")
		} // end if
	} //end namespaces loop

	return nil, errors.New("Namespace not found in transaction's ReadWriteSets")
}
