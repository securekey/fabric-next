/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package state

import (
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/extensions/gossip/api"
	"github.com/trustbloc/fabric-peer-ext/pkg/common/support"
)

var logger = flogging.MustGetLogger("ext_gossip_state")

// ChannelJoined is invoked when the peer joins a channel
func ChannelJoined(channelID string, ledger ledger.PeerLedger, publisher api.BlockPublisher) {
	logger.Infof("Initializing collection config retriever for channel [%s]", channelID)
	support.InitCollectionConfigRetriever(channelID, ledger, publisher)
}
