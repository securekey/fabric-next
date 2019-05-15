/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package discovery

import (
	"github.com/hyperledger/fabric/gossip/discovery"
)

// Member wraps a NetworkMember and provides additional info
type Member struct {
	discovery.NetworkMember
	ChannelID string
	MSPID     string
	Local     bool // Indicates whether this member is the local peer
}

func (m *Member) String() string {
	return m.Endpoint
}
