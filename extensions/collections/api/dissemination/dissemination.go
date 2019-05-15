/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package dissemination

import (
	"github.com/hyperledger/fabric/gossip/gossip"
	proto "github.com/hyperledger/fabric/protos/gossip"
)

// Plan contains the dissemination plan for Kevlar private data types
type Plan struct {
	Msg      *proto.SignedGossipMessage
	Criteria gossip.SendCriteria
}
