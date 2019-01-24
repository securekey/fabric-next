/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package roleutil

import (
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/gossip/api"
	"github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/gossip/discovery"
	"github.com/hyperledger/fabric/gossip/gossip"
	"github.com/hyperledger/fabric/gossip/util"
	proto "github.com/hyperledger/fabric/protos/gossip"
	"github.com/pkg/errors"
)

var logger = util.GetLogger(util.GossipLogger, "")

// RoleUtil provides functions to retrieve peers based on roles
type RoleUtil struct {
	channelID string
	self      *Member
	gossip    gossipAdapter
}

// NewRoleUtil returns a new RoleUtil
func NewRoleUtil(channelID string, gossip gossipAdapter) *RoleUtil {
	return &RoleUtil{
		channelID: channelID,
		gossip:    gossip,
		self:      getSelf(channelID, gossip),
	}
}

// Member wraps a NetworkMember and provides additional info
type Member struct {
	discovery.NetworkMember
	MSPID string
	Local bool // Inicates whether this member is the local peer
}

type filter func(m *Member) bool

type gossipAdapter interface {
	PeersOfChannel(common.ChainID) []discovery.NetworkMember
	SelfMembershipInfo() discovery.NetworkMember
	IdentityInfo() api.PeerIdentitySet
}

// Validators returns all peers in the local org that are validators, including the committer
func (r *RoleUtil) Validators(includeLocalPeer bool) []*Member {
	return r.getMembers(func(m *Member) bool {
		if m.Local && !includeLocalPeer {
			logger.Debugf("[%s] Not adding local peer", r.channelID)
			return false
		}
		roles := r.getRoles(m)
		if !roles.HasRole(ledgerconfig.ValidatorRole) {
			logger.Debugf("[%s] Not adding peer [%s] as a validator since it does not have the validator role", r.channelID, m.Endpoint)
			return false
		}
		if m.MSPID != r.self.MSPID {
			logger.Debugf("[%s] Not adding peer [%s] from MSP [%s] as a validator since it is not part of the local msp [%s]", r.channelID, m.Endpoint, m.MSPID, r.self.MSPID)
			return false
		}
		return true
	})
}

// Committer returns the committing peer for the local org
func (r *RoleUtil) Committer(includeLocalPeer bool) (*Member, error) {
	members := r.getMembers(func(m *Member) bool {
		if m.Local && !includeLocalPeer {
			logger.Debugf("[%s] Not adding local peer", r.channelID)
			return false
		}
		if !r.getRoles(m).HasRole(ledgerconfig.CommitterRole) {
			logger.Debugf("[%s] Not adding peer [%s] as a committer since it does not have the committer role", r.channelID, m.Endpoint)
			return false
		}
		if m.MSPID != r.self.MSPID {
			logger.Debugf("[%s] Not adding peer [%s] from MSP [%s] as a committer since it is not part of the local msp [%s]", r.channelID, m.Endpoint, m.MSPID, r.self.MSPID)
			return false
		}
		return true
	})
	if len(members) == 0 {
		return nil, errors.New("No remote committer found for local MSP")
	}
	return members[0], nil
}

func (r *RoleUtil) getMembers(accept filter) []*Member {
	identityInfo := r.gossip.IdentityInfo()
	mapByID := identityInfo.ByID()

	var peers []*Member
	for _, m := range r.gossip.PeersOfChannel(common.ChainID(r.channelID)) {
		identity, ok := mapByID[string(m.PKIid)]
		if !ok {
			logger.Warningf("[%s] Not adding peer [%s] as a validator since unable to determine MSP ID from PKIID for [%s]", r.channelID, m.Endpoint)
			continue
		}

		m := &Member{
			NetworkMember: m,
			MSPID:         string(identity.Organization),
		}

		if accept(m) {
			peers = append(peers, m)
		}
	}

	if accept(r.self) {
		peers = append(peers, r.self)
	}

	return peers
}

func (r *RoleUtil) getRoles(m *Member) gossip.Roles {
	if m.Properties == nil {
		logger.Debugf("[%s] Peer [%s] does not have any properties", r.channelID, m.Endpoint)
		return nil
	}
	return gossip.Roles(m.Properties.Roles)
}

func getSelf(channelID string, gossip gossipAdapter) *Member {
	self := gossip.SelfMembershipInfo()
	self.Properties = &proto.Properties{
		Roles: ledgerconfig.RolesAsString(),
	}

	identityInfo := gossip.IdentityInfo()
	mapByID := identityInfo.ByID()

	var mspID string
	selfIdentity, ok := mapByID[string(self.PKIid)]
	if ok {
		mspID = string(selfIdentity.Organization)
	} else {
		logger.Warningf("[%s] Unable to determine MSP ID from PKIID for self", channelID)
	}

	return &Member{
		NetworkMember: self,
		MSPID:         mspID,
		Local:         true,
	}
}
