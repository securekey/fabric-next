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

var logger = util.GetLogger(util.LoggingGossipModule, "")

// RoleUtil provides functions to retrieve peers based on roles
type RoleUtil struct {
	channelID string
	self      *Member
	gossip    gossipAdapter
}

// New returns a new RoleUtil
func New(channelID string, gossip gossipAdapter) *RoleUtil {
	return &RoleUtil{
		channelID: channelID,
		gossip:    gossip,
		self:      getSelf(channelID, gossip),
	}
}

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

// Roles returns the roles of the peer
func (m *Member) Roles() gossip.Roles {
	if m.Properties == nil {
		logger.Debugf("[%s] Peer [%s] does not have any properties", m.ChannelID, m.Endpoint)
		return nil
	}
	return gossip.Roles(m.Properties.Roles)
}

// HasRole returns true if the member has the given role
func (m *Member) HasRole(role ledgerconfig.Role) bool {
	return m.Roles().HasRole(role)
}

type filter func(m *Member) bool

type gossipAdapter interface {
	PeersOfChannel(common.ChainID) []discovery.NetworkMember
	SelfMembershipInfo() discovery.NetworkMember
	IdentityInfo() api.PeerIdentitySet
}

// Self returns the local peer
func (r *RoleUtil) Self() *Member {
	return r.self
}

// Validators returns all peers in the local org that are validators, including the committer
func (r *RoleUtil) Validators(includeLocalPeer bool) []*Member {
	return r.GetMembers(func(m *Member) bool {
		if m.Local && !includeLocalPeer {
			logger.Debugf("[%s] Not adding local peer", r.channelID)
			return false
		}
		if !m.HasRole(ledgerconfig.ValidatorRole) {
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
	members := r.GetMembers(func(m *Member) bool {
		if m.Local && !includeLocalPeer {
			logger.Debugf("[%s] Not adding local peer", r.channelID)
			return false
		}
		if !m.HasRole(ledgerconfig.CommitterRole) {
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

func (r *RoleUtil) GetMembers(accept filter) []*Member {
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

func (r *RoleUtil) GetMSPID(pkiID common.PKIidType) (string, bool) {
	identityInfo := r.gossip.IdentityInfo()
	mapByID := identityInfo.ByID()

	identity, ok := mapByID[string(pkiID)]
	if !ok {
		return "", false
	}
	return string(identity.Organization), true
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
