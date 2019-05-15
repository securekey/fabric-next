/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package dissemination

import (
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/common/privdata"
	"github.com/hyperledger/fabric/extensions/common/discovery"
	"github.com/hyperledger/fabric/extensions/config"
)

var logger = flogging.MustGetLogger("offledgerstore")

// Disseminator disseminates collection data to other endorsers
type Disseminator struct {
	*discovery.Discovery
	namespace  string
	collection string
	policy     privdata.CollectionAccessPolicy
}

// New returns a new disseminator
func New(channelID, namespace, collection string, policy privdata.CollectionAccessPolicy, gossip gossipAdapter) *Disseminator {
	return &Disseminator{
		Discovery:  discovery.New(channelID, gossip),
		namespace:  namespace,
		collection: collection,
		policy:     policy,
	}
}

// resolvePeersForDissemination resolves to a set of committers to which data should be disseminated
func (d *Disseminator) resolvePeersForDissemination() discovery.PeerGroup {
	orgs := d.policy.MemberOrgs()
	maxPeerCount := d.policy.MaximumPeerCount()

	logger.Debugf("[%s] Member orgs: %s", d.ChannelID(), orgs)

	peersForDissemination := d.getPeers(orgs)

	if len(peersForDissemination) < maxPeerCount {
		logger.Debugf("[%s] MaximumPeerCount in collection policy is %d and we only have %d committers. Adding some endorsers too...", d.ChannelID(), maxPeerCount, len(peersForDissemination))
		for _, peer := range d.getPeers(orgs).Remote().Shuffle() {
			if len(peersForDissemination) >= maxPeerCount {
				// We have enough peers
				break
			}
			logger.Debugf("Adding endorser [%s] ...", peer)
			peersForDissemination = append(peersForDissemination, peer)
		}
	}

	logger.Debugf("[%s] Peers for dissemination from orgs %s: %s", d.ChannelID(), orgs, peersForDissemination)

	return peersForDissemination
}

// ResolvePeersForRetrieval resolves to a set of peers from which data should may be retrieved
func (d *Disseminator) ResolvePeersForRetrieval() discovery.PeerGroup {
	orgs := d.policy.MemberOrgs()

	logger.Debugf("[%s] Member orgs: %s", d.ChannelID(), orgs)

	// Maximum number of peers to ask for the data
	maxPeers := getMaxPeersForRetrieval()

	var peersForRetrieval discovery.PeerGroup
	for _, peer := range d.getPeers(orgs).Remote().Shuffle() {
		if len(peersForRetrieval) >= maxPeers {
			// We have enough peers
			break
		}
		logger.Debugf("Adding endorser [%s] ...", peer)
		peersForRetrieval = append(peersForRetrieval, peer)
	}

	if len(peersForRetrieval) < maxPeers {
		// Add some committers too
		for _, peer := range d.getPeers(orgs).Remote().Shuffle() {
			if len(peersForRetrieval) >= maxPeers {
				// We have enough peers
				break
			}
			logger.Debugf("Adding committer [%s] ...", peer)
			peersForRetrieval = append(peersForRetrieval, peer)
		}
	}

	logger.Debugf("[%s] Peers for retrieval from orgs %s: %s", d.ChannelID(), orgs, peersForRetrieval)

	return peersForRetrieval
}

func (d *Disseminator) getPeers(mspIDs []string) discovery.PeerGroup {
	return d.GetMembers(func(m *discovery.Member) bool {
		if !contains(mspIDs, m.MSPID) {
			logger.Debugf("[%s] Not adding peer [%s] since it is not in any of the orgs [%s]", d.ChannelID(), m.Endpoint, mspIDs)
			return false
		}
		return true
	})
}

func contains(mspIDs []string, mspID string) bool {
	for _, m := range mspIDs {
		if m == mspID {
			return true
		}
	}
	return false
}

// getMaxPeersForRetrieval may be overridden by unit tests
var getMaxPeersForRetrieval = func() int {
	return config.GetOLCollMaxPeersForRetrieval()
}
