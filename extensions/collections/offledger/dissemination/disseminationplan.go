/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package dissemination

import (
	protobuf "github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/core/common/privdata"
	"github.com/hyperledger/fabric/extensions/collections/api/dissemination"
	"github.com/hyperledger/fabric/extensions/common"
	gossipapi "github.com/hyperledger/fabric/gossip/api"
	gcommon "github.com/hyperledger/fabric/gossip/common"
	gdiscovery "github.com/hyperledger/fabric/gossip/discovery"
	"github.com/hyperledger/fabric/gossip/gossip"
	cb "github.com/hyperledger/fabric/protos/common"
	proto "github.com/hyperledger/fabric/protos/gossip"
	"github.com/hyperledger/fabric/protos/ledger/rwset"
	"github.com/hyperledger/fabric/protos/ledger/rwset/kvrwset"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
)

type gossipAdapter interface {
	PeersOfChannel(gcommon.ChainID) []gdiscovery.NetworkMember
	SelfMembershipInfo() gdiscovery.NetworkMember
	IdentityInfo() gossipapi.PeerIdentitySet
}

// ComputeDisseminationPlan returns the dissemination plan for off ledger data
func ComputeDisseminationPlan(
	channelID, ns string,
	rwSet *rwset.CollectionPvtReadWriteSet,
	collConfig *cb.StaticCollectionConfig,
	colAP privdata.CollectionAccessPolicy,
	pvtDataMsg *proto.SignedGossipMessage,
	gossipAdapter gossipAdapter) ([]*dissemination.Plan, bool, error) {
	logger.Debugf("Computing dissemination plan for [%s:%s]", ns, rwSet.CollectionName)

	kvRwSet := &kvrwset.KVRWSet{}
	if err := protobuf.Unmarshal(rwSet.Rwset, kvRwSet); err != nil {
		return nil, true, errors.WithMessage(err, "error unmarshalling KV read/write set")
	}

	if err := validateAll(collConfig.Type, kvRwSet); err != nil {
		return nil, false, errors.Wrapf(err, "one or more keys did not validate for collection [%s:%s]", ns, rwSet.CollectionName)
	}

	peers := New(channelID, ns, rwSet.CollectionName, colAP, gossipAdapter).resolvePeersForDissemination().Remote()

	logger.Debugf("Peers for dissemination of collection [%s:%s]: %s", ns, rwSet.CollectionName, peers)

	routingFilter := func(member gdiscovery.NetworkMember) bool {
		if peers.ContainsPeer(member.Endpoint) {
			logger.Debugf("Including peer [%s] for dissemination of [%s:%s]", member.Endpoint, ns, rwSet.CollectionName)
			return true
		}

		logger.Debugf("Not including peer [%s] for dissemination of [%s:%s]", member.Endpoint, ns, rwSet.CollectionName)
		return false
	}

	sc := gossip.SendCriteria{
		Timeout:    viper.GetDuration("peer.gossip.pvtData.pushAckTimeout"),
		Channel:    gcommon.ChainID(channelID),
		MaxPeers:   len(peers),
		MinAck:     colAP.RequiredPeerCount(),
		IsEligible: routingFilter,
	}

	return []*dissemination.Plan{{
		Criteria: sc,
		Msg:      pvtDataMsg,
	}}, true, nil
}

func validateAll(collType cb.CollectionType, kvRWSet *kvrwset.KVRWSet) error {
	for _, ws := range kvRWSet.Writes {
		if err := validate(collType, ws); err != nil {
			return err
		}
	}
	return nil
}

func validate(collType cb.CollectionType, ws *kvrwset.KVWrite) error {
	if ws.Value == nil {
		return errors.Errorf("attempt to store nil value for key [%s]", ws.Key)
	}

	if collType == cb.CollectionType_COL_DCAS {
		expectedKey := common.GetCASKey(ws.Value)
		if ws.Key != expectedKey {
			return errors.Errorf("invalid CAS key [%s] - the key should be the hash of the value", ws.Key)
		}
	}
	return nil
}
