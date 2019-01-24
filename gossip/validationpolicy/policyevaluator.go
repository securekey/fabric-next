/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package validationpolicy

import (
	"hash/fnv"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/policies"
	"github.com/hyperledger/fabric/gossip/common"
	discimpl "github.com/hyperledger/fabric/gossip/discovery"
	"github.com/hyperledger/fabric/gossip/roleutil"
	"github.com/hyperledger/fabric/gossip/validationpolicy/peergroup"
	cb "github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/discovery"
	"github.com/hyperledger/fabric/protos/msp"
	"github.com/op/go-logging"
	"github.com/pkg/errors"
)

type policyEvaluator struct {
	channelID string
	policy    *InquireableValidationPolicy
	roleUtil  *roleutil.RoleUtil
}

func newPolicyEvaluator(channelID string, gossip gossipAdapter, policy *InquireableValidationPolicy) *policyEvaluator {
	return &policyEvaluator{
		channelID: channelID,
		policy:    policy,
		roleUtil:  roleutil.New(channelID, gossip),
	}
}

func (e *policyEvaluator) PeerGroups(block *cb.Block) (peergroup.PeerGroups, error) {
	logger.Infof("[%s] Calculating peer groups to validate block %d ...", e.channelID, block.Header.Number)

	d, err := valDiscovery.PeersForValidation(common.ChainID(e.channelID), e.policy.Policy)
	if err != nil {
		return nil, err
	}

	desc, err := e.createValidationDescriptor(d)
	if err != nil {
		return nil, err
	}

	var peerGroups peergroup.PeerGroups
	for i, layout := range desc.layouts {
		validators, canLayoutBeSatisfied := selectPeersForLayout(desc.validatorsByGroups, layout)
		if canLayoutBeSatisfied {
			peerGroups = append(peerGroups, validators...)
			if logger.IsEnabledFor(logging.INFO) {
				logger.Infof("Layout %d: %s", i, validators)
			}
		}
	}

	peerGroups.Sort()

	if logger.IsEnabledFor(logging.INFO) {
		logger.Infof("Peer groups: %s", peerGroups)
	}

	if e.policy.MaxGroups == 0 {
		return peerGroups, nil
	}

	// Deterministically, pick up to 'MaxGroups' groups

	startingIndex, err := e.getStartingIndex(block, len(peerGroups))
	if err != nil {
		return nil, err
	}

	logger.Infof("Peer groups starting index for block %d: %d", block.Header.Number, startingIndex)

	var refinedPeerGroups peergroup.PeerGroups

	it := peergroup.NewIterator(peerGroups, startingIndex)
	for pg := it.Next(); pg != nil && len(refinedPeerGroups) < int(e.policy.MaxGroups); pg = it.Next() {
		if refinedPeerGroups.ContainsAny(pg) {
			logger.Infof("Not adding peer group [%s] since at least one peer is already in the set of peer groups %s", pg, refinedPeerGroups)
			continue
		}
		logger.Infof("Adding unique peer group [%s]", pg)
		refinedPeerGroups = append(refinedPeerGroups, pg)
	}

	if len(refinedPeerGroups) < int(e.policy.MaxGroups) {
		logger.Infof("Found only %d of %d unique peer groups in set %s. Will select peer groups that have at least one unique peer...", len(refinedPeerGroups), e.policy.MaxGroups, peerGroups)
		it := peergroup.NewIterator(peerGroups, startingIndex)
		for pg := it.Next(); pg != nil && len(refinedPeerGroups) < int(e.policy.MaxGroups); pg = it.Next() {
			if refinedPeerGroups.ContainsAll(pg) {
				logger.Infof("Not adding peer group [%s] since all peers are already in the set of peer groups %s", pg, refinedPeerGroups)
				continue
			}
			logger.Infof("Adding peer group with at least one unique peer [%s]", pg)
			refinedPeerGroups = append(refinedPeerGroups, pg)
		}
	}

	if len(refinedPeerGroups) < int(e.policy.MaxGroups) {
		logger.Warningf("Found only %d of %d unique peer groups in set %s", len(refinedPeerGroups), e.policy.MaxGroups, peerGroups)
	}

	if logger.IsEnabledFor(logging.INFO) {
		logger.Infof("Refined peer groups: %s", refinedPeerGroups)
	}

	return refinedPeerGroups, nil
}

func (e *policyEvaluator) getStartingIndex(block *cb.Block, max int) (int, error) {
	h := fnv.New32a()
	_, err := h.Write(block.Header.DataHash)
	if err != nil {
		return 0, err
	}
	return int(h.Sum32()) % max, nil
}

func (e *policyEvaluator) Validate(validationResults []*ValidationResults) error {
	logger.Infof("[%s] Validating validator 'CrossOrg' policy...", e.channelID)

	// If this peer is a committer then there's no need to have consensus (since we should trust ourself)
	// FIXME: Implement
	return nil
}

type validationDescriptor struct {
	validatorsByGroups map[string][]*roleutil.Member
	layouts            []map[string]int
}

func (e *policyEvaluator) createValidationDescriptor(desc *discovery.ValidationDescriptor) (*validationDescriptor, error) {
	descriptor := &validationDescriptor{
		layouts:            []map[string]int{},
		validatorsByGroups: make(map[string][]*roleutil.Member),
	}
	for _, l := range desc.Layouts {
		currentLayout := make(map[string]int)
		descriptor.layouts = append(descriptor.layouts, currentLayout)
		for grp, count := range l.QuantitiesByGroup {
			if _, exists := desc.ValidatorsByGroups[grp]; !exists {
				return nil, errors.Errorf("group %s isn't mapped to validators, but exists in a layout", grp)
			}
			currentLayout[grp] = int(count)
		}
	}

	for grp, peers := range desc.ValidatorsByGroups {
		var validators []*roleutil.Member
		for _, p := range peers.Peers {
			member, err := e.asMember(p)
			if err != nil {
				return nil, errors.Wrap(err, "failed creating endorser object")
			}
			validators = append(validators, member)
		}
		descriptor.validatorsByGroups[grp] = validators
	}

	return descriptor, nil
}

func (e *policyEvaluator) asMember(peer *discovery.Peer) (*roleutil.Member, error) {
	if peer.MembershipInfo == nil || peer.StateInfo == nil {
		return nil, errors.Errorf("received empty envelope(s) for validators channel %s", e.channelID)
	}
	aliveMsg, err := peer.MembershipInfo.ToGossipMessage()
	if err != nil {
		return nil, errors.Wrap(err, "failed unmarshaling gossip envelope to alive message")
	}
	alive := aliveMsg.GetAliveMsg()

	stateInfMsg, err := peer.StateInfo.ToGossipMessage()
	if err != nil {
		return nil, errors.Wrap(err, "failed unmarshaling gossip envelope to state info message")
	}
	stateInfo := stateInfMsg.GetStateInfo()

	sID := &msp.SerializedIdentity{}
	if err := proto.Unmarshal(peer.Identity, sID); err != nil {
		return nil, errors.Wrap(err, "failed unmarshaling peer's identity")
	}

	return &roleutil.Member{
		NetworkMember: discimpl.NetworkMember{
			Endpoint:   alive.Membership.Endpoint,
			PKIid:      alive.Membership.PkiId,
			Properties: stateInfo.Properties,
		},
		MSPID: sID.Mspid,
		Local: e.roleUtil.Self().Endpoint == alive.Membership.Endpoint,
	}, nil
}

func selectPeersForLayout(validatorsByGroups map[string][]*roleutil.Member, layout map[string]int) (peergroup.PeerGroups, bool) {
	var peerGroups peergroup.PeerGroups
	for grp, count := range layout {
		validatorsOfGrp := validatorsByGroups[grp]
		if len(validatorsOfGrp) < count {
			// We couldn't select enough peers for this layout because the current group
			// requires more peers than we have available to be selected
			return nil, false
		}
		peerGroups = append(peerGroups, validatorsOfGrp)
	}

	return peergroup.NewPermutations().Groups(peerGroups...).Evaluate(), true
}

type validatorDiscovery interface {
	PeersForValidation(chainID common.ChainID, policy policies.InquireablePolicy) (*discovery.ValidationDescriptor, error)
}

var valDiscovery validatorDiscovery

func RegisterValidatorDiscovery(discovery validatorDiscovery) {
	valDiscovery = discovery
}
