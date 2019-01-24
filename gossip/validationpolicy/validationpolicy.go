/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package validationpolicy

import (
	"fmt"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/cauthdsl"
	"github.com/hyperledger/fabric/common/policies"
	"github.com/hyperledger/fabric/common/policies/inquire"
	ledgerUtil "github.com/hyperledger/fabric/core/ledger/util"
	"github.com/hyperledger/fabric/gossip/api"
	"github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/gossip/discovery"
	"github.com/hyperledger/fabric/gossip/roleutil"
	"github.com/hyperledger/fabric/gossip/util"
	"github.com/hyperledger/fabric/gossip/validationpolicy/peergroup"
	cb "github.com/hyperledger/fabric/protos/common"
	"github.com/op/go-logging"
	"github.com/pkg/errors"
)

var logger = util.GetLogger(util.LoggingGossipModule, "")

// ValidationResults contains the validation flags for the given block number.
type ValidationResults struct {
	BlockNumber uint64
	TxFlags     ledgerUtil.TxValidationFlags
	Err         error

	// Endpoint is the endpoint of the peer that provided the results.
	// Empty means local peer.
	Endpoint  string
	MSPID     string
	Signature []byte
}

type PolicyEvaluator interface {
	PeerGroups(block *cb.Block) (peergroup.PeerGroups, error)
	Validate(validationResults []*ValidationResults) error
}

type Policy struct {
	channelID string
	evaluator PolicyEvaluator
}

type gossipAdapter interface {
	PeersOfChannel(common.ChainID) []discovery.NetworkMember
	SelfMembershipInfo() discovery.NetworkMember
	IdentityInfo() api.PeerIdentitySet
}

type policyRetriever func(channelID string) ([]byte, error)

type InquireableValidationPolicy struct {
	Policy    policies.InquireablePolicy
	MaxGroups int
}

func New(channelID string, gossip gossipAdapter, getPolicyBytes policyRetriever) *Policy {
	return &Policy{
		channelID: channelID,
		evaluator: newPolicyEvaluatorWrapper(func() (PolicyEvaluator, error) {
			policyBytes, err := getPolicyBytes(channelID)
			if err != nil {
				return nil, err
			}

			var policy *InquireableValidationPolicy
			if len(policyBytes) == 0 {
				logger.Infof("[%s] No policy specified. Using single-org policy.", channelID)
				mspID := roleutil.New(channelID, gossip).Self().MSPID
				policy, err = getSingleOrgPolicy(mspID)
				if err != nil {
					return nil, err
				}
			} else {
				policy, err = getPolicy(policyBytes)
				if err != nil {
					return nil, err
				}
			}

			return newPolicyEvaluator(channelID, gossip, policy), nil
		}),
	}
}

// GetValidatingPeers returns the set of peers that are involved in validating the given block
func (v *Policy) GetValidatingPeers(block *cb.Block) ([]*roleutil.Member, error) {
	peerGroups, err := v.evaluator.PeerGroups(block)
	if err != nil {
		return nil, err
	}

	peerMap := make(map[string]*roleutil.Member)
	for _, pg := range peerGroups {
		for _, p := range pg {
			peerMap[p.Endpoint] = p
		}
	}

	var peers []*roleutil.Member
	for _, p := range peerMap {
		peers = append(peers, p)
	}

	return peers, nil
}

// GetTxFilter returns the transaction filter that determines whether or not the local peer
// should validate the transaction at a given index.
func (v *Policy) GetTxFilter(block *cb.Block) ledgerUtil.TxFilter {
	peerGroups, err := v.evaluator.PeerGroups(block)
	if err != nil {
		logger.Warningf("Error calculating peer groups for block %d: %s. Will validate all transactions.", block.Header.Number, err)
		return ledgerUtil.TxFilterAcceptAll
	}

	// FIXME: Change to Debug
	if logger.IsEnabledFor(logging.INFO) {
		logger.Infof("[%s] All validator groups:", v.channelID)
		for _, g := range peerGroups {
			logger.Infof("- %s", g)
		}
	}

	return func(txIdx int) bool {
		peerGroupForTx := peerGroups[txIdx%len(peerGroups)]
		// FIXME: Change to Debug
		logger.Infof("[%s] Validator group for TxIdx [%d] is %s", v.channelID, txIdx, peerGroupForTx)
		return peerGroupForTx.ContainsLocal()
	}
}

// Validate validates that the given validation results have come from a reliable source
// and that the validation policy has been satisfied.
func (v *Policy) Validate(results []*ValidationResults) error {
	return v.evaluator.Validate(results)
}

func getPolicy(policyBytes []byte) (*InquireableValidationPolicy, error) {
	sigPol := &cb.SignaturePolicyEnvelope{}
	if err := proto.Unmarshal(policyBytes, sigPol); err != nil {
		return nil, err
	}
	if len(sigPol.Identities) == 0 || sigPol.Rule == nil {
		return nil, errors.Errorf("Invalid policy, either Identities(%v) or Rule(%v) are empty:", sigPol.Identities, sigPol.Rule)
	}

	return &InquireableValidationPolicy{
		Policy:    inquire.NewInquireableSignaturePolicy(sigPol),
		MaxGroups: 3, // FIXME: Get this from the policy bytes somehow
	}, nil
}

func getSingleOrgPolicy(mspID string) (*InquireableValidationPolicy, error) {
	sigPol, err := cauthdsl.FromString("AND ('Org1MSP.member','Org2MSP.member')")
	if err != nil {
		return nil, err
	}
	logger.Infof("Created 'Org1MSP AND Org2MSP' validation policy: %+v", sigPol)
	return &InquireableValidationPolicy{
		Policy: inquire.NewInquireableSignaturePolicy(sigPol),
	}, nil
}

func getSingleOrgPolicyBackup(mspID string) (*InquireableValidationPolicy, error) {
	// FIXME: Should compose this manually instead of using the parser
	sigPol, err := cauthdsl.FromString(fmt.Sprintf("AND ('%s.member')", mspID))
	if err != nil {
		return nil, err
	}
	logger.Infof("Created single-org validation policy: %+v", sigPol)
	return &InquireableValidationPolicy{
		Policy: inquire.NewInquireableSignaturePolicy(sigPol),
	}, nil
}

type policyEvaluatorCreator func() (PolicyEvaluator, error)

type policyEvaluatorWrapper struct {
	sync.RWMutex
	createEvaluator policyEvaluatorCreator
	target          PolicyEvaluator
}

func newPolicyEvaluatorWrapper(createEvaluator policyEvaluatorCreator) *policyEvaluatorWrapper {
	return &policyEvaluatorWrapper{
		createEvaluator: createEvaluator,
	}
}

func (w *policyEvaluatorWrapper) PeerGroups(block *cb.Block) (peergroup.PeerGroups, error) {
	evaluator, err := w.getEvaluator()
	if err != nil {
		return nil, err
	}
	return evaluator.PeerGroups(block)
}

func (w *policyEvaluatorWrapper) Validate(validationResults []*ValidationResults) error {
	evaluator, err := w.getEvaluator()
	if err != nil {
		return err
	}
	return evaluator.Validate(validationResults)
}

func (w *policyEvaluatorWrapper) getEvaluator() (PolicyEvaluator, error) {
	w.RLock()
	evaluator := w.target
	w.RUnlock()

	if evaluator != nil {
		return evaluator, nil
	}

	w.Lock()
	defer w.Unlock()

	if w.target != nil {
		return w.target, nil
	}

	evaluator, err := w.createEvaluator()
	if err != nil {
		return nil, err
	}

	// TODO: Need to monitor changes in policy and/or expire the target
	w.target = evaluator

	return w.target, nil
}
