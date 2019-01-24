/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package validationpolicy

import (
	"encoding/binary"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/cauthdsl"
	"github.com/hyperledger/fabric/common/policies"
	"github.com/hyperledger/fabric/common/policies/inquire"
	"github.com/hyperledger/fabric/core/committer/txvalidator/validationpolicy/peergroup"
	ledgerUtil "github.com/hyperledger/fabric/core/ledger/util"
	"github.com/hyperledger/fabric/gossip/api"
	"github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/gossip/discovery"
	"github.com/hyperledger/fabric/gossip/roleutil"
	"github.com/hyperledger/fabric/gossip/util"
	mspmgmt "github.com/hyperledger/fabric/msp/mgmt"
	cb "github.com/hyperledger/fabric/protos/common"
	"github.com/op/go-logging"
	"github.com/pkg/errors"
)

var logger = util.GetLogger(util.LoggingGossipModule, "")

const defaultMaxGroups = 3

// ValidationResults contains the validation flags for the given block number.
type ValidationResults struct {
	BlockNumber uint64
	TxFlags     ledgerUtil.TxValidationFlags
	Err         error
	Endpoint    string // Endpoint is the endpoint of the peer that provided the results.
	Local       bool   // If true then that means the results were generated locally and validation is not required
	MSPID       string
	Signature   []byte
	Identity    []byte
}

func (vr *ValidationResults) String() string {
	if vr.Err == nil {
		return fmt.Sprintf("(MSP: [%s], Endpoint: [%s], Block: %d, TxFlags: %v)", vr.MSPID, vr.Endpoint, vr.BlockNumber, vr.TxFlags)
	}
	return fmt.Sprintf("(MSP: [%s], Endpoint: [%s], Block: %d, Err: %s)", vr.MSPID, vr.Endpoint, vr.BlockNumber, vr.Err)
}

type policyEvaluator interface {
	PeerGroups(block *cb.Block) (peergroup.PeerGroups, error)
	Validate(validationResults []*ValidationResults) error
	IsLocalOrgPolicy() (bool, error)
}

type mspIDRetriever func() (string, error)

var localMSPIDRetriever = func() (string, error) {
	return mspmgmt.GetLocalMSP().GetIdentifier()
}

// Policy manages the validation policy
type Policy struct {
	channelID string
	evaluator policyEvaluator
}

type gossipAdapter interface {
	PeersOfChannel(common.ChainID) []discovery.NetworkMember
	SelfMembershipInfo() discovery.NetworkMember
	IdentityInfo() api.PeerIdentitySet
}

// ConfigUpdateListener is a function that's invoked when the config is updated in the channel
type ConfigUpdateListener func()

// PolicyRetriever retrieves validation policy bytes
type PolicyRetriever interface {
	GetPolicyBytes() ([]byte, bool, error)
	AddListener(listener ConfigUpdateListener)
}

// New returns a new Validation Policy
func New(channelID string, gossip gossipAdapter, pp policies.Provider, policyRetriever PolicyRetriever) *Policy {
	evaluator := newEvaluatorWrapper(channelID, newEvaluatorCreator(gossip, pp, policyRetriever))

	policyRetriever.AddListener(func() {
		evaluator.reset()
	})

	return &Policy{
		channelID: channelID,
		evaluator: evaluator,
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
		logger.Infof("[%s] All validator groups for block %d:", v.channelID, block.Header.Number)
		for _, g := range peerGroups {
			logger.Infof("- %s", g)
		}
	}

	return func(txIdx int) bool {
		peerGroupForTx := peerGroups[txIdx%len(peerGroups)]
		// FIXME: Change to Debug
		logger.Infof("[%s] Validator group for block %d, TxIdx [%d] is %s", v.channelID, block.Header.Number, txIdx, peerGroupForTx)
		return peerGroupForTx.ContainsLocal()
	}
}

// Validate validates that the given validation results have come from a reliable source
// and that the validation policy has been satisfied.
func (v *Policy) Validate(results []*ValidationResults) error {
	return v.evaluator.Validate(results)
}

// IsLocalOrgPolicy returns true if the validation policy is the default, local-org policy
func (v *Policy) IsLocalOrgPolicy() (bool, error) {
	return v.evaluator.IsLocalOrgPolicy()
}

func newEvaluatorCreator(gossip gossipAdapter, pp policies.Provider, policyRetriever PolicyRetriever) func(string) (policyEvaluator, error) {
	return func(channelID string) (policyEvaluator, error) {
		// FIXME: Change to Debug
		logger.Infof("[%s] Creating new evaluator...", channelID)

		localMSPID, err := localMSPIDRetriever()
		if err != nil {
			return nil, errors.WithMessage(err, "error retrieving local MSP ID")
		}

		// 'local-org' policy
		strPolicy := fmt.Sprintf("MAX(0,AND('%s.member'))", localMSPID)

		// FIXME: Change to Debug
		logger.Infof("[%s] Parsing 'local-org' policy [%s]", channelID, strPolicy)

		sigPol, err := cauthdsl.FromString(strPolicy)
		if err != nil {
			return nil, errors.WithMessage(err, "error parsing 'local-org' policy")
		}

		orgPolicyBytes, err := proto.Marshal(sigPol)
		if err != nil {
			return nil, errors.WithMessage(err, "error marshaling 'local-org' policy")
		}

		orgValidator, _, err := pp.NewPolicy(orgPolicyBytes)
		if err != nil {
			return nil, errors.WithMessage(err, "error creating 'local-org' policy evaluator")
		}

		var validator policies.Policy
		var inquireablePolicy *inquireableValidationPolicy

		policyBytes, exists, err := policyRetriever.GetPolicyBytes()
		if err != nil {
			return nil, errors.WithMessage(err, "error retrieving validation policy bytes")
		}

		if !exists {
			// FIXME: Change to Debug
			logger.Infof("[%s] No validation policy specified. Using the default 'local-org' policy", channelID)
			validator = orgValidator
			if inquireablePolicy, err = newInquireablePolicy(channelID, orgPolicyBytes); err != nil {
				return nil, errors.WithMessage(err, "error creating 'local-org' inquireable validation policy")
			}
		} else {
			if validator, _, err = pp.NewPolicy(policyBytes); err != nil {
				return nil, errors.WithMessage(err, "error creating policy evaluator")
			}
			if inquireablePolicy, err = newInquireablePolicy(channelID, policyBytes); err != nil {
				return nil, errors.WithMessage(err, "error creating inquireable validation policy")
			}

		}

		// FIXME: In order to determine for sure if the 'local-org' policy is being used, we need
		// to go through the policy and determine if the local org is the only one specified.
		return newEvaluator(channelID, gossip, validator, orgValidator, inquireablePolicy, !exists)
	}
}

// GetDataToSign appends the block number, Tx Flags, and the serialized identity
// into a byte buffer to be signed by the validator.
func GetDataToSign(blockNum uint64, txFlags, identity []byte) []byte {
	blockNumBytes := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(blockNumBytes, blockNum)
	data := append(blockNumBytes, txFlags...)
	return append(data, identity...)
}

func newInquireablePolicy(channelID string, policyBytes []byte) (*inquireableValidationPolicy, error) {
	sigPol := &cb.SignaturePolicyEnvelope{}
	if err := proto.Unmarshal(policyBytes, sigPol); err != nil {
		return nil, err
	}

	// FIXME: Change to Debug
	logger.Infof("[%s] Max validation groups in signature policy envelope: %d", channelID, sigPol.MaxValidationGroups)

	if len(sigPol.Identities) == 0 || sigPol.Rule == nil {
		return nil, errors.Errorf("Invalid policy, either Identities(%v) or Rule(%v) are empty:", sigPol.Identities, sigPol.Rule)
	}

	return &inquireableValidationPolicy{
		InquireablePolicy: inquire.NewInquireableSignaturePolicy(sigPol),
		maxGroups:         sigPol.MaxValidationGroups,
	}, nil
}

type inquireableValidationPolicy struct {
	policies.InquireablePolicy
	maxGroups int32
}

func (p *inquireableValidationPolicy) MaxGroups() int32 {
	return p.maxGroups
}
