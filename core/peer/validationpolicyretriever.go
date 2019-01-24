/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package peer

import (
	"sync"

	"github.com/hyperledger/fabric/common/channelconfig"
	"github.com/hyperledger/fabric/core/committer/txvalidator/validationpolicy"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/pkg/errors"
)

type policyRetriever struct {
	lock      sync.RWMutex
	channelID string
	ledger    ledger.PeerLedger
	listener  validationpolicy.ConfigUpdateListener
}

func newPolicyRetriever(channelID string, ledger ledger.PeerLedger) *policyRetriever {
	return &policyRetriever{
		channelID: channelID,
		ledger:    ledger,
	}
}

// GetPolicyBytes returns the validation policy bytes from the config block
func (r *policyRetriever) GetPolicyBytes() ([]byte, bool, error) {
	return getValidationPolicy(r.channelID, r.ledger)
}

// AddListener adds the given listener that is invoked when the config is updated
func (r *policyRetriever) AddListener(listener validationpolicy.ConfigUpdateListener) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.listener = listener
}

func (r *policyRetriever) configUpdated(*channelconfig.Bundle) {
	r.lock.RLock()
	defer r.lock.RUnlock()

	if r.listener != nil {
		logger.Infof("Informing validation policy listener that the config has been updated")
		r.listener()
	}
}

func getValidationPolicy(channelID string, ledger ledger.PeerLedger) ([]byte, bool, error) {
	conf, err := retrievePersistedChannelConfig(ledger)
	if err != nil {
		return nil, false, errors.New("error retrieving persisted channel config")
	}

	appGroup, ok := conf.GetChannelGroup().GetGroups()[channelconfig.ApplicationGroupKey]
	if !ok {
		return nil, false, nil
	}

	policy, ok := appGroup.GetPolicies()[validationPolicyKey]
	if !ok {
		return nil, false, nil
	}

	return policy.GetPolicy().GetValue(), true, nil
}
