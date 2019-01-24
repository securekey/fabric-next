/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package validationpolicy

import (
	"sync"

	"github.com/hyperledger/fabric/core/committer/txvalidator/validationpolicy/peergroup"
	cb "github.com/hyperledger/fabric/protos/common"
)

type policyEvaluatorCreator func() (policyEvaluator, error)

type policyEvaluatorWrapper struct {
	sync.RWMutex
	createEvaluator policyEvaluatorCreator
	target          policyEvaluator
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

func (w *policyEvaluatorWrapper) getEvaluator() (policyEvaluator, error) {
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

	// FIXME: Need to monitor changes in policy and/or expire the target
	w.target = evaluator

	return w.target, nil
}
