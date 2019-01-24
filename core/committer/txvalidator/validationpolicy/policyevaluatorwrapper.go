/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package validationpolicy

import (
	"sync"

	"github.com/hyperledger/fabric/core/committer/txvalidator/validationpolicy/peergroup"
	cb "github.com/hyperledger/fabric/protos/common"
	"github.com/pkg/errors"
)

type policyEvaluatorCreator func(channelID string) (policyEvaluator, error)

type evaluatorWrapper struct {
	sync.RWMutex
	channelID       string
	createEvaluator policyEvaluatorCreator
	target          policyEvaluator
}

func newEvaluatorWrapper(channelID string, createEvaluator policyEvaluatorCreator) *evaluatorWrapper {
	return &evaluatorWrapper{
		channelID:       channelID,
		createEvaluator: createEvaluator,
	}
}

func (w *evaluatorWrapper) PeerGroups(block *cb.Block) (peergroup.PeerGroups, error) {
	evaluator, err := w.getEvaluator()
	if err != nil {
		return nil, err
	}
	return evaluator.PeerGroups(block)
}

func (w *evaluatorWrapper) Validate(validationResults []*ValidationResults) error {
	evaluator, err := w.getEvaluator()
	if err != nil {
		return err
	}
	return evaluator.Validate(validationResults)
}

func (w *evaluatorWrapper) IsLocalOrgPolicy() (bool, error) {
	evaluator, err := w.getEvaluator()
	if err != nil {
		return false, err
	}
	return evaluator.IsLocalOrgPolicy()
}

func (w *evaluatorWrapper) getEvaluator() (policyEvaluator, error) {
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

	evaluator, err := w.createEvaluator(w.channelID)
	if err != nil {
		logger.Errorf("[%s] Error creating validation policy evaluator: %s", w.channelID, err)
		return nil, errors.WithMessage(err, "error creating validation policy evaluator")
	}

	w.target = evaluator

	return w.target, nil
}

func (w *evaluatorWrapper) reset() {
	w.Lock()
	defer w.Unlock()
	w.target = nil
}
