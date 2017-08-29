/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package service

import (
	"reflect"

	"github.com/hyperledger/fabric/common/config/channel"

	"github.com/hyperledger/fabric/protos/peer"
)

// Config enumerates the configuration methods required by gossip
type Config interface {
	// ChainID returns the chainID for this channel
	ChainID() string

	// Organizations returns a map of org ID to ApplicationOrgConfig
	Organizations() map[string]config.ApplicationOrg

	// Sequence should return the sequence number of the current configuration
	Sequence() uint64
}

// ConfigProcessor receives config updates
type ConfigProcessor interface {
	// ProcessConfig should be invoked whenever a channel's configuration is initialized or updated
	ProcessConfigUpdate(config Config)
}

type configStore struct {
	anchorPeers []*peer.AnchorPeer
	orgMap      map[string]config.ApplicationOrg
}

type configEventReceiver interface {
	configUpdated(config Config)
}

type configEventer struct {
	lastConfig *configStore
	receiver   configEventReceiver
}

func newConfigEventer(receiver configEventReceiver) *configEventer {
	return &configEventer{
		receiver: receiver,
	}
}

// ProcessConfigUpdate should be invoked whenever a channel's configuration is intialized or updated
// it invokes the associated method in configEventReceiver when configuration is updated
// but only if the configuration value actually changed
// Note, that a changing sequence number is ignored as changing configuration
func (ce *configEventer) ProcessConfigUpdate(config Config) {
	logger.Debugf("Processing new config for channel %s", config.ChainID())
	orgMap := cloneOrgConfig(config.Organizations())
	if ce.lastConfig != nil && reflect.DeepEqual(ce.lastConfig.orgMap, orgMap) {
		logger.Debugf("Ignoring new config for channel %s because it contained no anchor peer updates", config.ChainID())
		return
	}

	var newAnchorPeers []*peer.AnchorPeer
	for _, group := range config.Organizations() {
		newAnchorPeers = append(newAnchorPeers, group.AnchorPeers()...)
	}

	newConfig := &configStore{
		orgMap:      orgMap,
		anchorPeers: newAnchorPeers,
	}
	ce.lastConfig = newConfig

	logger.Debugf("Calling out because config was updated for channel %s", config.ChainID())
	ce.receiver.configUpdated(config)
}

func cloneOrgConfig(src map[string]config.ApplicationOrg) map[string]config.ApplicationOrg {
	clone := make(map[string]config.ApplicationOrg)
	for k, v := range src {
		clone[k] = &appGrp{
			name:        v.Name(),
			mspID:       v.MSPID(),
			anchorPeers: v.AnchorPeers(),
		}
	}
	return clone
}

type appGrp struct {
	name        string
	mspID       string
	anchorPeers []*peer.AnchorPeer
}

func (ag *appGrp) Name() string {
	return ag.name
}

func (ag *appGrp) MSPID() string {
	return ag.mspID
}

func (ag *appGrp) AnchorPeers() []*peer.AnchorPeer {
	return ag.anchorPeers
}
