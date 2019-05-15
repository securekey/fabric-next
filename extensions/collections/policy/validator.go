/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package policy

import (
	"fmt"
	"time"

	"github.com/hyperledger/fabric/protos/common"
	"github.com/pkg/errors"
)

// Validator is a collection policy validator
type Validator struct {
}

// NewValidator returns a new collection policy validator
func NewValidator() *Validator {
	return &Validator{}
}

// Validate validates various collection config types
func (v *Validator) Validate(collConfig *common.CollectionConfig) error {
	config := collConfig.GetStaticCollectionConfig()
	if config == nil {
		return errors.New("unknown collection configuration type")
	}

	switch config.Type {
	case common.CollectionType_COL_TRANSIENT:
		return v.validateTransientConfig(config)
	case common.CollectionType_COL_DCAS:
		fallthrough
	case common.CollectionType_COL_OFFLEDGER:
		return v.validateOffLedgerConfig(config)
	default:
		// Nothing to do
		return nil
	}
}

// ValidateNewCollectionConfigsAgainstOld validates updated collection configs
func (v *Validator) ValidateNewCollectionConfigsAgainstOld(newCollectionConfigs []*common.CollectionConfig, oldCollectionConfigs []*common.CollectionConfig,
) error {
	newCollectionsMap := make(map[string]*common.StaticCollectionConfig, len(newCollectionConfigs))
	for _, newCollectionConfig := range newCollectionConfigs {
		newCollection := newCollectionConfig.GetStaticCollectionConfig()
		newCollectionsMap[newCollection.GetName()] = newCollection
	}

	for _, oldCollConfig := range oldCollectionConfigs {
		oldColl := oldCollConfig.GetStaticCollectionConfig()
		if oldColl == nil {
			// This shouldn't happen since we've already gone through the basic validation
			return errors.New("invalid collection")
		}
		newColl, ok := newCollectionsMap[oldColl.Name]
		if !ok {
			continue
		}

		newCollType := getCollType(newColl)
		oldCollType := getCollType(oldColl)
		if newCollType != oldCollType {
			return fmt.Errorf("collection-name: %s -- attempt to change collection type from [%s] to [%s]", oldColl.Name, oldCollType, newCollType)
		}
	}
	return nil
}

func (v *Validator) validateTransientConfig(config *common.StaticCollectionConfig) error {
	if config.RequiredPeerCount <= 0 {
		return errors.Errorf("collection-name: %s -- required peer count must be greater than 0", config.Name)
	}

	if config.RequiredPeerCount > config.MaximumPeerCount {
		return errors.Errorf("collection-name: %s -- maximum peer count (%d) must be greater than or equal to required peer count (%d)", config.Name, config.MaximumPeerCount, config.RequiredPeerCount)
	}
	if config.TimeToLive == "" {
		return errors.Errorf("collection-name: %s -- time to live must be specified", config.Name)
	}

	if config.BlockToLive != 0 {
		return errors.Errorf("collection-name: %s -- block-to-live not supported", config.Name)
	}

	_, err := time.ParseDuration(config.TimeToLive)
	if err != nil {
		return errors.Errorf("collection-name: %s -- invalid time format for time to live: %s", config.Name, err)
	}

	return nil
}

func (v *Validator) validateOffLedgerConfig(config *common.StaticCollectionConfig) error {
	if config.RequiredPeerCount <= 0 {
		return errors.Errorf("collection-name: %s -- required peer count must be greater than 0", config.Name)
	}

	if config.RequiredPeerCount > config.MaximumPeerCount {
		return errors.Errorf("collection-name: %s -- maximum peer count (%d) must be greater than or equal to required peer count (%d)", config.Name, config.MaximumPeerCount, config.RequiredPeerCount)
	}

	if config.BlockToLive != 0 {
		return errors.Errorf("collection-name: %s -- block-to-live not supported", config.Name)
	}

	if config.TimeToLive != "" {
		_, err := time.ParseDuration(config.TimeToLive)
		if err != nil {
			return errors.Errorf("collection-name: %s -- invalid time format for time to live: %s", config.Name, err)
		}
	}

	return nil
}

func getCollType(config *common.StaticCollectionConfig) common.CollectionType {
	switch config.Type {
	case common.CollectionType_COL_TRANSIENT:
		return common.CollectionType_COL_TRANSIENT
	case common.CollectionType_COL_DCAS:
		return common.CollectionType_COL_DCAS
	case common.CollectionType_COL_PRIVATE:
		fallthrough
	default:
		return common.CollectionType_COL_PRIVATE
	}
}
