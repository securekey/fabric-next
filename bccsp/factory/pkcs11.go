// +build pkcs11

/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package factory

import (
	"github.com/hyperledger/fabric/bccsp"
	"github.com/hyperledger/fabric/bccsp/pkcs11"
	"github.com/pkg/errors"
	"github.com/hyperledger/fabric/bccsp/grep11"
)

// FactoryOpts holds configuration information used to initialize factory implementations
type FactoryOpts struct {
	ProviderName string             `mapstructure:"default" json:"default" yaml:"Default"`
	SwOpts       *SwOpts            `mapstructure:"SW,omitempty" json:"SW,omitempty" yaml:"SwOpts"`
	PluginOpts   *PluginOpts        `mapstructure:"PLUGIN,omitempty" json:"PLUGIN,omitempty" yaml:"PluginOpts"`
	Pkcs11Opts   *pkcs11.PKCS11Opts `mapstructure:"PKCS11,omitempty" json:"PKCS11,omitempty" yaml:"PKCS11"`
	Grep11Opts   *grep11.GREP11Opts `mapstructure:"GREP11,omitempty" json:"GREP11,omitempty" yaml:"GREP11"`
}

// InitFactories must be called before using factory interfaces
// It is acceptable to call with config = nil, in which case
// some defaults will get used
// Error is returned only if defaultBCCSP cannot be found
func InitFactories(config *FactoryOpts) error {
	factoriesInitOnce.Do(func() {
		setFactories(config)
	})

	return factoriesInitError
}

func setFactories(config *FactoryOpts) error {
	// Take some precautions on default opts
	if config == nil {
		config = GetDefaultOpts()
	}

	if config.ProviderName == "" {
		config.ProviderName = "SW"
	}

	if config.SwOpts == nil {
		config.SwOpts = GetDefaultOpts().SwOpts
	}

	// Initialize factories map
	bccspMap = make(map[string]bccsp.BCCSP)

	// Software-Based BCCSP
	if config.SwOpts != nil {
		f := &SWFactory{}
		err := initBCCSP(f, config)
		if err != nil {
			factoriesInitError = errors.Wrap(err, "Failed initializing SW.BCCSP")
		}
	}

	// PKCS11-Based BCCSP
	if config.Pkcs11Opts != nil {
		f := &PKCS11Factory{}
		err := initBCCSP(f, config)
		if err != nil {
			factoriesInitError = errors.Wrapf(err, "Failed initializing PKCS11.BCCSP %s", factoriesInitError)
		}
	}

	// BCCSP Plugin
	if config.PluginOpts != nil {
		f := &PluginFactory{}
		err := initBCCSP(f, config)
		if err != nil {
			factoriesInitError = errors.Wrapf(err, "Failed initializing PKCS11.BCCSP %s", factoriesInitError)
		}
	}

	// GREP11-Based BCCSP
	if config.Grep11Opts != nil {
		f := &GREP11Factory{}
		err := initBCCSP(f, config)
		if err != nil {
			factoriesInitError = errors.Errorf("Failed initializing GREP11.BCCSP %s\n[%s]", factoriesInitError, err)
		}
	}

	var ok bool
	defaultBCCSP, ok = bccspMap[config.ProviderName]
	if !ok {
		factoriesInitError = errors.Errorf("%s\nCould not find default `%s` BCCSP", factoriesInitError, config.ProviderName)
	}

	return factoriesInitError
}

// GetBCCSPFromOpts returns a BCCSP created according to the options passed in input.
func GetBCCSPFromOpts(config *FactoryOpts) (bccsp.BCCSP, error) {
	var f BCCSPFactory
	switch config.ProviderName {
	case "SW":
		f = &SWFactory{}
	case "PKCS11":
		f = &PKCS11Factory{}
	case "PLUGIN":
		f = &PluginFactory{}
	case "GREP11":
		f = &GREP11Factory{}
	default:
		return nil, errors.Errorf("Could not find BCCSP, no '%s' provider", config.ProviderName)
	}

	csp, err := f.Get(config)
	if err != nil {
		return nil, errors.Wrapf(err, "Could not initialize BCCSP %s", f.Name())
	}
	return csp, nil
}
