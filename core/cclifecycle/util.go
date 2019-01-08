/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cc

import (
	"time"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/chaincode"
	"github.com/hyperledger/fabric/common/util/retry"
	"github.com/hyperledger/fabric/core/common/ccprovider"
	"github.com/hyperledger/fabric/core/common/privdata"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/pkg/errors"
)

var (
	// AcceptAll returns a predicate that accepts all Metadata
	AcceptAll ChaincodePredicate = func(cc chaincode.Metadata) bool {
		return true
	}
)

// ChaincodePredicate accepts or rejects chaincode based on its metadata
type ChaincodePredicate func(cc chaincode.Metadata) bool

var errEmptySet = errors.New("empty set")

// DeployedChaincodes retrieves the metadata of the given deployed chaincodes
func DeployedChaincodes(q Query, filter ChaincodePredicate, loadCollections bool, chaincodes ...string) (chaincode.MetadataSet, error) {
	defer q.Done()

	if ledgerconfig.IsCommitter() {
		set, err := getDeployedChaincodes(q, filter, loadCollections, chaincodes...)
		if err != nil {
			Logger.Errorf("Query failed: %s", err)
			return nil, err
		}
		return set, nil
	}

	// The data may not be in the state DB yet. Try a few times.
	Logger.Debugf("I am NOT a committer. Attempting to retrieve deployed chaincodes from state DB...")

	// TODO: Make configurable
	maxAttempts := 10
	maxBackoff := 2 * time.Second

	set, err := retry.Invoke(
		func() (interface{}, error) {
			set, err := getDeployedChaincodes(q, filter, loadCollections, chaincodes...)
			if err != nil {
				return nil, err
			}
			if len(set) == 0 {
				return nil, errEmptySet
			}
			return set, nil
		},
		retry.WithMaxAttempts(maxAttempts),
		retry.WithMaxBackoff(maxBackoff),
		retry.WithBeforeRetry(func(err error, attempt int, backoff time.Duration) bool {
			if err == errEmptySet {
				Logger.Debugf("Got empty set on attempt #%d: %s. Retrying in %s.", attempt, err, backoff)
				return true
			}
			Logger.Errorf("Query failed on attempt #%d: %s. No retry will be attempted.", attempt, err)
			return false
		}),
	)
	if err == errEmptySet {
		return nil, nil
	}
	if set == nil {
		return nil, err
	}
	return set.(chaincode.MetadataSet), nil
}

func getDeployedChaincodes(q Query, filter ChaincodePredicate, loadCollections bool, chaincodes ...string) (chaincode.MetadataSet, error) {
	var res chaincode.MetadataSet
	for _, cc := range chaincodes {
		data, err := q.GetState("lscc", cc)
		if err != nil {
			Logger.Error("Failed querying lscc namespace:", err)
			return nil, errors.WithStack(err)
		}
		if len(data) == 0 {
			Logger.Info("Chaincode", cc, "isn't instantiated")
			continue
		}
		ccInfo, err := extractCCInfo(data)
		if err != nil {
			Logger.Error("Failed extracting chaincode info about", cc, "from LSCC returned payload. Error:", err)
			continue
		}
		if ccInfo.Name != cc {
			Logger.Error("Chaincode", cc, "is listed in LSCC as", ccInfo.Name)
			continue
		}

		instCC := chaincode.Metadata{
			Name:    ccInfo.Name,
			Version: ccInfo.Version,
			Id:      ccInfo.Id,
			Policy:  ccInfo.Policy,
		}

		if !filter(instCC) {
			Logger.Debug("Filtered out", instCC)
			continue
		}

		if loadCollections {
			key := privdata.BuildCollectionKVSKey(cc)
			collectionData, err := q.GetState("lscc", key)
			if err != nil {
				Logger.Errorf("Failed querying lscc namespace for %s: %v", key, err)
				return nil, errors.WithStack(err)
			}
			instCC.CollectionsConfig = collectionData
			Logger.Debug("Retrieved collection config for", cc, "from", key)
		}

		res = append(res, instCC)
	}
	Logger.Debug("Returning", res)
	return res, nil
}

func deployedCCToNameVersion(cc chaincode.Metadata) nameVersion {
	return nameVersion{
		name:    cc.Name,
		version: cc.Version,
	}
}

func extractCCInfo(data []byte) (*ccprovider.ChaincodeData, error) {
	cd := &ccprovider.ChaincodeData{}
	if err := proto.Unmarshal(data, cd); err != nil {
		return nil, errors.Wrap(err, "failed unmarshaling lscc read value into ChaincodeData")
	}
	return cd, nil
}

type nameVersion struct {
	name    string
	version string
}

func installedCCToNameVersion(cc chaincode.InstalledChaincode) nameVersion {
	return nameVersion{
		name:    cc.Name,
		version: cc.Version,
	}
}

func names(installedChaincodes []chaincode.InstalledChaincode) []string {
	var ccs []string
	for _, cc := range installedChaincodes {
		ccs = append(ccs, cc.Name)
	}
	return ccs
}
