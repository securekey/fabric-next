/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package privdata

import (
	"github.com/hyperledger/fabric/core/common/privdata"
	kdissemination "github.com/hyperledger/fabric/extensions/collections/dissemination"
	"github.com/hyperledger/fabric/protos/common"
	proto "github.com/hyperledger/fabric/protos/gossip"
	"github.com/hyperledger/fabric/protos/ledger/rwset"
)

func (d *distributorImpl) disseminationPlanForExtensions(ns string, rwSet *rwset.CollectionPvtReadWriteSet, colCP *common.CollectionConfig, colAP privdata.CollectionAccessPolicy, pvtDataMsg *proto.SignedGossipMessage) ([]*dissemination, error) {
	dissPlan, handled, err := kdissemination.ComputeDisseminationPlan(d.chainID, ns, rwSet, colCP, colAP, pvtDataMsg, d.gossipAdapter)
	if err != nil {
		return nil, err
	}

	if !handled {
		// Use default dissemination plan
		return d.disseminationPlanForMsg(colAP, colAP.AccessFilter(), pvtDataMsg)
	}

	dPlan := make([]*dissemination, len(dissPlan))
	for i, dp := range dissPlan {
		dPlan[i] = &dissemination{msg: dp.Msg, criteria: dp.Criteria}
	}
	return dPlan, nil
}
