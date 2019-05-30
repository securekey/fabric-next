/*
	Copyright Digital Asset Holdings, LLC. All Rights Reserved.
	Copyright IBM Corp. All Rights Reserved.

	SPDX-License-Identifier: Apache-2.0
*/

package chaincode

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/cauthdsl"
	pcommon "github.com/hyperledger/fabric/protos/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const sampleCollectionConfigTransient = `[
		{
			"name": "foo",
			"policy": "OR('A.member', 'B.member')",
			"requiredPeerCount": 3,
			"maxPeerCount": 5,
			"type": "TRANSIENT",
			"timeToLive": "2m"
		}
	]`

const sampleCollectionConfigInvalidType = `[
		{
			"name": "foo",
			"policy": "OR('A.member', 'B.member')",
			"requiredPeerCount": 3,
			"maxPeerCount": 5,
			"type": "INVALID"
		}
	]`

func TestCollectionTypeParsing(t *testing.T) {
	pol, _ := cauthdsl.FromString("OR('A.member', 'B.member')")

	t.Run("Invalid Collection Config Type", func(t *testing.T) {
		_, err := getCollectionConfigFromBytes([]byte(sampleCollectionConfigInvalidType))
		assert.Error(t, err)
	})

	t.Run("Transient Collection Config", func(t *testing.T) {
		cc, err := getCollectionConfigFromBytes([]byte(sampleCollectionConfigTransient))
		assert.NoError(t, err)
		assert.NotNil(t, cc)
		ccp := &pcommon.CollectionConfigPackage{Config: []*pcommon.CollectionConfig{}}
		err = proto.Unmarshal(cc, ccp)
		assert.NoError(t, err)
		conf := ccp.Config[0].GetStaticCollectionConfig()
		require.NotNil(t, conf)
		assert.Equal(t, "foo", conf.Name)
		assert.Equal(t, int32(3), conf.RequiredPeerCount)
		assert.Equal(t, int32(5), conf.MaximumPeerCount)
		assert.True(t, proto.Equal(pol, conf.MemberOrgsPolicy.GetSignaturePolicy()))
		assert.Equal(t, "2m", conf.TimeToLive)
	})
}
