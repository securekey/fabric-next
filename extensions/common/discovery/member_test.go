/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package discovery

import (
	"fmt"
	"testing"

	"github.com/hyperledger/fabric/gossip/discovery"
	"github.com/stretchr/testify/require"
)

const (
	org1MSP = "org1MSP"

	p0Endpoint = "p0.org1.com"
	p1Endpoint = "p1.org1.com"
	p2Endpoint = "p2.org1.com"
	p3Endpoint = "p3.org1.com"
	p4Endpoint = "p4.org1.com"
	p5Endpoint = "p5.org1.com"
	p6Endpoint = "p6.org1.com"
	p7Endpoint = "p7.org1.com"
)

var (
	p1 = newMember(org1MSP, p1Endpoint, false)
	p2 = newMember(org1MSP, p2Endpoint, false)
	p3 = newMember(org1MSP, p3Endpoint, false)
	p4 = newMember(org1MSP, p4Endpoint, false)
	p5 = newMember(org1MSP, p5Endpoint, true)
	p6 = newMember(org1MSP, p6Endpoint, false)
	p7 = newMember(org1MSP, p7Endpoint, false)
)

type testCase struct {
	endpoint string
	member   *Member
}

func TestMember(t *testing.T) {
	testCases := []testCase{
		{
			p1Endpoint,
			p1,
		},
		{
			p2Endpoint,
			p2,
		},
		{
			p3Endpoint,
			p3,
		},
		{
			p4Endpoint,
			p4,
		},
		{
			p5Endpoint,
			p5,
		},
		{
			p6Endpoint,
			p6,
		},
		{
			p7Endpoint,
			p7,
		},
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Testing Member %s", tc.member), func(t *testing.T) {
			require.Equal(t, tc.endpoint, fmt.Sprintf("%s", tc.member), "Member doesn't match Endpoint assigned")
			if tc.member == p5 {
				require.True(t, tc.member.Local)
			} else {
				require.False(t, tc.member.Local)
			}
		})
	}
}

func newMember(mspID, endpoint string, local bool) *Member {
	m := &Member{
		NetworkMember: discovery.NetworkMember{
			Endpoint: endpoint,
		},
		MSPID: mspID,
		Local: local,
	}
	return m
}
