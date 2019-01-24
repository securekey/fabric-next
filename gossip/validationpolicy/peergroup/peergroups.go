/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package peergroup

import (
	"github.com/hyperledger/fabric/gossip/roleutil"
	"sort"
)

// PeerGroups is a group of peers
type PeerGroups []PeerGroup

func (g PeerGroups) String() string {
	s := "("
	for i, p := range g {
		s += p.String()
		if i+1 < len(g) {
			s += ", "
		}
	}
	s += ")"
	return s
}

// Sort sorts the peer group by endpoint
func (g PeerGroups) Sort() PeerGroups {
	// First sort each peer group
	for _, pg := range g {
		pg.Sort()
	}
	// Now sort the peer groups
	sort.Sort(g)

	return g
}

func (g PeerGroups) Contains(p *roleutil.Member) bool {
	for _, pg := range g {
		if pg.Contains(p) {
			return true
		}
	}
	return false
}

func (g PeerGroups) ContainsAll(peerGroup PeerGroup) bool {
	for _, p := range peerGroup {
		if !g.Contains(p) {
			return false
		}
	}
	return true
}

func (g PeerGroups) ContainsAny(peerGroup PeerGroup) bool {
	for _, pg := range g {
		if pg.ContainsAny(peerGroup) {
			return true
		}
	}
	return false
}

func (g PeerGroups) Len() int {
	return len(g)
}

func (g PeerGroups) Less(i, j int) bool {
	return g[i].String() < g[j].String()
}

func (g PeerGroups) Swap(i, j int) {
	g[i], g[j] = g[j], g[i]
}
