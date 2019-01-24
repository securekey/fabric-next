/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package peergroup

import (
	"sort"

	"github.com/hyperledger/fabric/gossip/roleutil"
)

// PeerGroup is a group of peers
type PeerGroup []*roleutil.Member

// ContainsLocal return true if one of the peers in the group is the local peer
func (g PeerGroup) ContainsLocal() bool {
	for _, p := range g {
		if p.Local {
			return true
		}
	}
	return false
}

func (g PeerGroup) String() string {
	s := "["
	for i, p := range g {
		s += p.String()
		if i+1 < len(g) {
			s += ", "
		}
	}
	s += "]"
	return s
}

// Sort sorts the peer group by endpoint
func (g PeerGroup) Sort() PeerGroup {
	sort.Sort(g)
	return g
}

// ContainsAll returns true if ALL of the peers within the given peer group are contained within this peer group
func (g PeerGroup) ContainsAll(peerGroup PeerGroup) bool {
	for _, p := range peerGroup {
		if !g.Contains(p) {
			return false
		}
	}
	return true
}

// ContainsAny returns true if ANY of the peers within the given peer group are contained within this peer group
func (g PeerGroup) ContainsAny(peerGroup PeerGroup) bool {
	for _, p := range peerGroup {
		if g.Contains(p) {
			return true
		}
	}
	return false
}

// Contains returns true if the given peer is contained within this peer group
func (g PeerGroup) Contains(peer *roleutil.Member) bool {
	for _, p := range g {
		if p.Endpoint == peer.Endpoint {
			return true
		}
	}
	return false
}

func (g PeerGroup) Len() int {
	return len(g)
}

func (g PeerGroup) Less(i, j int) bool {
	return g[i].Endpoint < g[j].Endpoint
}

func (g PeerGroup) Swap(i, j int) {
	g[i], g[j] = g[j], g[i]
}
