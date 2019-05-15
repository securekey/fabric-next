/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package discovery

import (
	"fmt"

	"github.com/hyperledger/fabric/common/graph"
)

// Permuations calculates the permutations of the peer groups.
type Permuations struct {
	vr    *graph.TreeVertex
	index int
}

// NewPermutations returns a new Permutations struct
func NewPermutations() *Permuations {
	return &Permuations{
		vr: graph.NewTreeVertex("root", nil),
	}
}

// Groups adds the given peer groups
func (p *Permuations) Groups(peerGroups ...PeerGroup) *Permuations {
	for _, pg := range peerGroups {
		p.addGroup(pg)
	}
	return p
}

// Evaluate calculates the permutations of the peer groups.
// For example:
//   If the given peer groups are [(p1,p2), (p3,p4)] then the return value
//   will be [(p1,p3),(p1,p4),(p2,p3),(p2,p4)]
func (p *Permuations) Evaluate() PeerGroups {
	var groups PeerGroups
	for _, permutation := range p.vr.ToTree().Permute() {
		groups = append(groups, combinations(permutation.BFS()))
	}
	return groups
}

func (p *Permuations) addGroup(pg PeerGroup) *Permuations {
	p.index++
	p.vr.Threshold = p.index

	gvr := p.vr.AddDescendant(graph.NewTreeVertex(fmt.Sprintf("%d", p.index), nil))
	gvr.Threshold = 1

	for _, p := range pg {
		gvr.AddDescendant(graph.NewTreeVertex(p.Endpoint, p))
	}
	return p
}

func combinations(it graph.Iterator) PeerGroup {
	var peerGroup PeerGroup
	for v := it.Next(); v != nil; v = it.Next() {
		if v.Data != nil {
			peerGroup = append(peerGroup, v.Data.(*Member))
		}
	}
	return peerGroup
}
