/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package peergroup

// Iterator is a peer group iterator
type Iterator struct {
	peerGroups    PeerGroups
	startingIndex int
	index         int
	done          bool
}

// NewIterator returns a new peer group iterator
func NewIterator(peerGroups PeerGroups, index int) *Iterator {
	return &Iterator{
		peerGroups:    peerGroups,
		startingIndex: index,
		index:         index,
	}
}

// Next returns the next peer group
func (it *Iterator) Next() PeerGroup {
	if it.done || len(it.peerGroups) == 0 {
		return nil
	}

	pg := it.peerGroups[it.index]
	it.index++
	if it.index >= len(it.peerGroups) {
		it.index = 0
	}
	if it.index == it.startingIndex {
		it.done = true
	}

	return pg
}
