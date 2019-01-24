/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package peergroup

type Iterator struct {
	peerGroups    PeerGroups
	startingIndex int
	index         int
	done          bool
}

func NewIterator(peerGroups PeerGroups, index int) *Iterator {
	return &Iterator{
		peerGroups:    peerGroups,
		startingIndex: index,
		index:         index,
	}
}

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
