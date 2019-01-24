/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package graph

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestF(t *testing.T) {
	vR := NewTreeVertex("r", nil)
	vR.Threshold = 2

	vD := vR.AddDescendant(NewTreeVertex("D", nil))
	vD.Threshold = 2
	for _, id := range []string{"A", "B", "C"} {
		vD.AddDescendant(NewTreeVertex(id, nil))
	}

	vE := vR.AddDescendant(NewTreeVertex("E", nil))
	vE.Threshold = 2
	for _, id := range []string{"a", "b", "c"} {
		vE.AddDescendant(NewTreeVertex(id, nil))
	}

	vF := vR.AddDescendant(NewTreeVertex("F", nil))
	vF.Threshold = 2
	for _, id := range []string{"1", "2", "3"} {
		vF.AddDescendant(NewTreeVertex(id, nil))
	}

	permutations := vR.ToTree().Permute()
	// For a sub-tree with r-(D,E) we have 9 combinations (3 combinations of each sub-tree where D and E are the roots)
	// For a sub-tree with r-(D,F) we have 9 combinations from the same logic
	// For a sub-tree with r-(E,F) we have 9 combinations too
	// Total 27 combinations
	assert.Equal(t, 27, len(permutations))

	listCombination := func(i Iterator) []string {
		var traversal []string
		for {
			v := i.Next()
			if v == nil {
				break
			}
			traversal = append(traversal, v.Id)
		}
		return traversal
	}

	// First combination is a left most traversal on the combination graph
	expectedScan := []string{"r", "D", "E", "A", "B", "a", "b"}
	assert.Equal(t, expectedScan, listCombination(permutations[0].BFS()))

	// Last combination is a right most traversal on the combination graph
	expectedScan = []string{"r", "E", "F", "b", "c", "2", "3"}
	assert.Equal(t, expectedScan, listCombination(permutations[26].BFS()))
}

func TestG(t *testing.T) {
	vR := NewTreeVertex("parent-0", nil)
	vR.Threshold = 3

	vD := vR.AddDescendant(NewTreeVertex("parent-1", nil))
	vD.Threshold = 1
	for _, id := range []string{"A", "B", "C"} {
		vD.AddDescendant(NewTreeVertex(id, nil))
	}

	vE := vR.AddDescendant(NewTreeVertex("parent-2", nil))
	vE.Threshold = 1
	for _, id := range []string{"a", "b", "c"} {
		vE.AddDescendant(NewTreeVertex(id, nil))
	}

	vF := vR.AddDescendant(NewTreeVertex("parent-3", nil))
	vF.Threshold = 1
	for _, id := range []string{"1", "2", "3"} {
		vF.AddDescendant(NewTreeVertex(id, nil))
	}

	permutations := vR.ToTree().Permute()
	// assert.Equal(t, 6, len(permutations))

	listCombination := func(i Iterator) []string {
		var traversal []string
		for {
			v := i.Next()
			if v == nil {
				break
			}
			if !strings.Contains(v.Id, "parent-") {
				traversal = append(traversal, v.Id)
			}
		}
		return traversal
	}

	for i, permutation := range permutations {
		t.Logf("%d: %s", i, listCombination(permutation.BFS()))
	}

}
