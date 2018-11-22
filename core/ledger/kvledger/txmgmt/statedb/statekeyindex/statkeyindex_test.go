/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package statekeyindex

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMarshalMetadata(t *testing.T) {
	em := Metadata{
		BlockNumber: 1,
		TxNumber: 2,
	}

	eb, err := MarshalMetadata(&em)
	assert.NoError(t, err, "marshaling metadata should succeed")

	am, err := UnmarshalMetadata(eb)
	assert.NoError(t, err, "unmarshaling metadata should succeed")

	assert.EqualValues(t, &em, am, "marshal/unmarshal should result in same value")
}