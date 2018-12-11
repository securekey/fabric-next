/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package statekeyindex

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarshalMetadata(t *testing.T) {
	em := Metadata{
		BlockNumber: 1,
		TxNumber:    2,
	}

	eb, err := MarshalMetadata(&em)
	assert.NoError(t, err, "marshaling metadata should succeed")

	am, err := UnmarshalMetadata(eb)
	assert.NoError(t, err, "unmarshaling metadata should succeed")

	assert.EqualValues(t, em, am, "marshal/unmarshal should result in same value")
}

func TestGet(t *testing.T) {
	dbfile, err := ioutil.TempDir("", "statekeyindextest")
	require.NoError(t, err)
	defer os.Remove(dbfile)
	const dbname = "test"
	key := CompositeKey{Key: "123", Namespace: "namespace"}
	expected := Metadata{BlockNumber: 5, TxNumber: 3}
	stateidx := newStateKeyIndex(
		leveldbhelper.NewProvider(
			&leveldbhelper.Conf{DBPath: dbfile},
		).GetDBHandle(dbname),
		dbname,
	)
	err = stateidx.AddIndex([]*IndexUpdate{{
		Key:   key,
		Value: expected,
	}})
	require.NoError(t, err)
	actual, found, err := stateidx.GetMetadata(&key)
	require.NoError(t, err)
	assert.True(t, found, "expected the metadata to be found")
	assert.Equal(t, expected, actual, "metadata added to the index should be equal to that retrieved using GetMetadata()")
}
