/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package dcas

import (
	"testing"

	storeapi "github.com/hyperledger/fabric/extensions/collections/api/store"
	"github.com/hyperledger/fabric/extensions/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	ns1   = "chaincode1"
	coll1 = "coll1"
	txID1 = "txid1"
)

func TestValidator(t *testing.T) {
	value := []byte("value1")

	t.Run("Valid key/value -> success", func(t *testing.T) {
		err := Validator("", "", "", common.GetCASKey(value), value)
		assert.NoError(t, err)
	})

	t.Run("Invalid key -> error", func(t *testing.T) {
		err := Validator("", "", "", "key1", value)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "the key should be the hash of the value")
	})

	t.Run("Nil value -> error", func(t *testing.T) {
		err := Validator("", "", "", "key1", nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nil value for key")
	})
}

func TestDecorator(t *testing.T) {
	value1_1 := []byte("value1_1")
	value := &storeapi.ExpiringValue{
		Value: value1_1,
	}

	t.Run("CAS key -> success", func(t *testing.T) {
		key := storeapi.NewKey(txID1, ns1, coll1, common.GetCASKey(value1_1))
		k, v, err := Decorator(key, value)
		assert.NoError(t, err)
		assert.Equal(t, key, k)
		assert.Equal(t, value, v)
	})

	t.Run("Empty key -> success", func(t *testing.T) {
		key := storeapi.NewKey(txID1, ns1, coll1, "")
		k, v, err := Decorator(key, value)
		assert.NoError(t, err)
		assert.Equal(t, common.GetCASKey(value1_1), k.Key)
		assert.Equal(t, value, v)
	})

	t.Run("Invalid key -> error", func(t *testing.T) {
		key := storeapi.NewKey(txID1, ns1, coll1, "key1")
		k, v, err := Decorator(key, value)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "the key should be the hash of the value")
		assert.Nil(t, k)
		assert.Nil(t, v)
	})

	t.Run("Nil value -> error", func(t *testing.T) {
		k, v, err := Decorator(storeapi.NewKey(txID1, ns1, coll1, "key1"), &storeapi.ExpiringValue{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "nil value for key")
		assert.Nil(t, k)
		assert.Nil(t, v)
	})
}
