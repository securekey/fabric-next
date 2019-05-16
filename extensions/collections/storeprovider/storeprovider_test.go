/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package storeprovider

import (
	"testing"

	offledgerapi "github.com/hyperledger/fabric/extensions/collections/api/offledger"
	storeapi "github.com/hyperledger/fabric/extensions/collections/api/store"
	tdataapi "github.com/hyperledger/fabric/extensions/collections/api/transientdata"
	"github.com/hyperledger/fabric/extensions/collections/storeprovider/mocks"
	kmocks "github.com/hyperledger/fabric/extensions/mocks"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreProvider(t *testing.T) {
	tdataProvider := mocks.NewTransientDataStoreProvider()
	olProvider := mocks.NewStoreProvider()

	newTransientDataProviderFactory = func() tdataapi.StoreProvider {
		return tdataProvider
	}

	newOffLedgerProviderFactory = func() offledgerapi.StoreProvider {
		return olProvider
	}

	t.Run("OpenStore - success", func(t *testing.T) {
		p := NewProviderFactory()
		require.NotNil(t, p)

		s, err := p.OpenStore("testchannel")
		require.NoError(t, err)
		require.NotNil(t, s)

		s2 := p.StoreForChannel("testchannel")
		require.Equal(t, s, s2)
	})

	t.Run("OpenStore - transient data error", func(t *testing.T) {
		p := NewProviderFactory()
		require.NotNil(t, p)

		expectedErr := errors.New("transientdata error")
		tdataProvider.Error(expectedErr)
		defer tdataProvider.Error(nil)

		s, err := p.OpenStore("testchannel")
		assert.EqualError(t, err, expectedErr.Error())
		require.Nil(t, s)
	})

	t.Run("OpenStore - DCAS data error", func(t *testing.T) {
		p := NewProviderFactory()
		require.NotNil(t, p)

		expectedErr := errors.New("dcas error")
		olProvider.Error(expectedErr)
		defer olProvider.Error(nil)

		s, err := p.OpenStore("testchannel")
		assert.EqualError(t, err, expectedErr.Error())
		require.Nil(t, s)
	})

	t.Run("Close - success", func(t *testing.T) {
		p := NewProviderFactory()
		require.NotNil(t, p)

		s, err := p.OpenStore("testchannel")
		require.NoError(t, err)
		require.NotNil(t, s)

		assert.False(t, tdataProvider.IsStoreClosed())
		assert.False(t, olProvider.IsStoreClosed())

		p.Close()

		assert.True(t, tdataProvider.IsStoreClosed())
		assert.True(t, olProvider.IsStoreClosed())
	})
}

func TestStore_PutAndGetData(t *testing.T) {
	const (
		tx1   = "tx1"
		ns1   = "ns1"
		coll1 = "coll1"
		coll2 = "coll2"
		key1  = "key1"
		key2  = "key2"
	)

	k1 := storeapi.NewKey(tx1, ns1, coll1, key1)
	k2 := storeapi.NewKey(tx1, ns1, coll1, key2)
	k3 := storeapi.NewKey(tx1, ns1, coll2, key1)

	v1 := &storeapi.ExpiringValue{Value: []byte("value1")}
	v2 := &storeapi.ExpiringValue{Value: []byte("value1")}

	tdataProvider := mocks.NewTransientDataStoreProvider()
	olProvider := mocks.NewStoreProvider()

	newTransientDataProviderFactory = func() tdataapi.StoreProvider {
		return tdataProvider.Data(k1, v1).Data(k2, v2)
	}

	newOffLedgerProviderFactory = func() offledgerapi.StoreProvider {
		return olProvider.Data(k1, v2).Data(k2, v1)
	}

	p := NewProviderFactory()
	require.NotNil(t, p)

	s, err := p.OpenStore("testchannel")
	require.NoError(t, err)
	require.NotNil(t, s)

	t.Run("GetTransientData", func(t *testing.T) {
		value, err := s.GetTransientData(k1)
		require.NoError(t, err)
		require.NotNil(t, value)

		values, err := s.GetTransientDataMultipleKeys(storeapi.NewMultiKey(tx1, ns1, coll1, key1, key2))
		require.NoError(t, err)
		assert.Equal(t, 2, len(values))
	})

	t.Run("GetData", func(t *testing.T) {
		value, err := s.GetData(k1)
		require.NoError(t, err)
		require.NotNil(t, value)

		values, err := s.GetDataMultipleKeys(storeapi.NewMultiKey(tx1, ns1, coll1, key1, key2))
		require.NoError(t, err)
		assert.Equal(t, 2, len(values))
	})

	t.Run("PutData", func(t *testing.T) {
		collConfig := &common.StaticCollectionConfig{
			Type: common.CollectionType_COL_DCAS,
			Name: coll2,
		}
		err := s.PutData(collConfig, k3, v1)
		require.NoError(t, err)
	})

	t.Run("Persist", func(t *testing.T) {
		isCommitter = func() bool { return true }

		err := s.Persist(tx1, kmocks.NewPvtReadWriteSetBuilder().Build())
		assert.NoError(t, err)

		expectedErr := errors.New("transient data error")
		tdataProvider.StoreError(expectedErr)
		err = s.Persist(tx1, kmocks.NewPvtReadWriteSetBuilder().Build())
		require.Error(t, err)
		assert.Contains(t, err.Error(), expectedErr.Error())
		tdataProvider.StoreError(nil)

		expectedErr = errors.New("DCAS error")
		olProvider.StoreError(expectedErr)
		err = s.Persist(tx1, kmocks.NewPvtReadWriteSetBuilder().Build())
		require.Error(t, err)
		assert.Contains(t, err.Error(), expectedErr.Error())
		olProvider.StoreError(nil)
	})
}
