/*
Copyright IBM Corp. 2016, 2017 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

		 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package statecouchdb

import (
	"archive/tar"
	"bytes"
	"log"
	"os"
	"testing"
	"time"

	"github.com/hyperledger/fabric/common/ledger/testutil"
	"github.com/hyperledger/fabric/core/ledger/cceventmgmt"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb/commontests"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	ledgertestutil "github.com/hyperledger/fabric/core/ledger/testutil"
	"github.com/spf13/viper"
)

func TestMain(m *testing.M) {

	// Read the core.yaml file for default config.
	ledgertestutil.SetupCoreYAMLConfig()
	viper.Set("peer.fileSystemPath", "/tmp/fabric/ledgertests/kvledger/txmgmt/statedb/statecouchdb")

	// Switch to CouchDB
	viper.Set("ledger.state.stateDatabase", "CouchDB")

	// both vagrant and CI have couchdb configured at host "couchdb"
	viper.Set("ledger.state.couchDBConfig.couchDBAddress", "couchdb:5984")
	// Replace with correct username/password such as
	// admin/admin if user security is enabled on couchdb.
	viper.Set("ledger.state.couchDBConfig.username", "")
	viper.Set("ledger.state.couchDBConfig.password", "")
	viper.Set("ledger.state.couchDBConfig.maxRetries", 3)
	viper.Set("ledger.state.couchDBConfig.maxRetriesOnStartup", 10)
	viper.Set("ledger.state.couchDBConfig.requestTimeout", time.Second*35)

	//run the actual test
	result := m.Run()

	//revert to default goleveldb
	viper.Set("ledger.state.stateDatabase", "goleveldb")
	os.Exit(result)
}

func TestBasicRW(t *testing.T) {
	env := NewTestVDBEnv(t)
	env.Cleanup("testbasicrw_")
	env.Cleanup("testbasicrw_ns")
	env.Cleanup("testbasicrw_ns1")
	env.Cleanup("testbasicrw_ns2")
	defer env.Cleanup("testbasicrw_")
	defer env.Cleanup("testbasicrw_ns")
	defer env.Cleanup("testbasicrw_ns1")
	defer env.Cleanup("testbasicrw_ns2")
	commontests.TestBasicRW(t, env.DBProvider)

}

func TestMultiDBBasicRW(t *testing.T) {
	env := NewTestVDBEnv(t)
	env.Cleanup("testmultidbbasicrw_")
	env.Cleanup("testmultidbbasicrw_ns1")
	env.Cleanup("testmultidbbasicrw2_")
	env.Cleanup("testmultidbbasicrw2_ns1")
	defer env.Cleanup("testmultidbbasicrw_")
	defer env.Cleanup("testmultidbbasicrw_ns1")
	defer env.Cleanup("testmultidbbasicrw2_")
	defer env.Cleanup("testmultidbbasicrw2_ns1")
	commontests.TestMultiDBBasicRW(t, env.DBProvider)

}

func TestDeletes(t *testing.T) {
	env := NewTestVDBEnv(t)
	env.Cleanup("testdeletes_")
	env.Cleanup("testdeletes_ns")
	defer env.Cleanup("testdeletes_")
	defer env.Cleanup("testdeletes_ns")
	commontests.TestDeletes(t, env.DBProvider)
}

func TestIterator(t *testing.T) {
	env := NewTestVDBEnv(t)
	env.Cleanup("testiterator_")
	env.Cleanup("testiterator_ns1")
	env.Cleanup("testiterator_ns2")
	env.Cleanup("testiterator_ns3")
	defer env.Cleanup("testiterator_")
	defer env.Cleanup("testiterator_ns1")
	defer env.Cleanup("testiterator_ns2")
	defer env.Cleanup("testiterator_ns3")
	commontests.TestIterator(t, env.DBProvider)
}

func TestEncodeDecodeValueAndVersion(t *testing.T) {
	testValueAndVersionEncoding(t, []byte("value1"), version.NewHeight(1, 2))
	testValueAndVersionEncoding(t, []byte{}, version.NewHeight(50, 50))
}

func testValueAndVersionEncoding(t *testing.T, value []byte, version *version.Height) {
	encodedValue := statedb.EncodeValue(value, version)
	val, ver := statedb.DecodeValue(encodedValue)
	testutil.AssertEquals(t, val, value)
	testutil.AssertEquals(t, ver, version)
}

// The following tests are unique to couchdb, they are not used in leveldb
//  query test
func TestQuery(t *testing.T) {
	env := NewTestVDBEnv(t)
	env.Cleanup("testquery_")
	env.Cleanup("testquery_ns1")
	env.Cleanup("testquery_ns2")
	env.Cleanup("testquery_ns3")
	defer env.Cleanup("testquery_")
	defer env.Cleanup("testquery_ns1")
	defer env.Cleanup("testquery_ns2")
	defer env.Cleanup("testquery_ns3")
	commontests.TestQuery(t, env.DBProvider)
}

func TestGetStateMultipleKeys(t *testing.T) {

	env := NewTestVDBEnv(t)
	env.Cleanup("testgetmultiplekeys_")
	env.Cleanup("testgetmultiplekeys_ns1")
	env.Cleanup("testgetmultiplekeys_ns2")
	defer env.Cleanup("testgetmultiplekeys_")
	defer env.Cleanup("testgetmultiplekeys_ns1")
	defer env.Cleanup("testgetmultiplekeys_ns2")
	commontests.TestGetStateMultipleKeys(t, env.DBProvider)
}

func TestGetVersion(t *testing.T) {
	env := NewTestVDBEnv(t)
	env.Cleanup("testgetversion_")
	env.Cleanup("testgetversion_ns")
	env.Cleanup("testgetversion_ns2")
	defer env.Cleanup("testgetversion_")
	defer env.Cleanup("testgetversion_ns")
	defer env.Cleanup("testgetversion_ns2")
	commontests.TestGetVersion(t, env.DBProvider)
}

func TestSmallBatchSize(t *testing.T) {
	viper.Set("ledger.state.couchDBConfig.maxBatchUpdateSize", 2)
	env := NewTestVDBEnv(t)
	env.Cleanup("testsmallbatchsize_")
	env.Cleanup("testsmallbatchsize_ns1")
	defer env.Cleanup("testsmallbatchsize_")
	defer env.Cleanup("testsmallbatchsize_ns1")
	defer viper.Set("ledger.state.couchDBConfig.maxBatchUpdateSize", 1000)
	commontests.TestSmallBatchSize(t, env.DBProvider)
}

func TestBatchRetry(t *testing.T) {
	env := NewTestVDBEnv(t)
	env.Cleanup("testbatchretry_")
	env.Cleanup("testbatchretry_ns")
	env.Cleanup("testbatchretry_ns1")
	defer env.Cleanup("testbatchretry_")
	defer env.Cleanup("testbatchretry_ns")
	defer env.Cleanup("testbatchretry_ns1")
	commontests.TestBatchWithIndividualRetry(t, env.DBProvider)
}

// TestUtilityFunctions tests utility functions
func TestUtilityFunctions(t *testing.T) {

	env := NewTestVDBEnv(t)
	env.Cleanup("testutilityfunctions_")
	defer env.Cleanup("testutilityfunctions_")

	db, err := env.DBProvider.GetDBHandle("testutilityfunctions")
	testutil.AssertNoError(t, err, "")

	// BytesKeySuppoted should be false for CouchDB
	byteKeySupported := db.BytesKeySuppoted()
	testutil.AssertEquals(t, byteKeySupported, false)

	// ValidateKey should return nil for a valid key
	err = db.ValidateKey("testKey")
	testutil.AssertNil(t, err)

	// ValidateKey should return an error for an invalid key
	err = db.ValidateKey(string([]byte{0xff, 0xfe, 0xfd}))
	testutil.AssertError(t, err, "ValidateKey should have thrown an error for an invalid utf-8 string")

}

func TestDebugFunctions(t *testing.T) {

	//Test printCompositeKeys
	// initialize a key list
	loadKeys := []*statedb.CompositeKey{}
	//create a composite key and add to the key list
	compositeKey := statedb.CompositeKey{Namespace: "ns", Key: "key3"}
	loadKeys = append(loadKeys, &compositeKey)
	compositeKey = statedb.CompositeKey{Namespace: "ns", Key: "key4"}
	loadKeys = append(loadKeys, &compositeKey)
	testutil.AssertEquals(t, printCompositeKeys(loadKeys), "[ns,key4],[ns,key4]")

}

func TestHandleChaincodeDeploy(t *testing.T) {

	env := NewTestVDBEnv(t)
	env.Cleanup("testinit_")
	env.Cleanup("testinit_ns1")
	env.Cleanup("testinit_ns2")
	defer env.Cleanup("testinit_")
	defer env.Cleanup("testinit_ns1")
	defer env.Cleanup("testinit_ns2")

	db, err := env.DBProvider.GetDBHandle("testinit")
	testutil.AssertNoError(t, err, "")
	db.Open()
	defer db.Close()
	batch := statedb.NewUpdateBatch()

	jsonValue1 := "{\"asset_name\": \"marble1\",\"color\": \"blue\",\"size\": 1,\"owner\": \"tom\"}"
	batch.Put("ns1", "key1", []byte(jsonValue1), version.NewHeight(1, 1))
	jsonValue2 := "{\"asset_name\": \"marble2\",\"color\": \"blue\",\"size\": 2,\"owner\": \"jerry\"}"
	batch.Put("ns1", "key2", []byte(jsonValue2), version.NewHeight(1, 2))
	jsonValue3 := "{\"asset_name\": \"marble3\",\"color\": \"blue\",\"size\": 3,\"owner\": \"fred\"}"
	batch.Put("ns1", "key3", []byte(jsonValue3), version.NewHeight(1, 3))
	jsonValue4 := "{\"asset_name\": \"marble4\",\"color\": \"blue\",\"size\": 4,\"owner\": \"martha\"}"
	batch.Put("ns1", "key4", []byte(jsonValue4), version.NewHeight(1, 4))
	jsonValue5 := "{\"asset_name\": \"marble5\",\"color\": \"blue\",\"size\": 5,\"owner\": \"fred\"}"
	batch.Put("ns1", "key5", []byte(jsonValue5), version.NewHeight(1, 5))
	jsonValue6 := "{\"asset_name\": \"marble6\",\"color\": \"blue\",\"size\": 6,\"owner\": \"elaine\"}"
	batch.Put("ns1", "key6", []byte(jsonValue6), version.NewHeight(1, 6))
	jsonValue7 := "{\"asset_name\": \"marble7\",\"color\": \"blue\",\"size\": 7,\"owner\": \"fred\"}"
	batch.Put("ns1", "key7", []byte(jsonValue7), version.NewHeight(1, 7))
	jsonValue8 := "{\"asset_name\": \"marble8\",\"color\": \"blue\",\"size\": 8,\"owner\": \"elaine\"}"
	batch.Put("ns1", "key8", []byte(jsonValue8), version.NewHeight(1, 8))
	jsonValue9 := "{\"asset_name\": \"marble9\",\"color\": \"green\",\"size\": 9,\"owner\": \"fred\"}"
	batch.Put("ns1", "key9", []byte(jsonValue9), version.NewHeight(1, 9))
	jsonValue10 := "{\"asset_name\": \"marble10\",\"color\": \"green\",\"size\": 10,\"owner\": \"mary\"}"
	batch.Put("ns1", "key10", []byte(jsonValue10), version.NewHeight(1, 10))
	jsonValue11 := "{\"asset_name\": \"marble11\",\"color\": \"cyan\",\"size\": 1000007,\"owner\": \"joe\"}"
	batch.Put("ns1", "key11", []byte(jsonValue11), version.NewHeight(1, 11))

	//add keys for a separate namespace
	batch.Put("ns2", "key1", []byte(jsonValue1), version.NewHeight(1, 12))
	batch.Put("ns2", "key2", []byte(jsonValue2), version.NewHeight(1, 13))
	batch.Put("ns2", "key3", []byte(jsonValue3), version.NewHeight(1, 14))
	batch.Put("ns2", "key4", []byte(jsonValue4), version.NewHeight(1, 15))
	batch.Put("ns2", "key5", []byte(jsonValue5), version.NewHeight(1, 16))
	batch.Put("ns2", "key6", []byte(jsonValue6), version.NewHeight(1, 17))
	batch.Put("ns2", "key7", []byte(jsonValue7), version.NewHeight(1, 18))
	batch.Put("ns2", "key8", []byte(jsonValue8), version.NewHeight(1, 19))
	batch.Put("ns2", "key9", []byte(jsonValue9), version.NewHeight(1, 20))
	batch.Put("ns2", "key10", []byte(jsonValue10), version.NewHeight(1, 21))

	savePoint := version.NewHeight(2, 22)
	db.ApplyUpdates(batch, savePoint)

	//Create a buffer for the tar file
	buffer := new(bytes.Buffer)
	tarWriter := tar.NewWriter(buffer)

	//Add 2 index definitions
	var files = []struct {
		Name, Body string
	}{
		{"META-INF/statedb/couchdb/indexes/indexColorSortName.json", "{\"index\":{\"fields\":[{\"data.color\":\"desc\"}]},\"ddoc\":\"indexColorSortName\",\"name\":\"indexColorSortName\",\"type\":\"json\"}"},
		{"META-INF/statedb/couchdb/indexes/indexSizeSortName.json", "{\"index\":{\"fields\":[{\"data.size\":\"desc\"}]},\"ddoc\":\"indexSizeSortName\",\"name\":\"indexSizeSortName\",\"type\":\"json\"}"},
	}
	for _, file := range files {
		tarHeader := &tar.Header{
			Name: file.Name,
			Mode: 0600,
			Size: int64(len(file.Body)),
		}
		err := tarWriter.WriteHeader(tarHeader)
		testutil.AssertNoError(t, err, "")

		_, err = tarWriter.Write([]byte(file.Body))
		testutil.AssertNoError(t, err, "")

	}
	// Make sure to check the error on Close.
	if err := tarWriter.Close(); err != nil {
		log.Fatalln(err)
	}

	//Create a query
	queryString := `{"selector":{"owner":"fred"}}`

	_, err = db.ExecuteQuery("ns1", queryString)
	testutil.AssertNoError(t, err, "")

	//Create a query with a sort
	queryString = `{"selector":{"owner":"fred"}, "sort": [{"size": "desc"}]}`

	_, err = db.ExecuteQuery("ns1", queryString)
	testutil.AssertError(t, err, "Error should have been thrown for a missing index")

	if handleDefinition, ok := db.(cceventmgmt.ChaincodeLifecycleEventListener); ok {

		chaincodeDef := &cceventmgmt.ChaincodeDefinition{Name: "ns1", Hash: nil, Version: ""}

		//Test HandleChaincodeDefinition with a valid tar file
		err := handleDefinition.HandleChaincodeDeploy(chaincodeDef, buffer.Bytes())
		testutil.AssertNoError(t, err, "")

		//Test HandleChaincodeDefinition with a nil tar file
		err = handleDefinition.HandleChaincodeDeploy(chaincodeDef, nil)
		testutil.AssertNoError(t, err, "")

		//Test HandleChaincodeDefinition with a bad tar file
		err = handleDefinition.HandleChaincodeDeploy(chaincodeDef, []byte(`This is a really bad tar file`))
		testutil.AssertError(t, err, "Error should have been thrown for a bad tar file")

		//Test HandleChaincodeDefinition with a nil chaincodeDef
		err = handleDefinition.HandleChaincodeDeploy(nil, []byte(`This is a really bad tar file`))
		testutil.AssertError(t, err, "Error should have been thrown for a nil chaincodeDefinition")

		//Sleep to allow time for index creation
		time.Sleep(100 * time.Millisecond)
		//Create a query with a sort
		queryString = `{"selector":{"owner":"fred"}, "sort": [{"size": "desc"}]}`

		//Query should complete without error
		_, err = db.ExecuteQuery("ns1", queryString)
		testutil.AssertNoError(t, err, "")

		//Query namespace "ns2", index is only created in "ns1".  This should return an error.
		_, err = db.ExecuteQuery("ns2", queryString)
		testutil.AssertError(t, err, "Error should have been thrown for a missing index")

	}
}
