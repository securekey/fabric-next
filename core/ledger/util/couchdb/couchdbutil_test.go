/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package couchdb

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/hyperledger/fabric/common/ledger/testutil"
	"github.com/hyperledger/fabric/common/util"
)

//Unit test of couch db util functionality
func TestCreateCouchDBConnectionAndDB(t *testing.T) {

	database := "testcreatecouchdbconnectionanddb"
	cleanup(database)
	defer cleanup(database)
	//create a new connection
	couchInstance, err := CreateCouchInstance(couchDBDef.URL, couchDBDef.Username, couchDBDef.Password,
		couchDBDef.MaxRetries, couchDBDef.MaxRetriesOnStartup, couchDBDef.RequestTimeout)
	testutil.AssertNoError(t, err, fmt.Sprintf("Error when trying to CreateCouchInstance"))

	_, err = CreateCouchDatabase(couchInstance, database)
	testutil.AssertNoError(t, err, fmt.Sprintf("Error when trying to CreateCouchDatabase"))

}

//Unit test of couch db util functionality
func TestCreateCouchDBSystemDBs(t *testing.T) {

	database := "testcreatecouchdbsystemdb"
	cleanup(database)
	defer cleanup(database)

	//create a new connection
	couchInstance, err := CreateCouchInstance(couchDBDef.URL, couchDBDef.Username, couchDBDef.Password,
		couchDBDef.MaxRetries, couchDBDef.MaxRetriesOnStartup, couchDBDef.RequestTimeout)

	testutil.AssertNoError(t, err, fmt.Sprintf("Error when trying to CreateCouchInstance"))

	err = CreateSystemDatabasesIfNotExist(couchInstance)
	testutil.AssertNoError(t, err, fmt.Sprintf("Error when trying to create system databases"))

	db := CouchDatabase{CouchInstance: couchInstance, DBName: "_users"}

	//Retrieve the info for the new database and make sure the name matches
	dbResp, errdb := db.GetDatabaseInfo()
	testutil.AssertNoError(t, errdb, fmt.Sprintf("Error when trying to retrieve _users database information"))
	testutil.AssertEquals(t, dbResp.DbName, "_users")

	db = CouchDatabase{CouchInstance: couchInstance, DBName: "_replicator"}

	//Retrieve the info for the new database and make sure the name matches
	dbResp, errdb = db.GetDatabaseInfo()
	testutil.AssertNoError(t, errdb, fmt.Sprintf("Error when trying to retrieve _replicator database information"))
	testutil.AssertEquals(t, dbResp.DbName, "_replicator")

	db = CouchDatabase{CouchInstance: couchInstance, DBName: "_global_changes"}

	//Retrieve the info for the new database and make sure the name matches
	dbResp, errdb = db.GetDatabaseInfo()
	testutil.AssertNoError(t, errdb, fmt.Sprintf("Error when trying to retrieve _global_changes database information"))
	testutil.AssertEquals(t, dbResp.DbName, "_global_changes")

}
func TestDatabaseMapping(t *testing.T) {
	//create a new instance and database object using a database name mixed case
	_, err := mapAndValidateDatabaseName("testDB")
	testutil.AssertError(t, err, "Error expected because the name contains capital letters")

	//create a new instance and database object using a database name with special characters
	_, err = mapAndValidateDatabaseName("test1234/1")
	testutil.AssertError(t, err, "Error expected because the name contains illegal chars")

	//create a new instance and database object using a database name with special characters
	_, err = mapAndValidateDatabaseName("5test1234")
	testutil.AssertError(t, err, "Error expected because the name starts with a number")

	//create a new instance and database object using an empty string
	_, err = mapAndValidateDatabaseName("")
	testutil.AssertError(t, err, fmt.Sprintf("Error should have been thrown for an invalid name"))

	_, err = mapAndValidateDatabaseName("a12345678901234567890123456789012345678901234" +
		"56789012345678901234567890123456789012345678901234567890123456789012345678901234567890" +
		"12345678901234567890123456789012345678901234567890123456789012345678901234567890123456" +
		"78901234567890123456789012345678901234567890")
	testutil.AssertError(t, err, fmt.Sprintf("Error should have been thrown for an invalid name"))

	transformedName, err := mapAndValidateDatabaseName("test.my.db-1")
	testutil.AssertNoError(t, err, "")
	testutil.AssertEquals(t, transformedName, "test$my$db-1")
}

func TestConstructMetadataDBName(t *testing.T) {
	// Allowed pattern for chainName: [a-z][a-z0-9.-]
	chainName := "tob2g.y-z0f.qwp-rq5g4-ogid5g6oucyryg9sc16mz0t4vuake5q557esz7sn493nf0ghch0xih6dwuirokyoi4jvs67gh6r5v6mhz3-292un2-9egdcs88cstg3f7xa9m1i8v4gj0t3jedsm-woh3kgiqehwej6h93hdy5tr4v.1qmmqjzz0ox62k.507sh3fkw3-mfqh.ukfvxlm5szfbwtpfkd1r4j.cy8oft5obvwqpzjxb27xuw6"

	truncatedChainName := "tob2g.y-z0f.qwp-rq5g4-ogid5g6oucyryg9sc16mz0t4vuak"
	testutil.AssertEquals(t, len(truncatedChainName), chainNameAllowedLength)

	// <first 50 chars (i.e., chainNameAllowedLength) of chainName> + 1 char for '(' + <64 chars for SHA256 hash
	// (hex encoding) of untruncated chainName> + 1 char for ')' + 1 char for '_' = 117 chars
	hash := hex.EncodeToString(util.ComputeSHA256([]byte(chainName)))
	expectedDBName := truncatedChainName + "(" + hash + ")" + "_"
	expectedDBNameLength := 117

	constructedDBName := ConstructMetadataDBName(chainName)
	testutil.AssertEquals(t, len(constructedDBName), expectedDBNameLength)
	testutil.AssertEquals(t, constructedDBName, expectedDBName)
}

func TestBlockchainDBNames(t *testing.T) {
	constructedDBName := ConstructBlockchainDBName("traders", "")
	expectedDBName := "traders_"
	testutil.AssertEquals(t, constructedDBName, expectedDBName)

	constructedDBName = ConstructBlockchainDBName("traders", "blocks")
	expectedDBName = "traders$$blocks_"
	testutil.AssertEquals(t, constructedDBName, expectedDBName)
}

func TestTruncatedBlockchainDBNames(t *testing.T) {
	testTruncatedBlockchainDBName(t, "")
	testTruncatedBlockchainDBName(t, "blocks")
}

func testTruncatedBlockchainDBName(t *testing.T, dbName string) {
	// Allowed pattern for chainName: [a-z][a-z0-9.-]
	chainName := "tob2g.y-z0f.qwp-rq5g4-ogid5g6oucyryg9sc16mz0t4vuake5q557esz7sn493nf0ghch0xih6dwuirokyoi4jvs67gh6r5v6mhz3-292un2-9egdcs88cstg3f7xa9m1i8v4gj0t3jedsm-woh3kgiqehwej6h93hdy5tr4v.1qmmqjzz0ox62k.507sh3fkw3-mfqh.ukfvxlm5szfbwtpfkd1r4j.cy8oft5obvwqpzjxb27xuw6"

	truncatedChainName := "tob2g.y-z0f.qwp-rq5g4-ogid5g6oucyryg9sc16mz0t4vuak"
	testutil.AssertEquals(t, len(truncatedChainName), chainNameAllowedLength)

	// <first 50 chars (i.e., chainNameAllowedLength) of chainName> + 1 char for '(' + <64 chars for SHA256 hash
	// (hex encoding) of untruncated chainName> + 1 char for ')' + 1 char for '_' = 117 chars
	// plus 2 for $$ seperator + length of the dbName
	hash := hex.EncodeToString(util.ComputeSHA256([]byte(chainName + "$$" + dbName)))
	expectedDBName := truncatedChainName + "$$" + dbName + "(" + hash + ")" + "_"
	expectedDBNameLength := 119 + len(dbName)

	if len(dbName) == 0 {
		hash = hex.EncodeToString(util.ComputeSHA256([]byte(chainName)))
		expectedDBName = truncatedChainName + "(" + hash + ")" + "_"
		expectedDBNameLength = 117
	}

	constructedDBName := ConstructBlockchainDBName(chainName, dbName)
	testutil.AssertEquals(t, len(constructedDBName), expectedDBNameLength)
	testutil.AssertEquals(t, constructedDBName, expectedDBName)
}


func TestConstructBlockchainDBName(t *testing.T) {
	// Allowed pattern for chainName: [a-z][a-z0-9.-]
	chainName := "tob2g.y-z0f.qwp-rq5g4-ogid5g6oucyryg9sc16mz0t4vuake5q557esz7sn493nf0ghch0xih6dwuirokyoi4jvs67gh6r5v6mhz3-292un2-9egdcs88cstg3f7xa9m1i8v4gj0t3jedsm-woh3kgiqehwej6h93hdy5tr4v.1qmmqjzz0ox62k.507sh3fkw3-mfqh.ukfvxlm5szfbwtpfkd1r4j.cy8oft5obvwqpzjxb27xuw6"

	truncatedChainName := "tob2g.y-z0f.qwp-rq5g4-ogid5g6oucyryg9sc16mz0t4vuak"
	testutil.AssertEquals(t, len(truncatedChainName), chainNameAllowedLength)

	// <first 50 chars (i.e., chainNameAllowedLength) of chainName> + 1 char for '(' + <64 chars for SHA256 hash
	// (hex encoding) of untruncated chainName> + 1 char for ')' + 1 char for '_' = 117 chars
	hash := hex.EncodeToString(util.ComputeSHA256([]byte(chainName)))
	expectedDBName := truncatedChainName + "(" + hash + ")" + "_"
	expectedDBNameLength := 117

	constructedDBName := ConstructMetadataDBName(chainName)
	testutil.AssertEquals(t, len(constructedDBName), expectedDBNameLength)
	testutil.AssertEquals(t, constructedDBName, expectedDBName)

}

func TestConstructedNamespaceDBName(t *testing.T) {
	// === SCENARIO 1: chainName_ns$$coll ===

	// Allowed pattern for chainName: [a-z][a-z0-9.-]
	chainName := "tob2g.y-z0f.qwp-rq5g4-ogid5g6oucyryg9sc16mz0t4vuake5q557esz7sn493nf0ghch0xih6dwuirokyoi4jvs67gh6r5v6mhz3-292un2-9egdcs88cstg3f7xa9m1i8v4gj0t3jedsm-woh3kgiqehwej6h93hdy5tr4v.1qmmqjzz0ox62k.507sh3fkw3-mfqh.ukfvxlm5szfbwtpfkd1r4j.cy8oft5obvwqpzjxb27xuw6"

	// Allowed pattern for namespace and collection: [a-zA-Z0-9_-]
	ns := "wMCnSXiV9YoIqNQyNvFVTdM8XnUtvrOFFIWsKelmP5NEszmNLl8YhtOKbFu3P_NgwgsYF8PsfwjYCD8f1XRpANQLoErDHwLlweryqXeJ6vzT2x0pS_GwSx0m6tBI0zOmHQOq_2De8A87x6zUOPwufC2T6dkidFxiuq8Sey2-5vUo_iNKCij3WTeCnKx78PUIg_U1gp4_0KTvYVtRBRvH0kz5usizBxPaiFu3TPhB9XLviScvdUVSbSYJ0Z"
	// first letter 'p' denotes private data namespace. We can use 'h' to denote hashed data namespace as defined in
	// privacyenabledstate/common_storage_db.go
	coll := "pvWjtfSTXVK8WJus5s6zWoMIciXd7qHRZIusF9SkOS6m8XuHCiJDE9cCRuVerq22Na8qBL2ywDGFpVMIuzfyEXLjeJb0mMuH4cwewT6r1INOTOSYwrikwOLlT_fl0V1L7IQEwUBB8WCvRqSdj6j5-E5aGul_pv_0UeCdwWiyA_GrZmP7ocLzfj2vP8btigrajqdH-irLO2ydEjQUAvf8fiuxru9la402KmKRy457GgI98UHoUdqV3f3FCdR"

	truncatedChainName := "tob2g.y-z0f.qwp-rq5g4-ogid5g6oucyryg9sc16mz0t4vuak"
	truncatedEscapedNs := "w$m$cn$s$xi$v9$yo$iq$n$qy$nv$f$v$td$m8$xn$utvr$o$f"
	truncatedEscapedColl := "pv$wjtf$s$t$x$v$k8$w$jus5s6z$wo$m$ici$xd7q$h$r$z$i"
	testutil.AssertEquals(t, len(truncatedChainName), chainNameAllowedLength)
	testutil.AssertEquals(t, len(truncatedEscapedNs), namespaceNameAllowedLength)
	testutil.AssertEquals(t, len(truncatedEscapedColl), collectionNameAllowedLength)

	untruncatedDBName := chainName + "_" + ns + "$$" + coll
	hash := hex.EncodeToString(util.ComputeSHA256([]byte(untruncatedDBName)))
	expectedDBName := truncatedChainName + "_" + truncatedEscapedNs + "$$" + truncatedEscapedColl + "(" + hash + ")"
	// <first 50 chars (i.e., chainNameAllowedLength) of chainName> + 1 char for '_' + <first 50 chars
	// (i.e., namespaceNameAllowedLength) of escaped namespace> + 2 chars for '$$' + <first 50 chars
	// (i.e., collectionNameAllowedLength) of escaped collection> + 1 char for '(' + <64 chars for SHA256 hash
	// (hex encoding) of untruncated chainName_ns$$coll> + 1 char for ')' = 219 chars
	expectedDBNameLength := 219

	namespace := ns + "$$" + coll
	constructedDBName := ConstructNamespaceDBName(chainName, namespace)
	testutil.AssertEquals(t, len(constructedDBName), expectedDBNameLength)
	testutil.AssertEquals(t, constructedDBName, expectedDBName)

	// === SCENARIO 2: chainName_ns ===

	untruncatedDBName = chainName + "_" + ns
	hash = hex.EncodeToString(util.ComputeSHA256([]byte(untruncatedDBName)))
	expectedDBName = truncatedChainName + "_" + truncatedEscapedNs + "(" + hash + ")"
	// <first 50 chars (i.e., chainNameAllowedLength) of chainName> + 1 char for '_' + <first 50 chars
	// (i.e., namespaceNameAllowedLength) of escaped namespace> + 1 char for '(' + <64 chars for SHA256 hash
	// (hex encoding) of untruncated chainName_ns> + 1 char for ')' = 167 chars
	expectedDBNameLength = 167

	namespace = ns
	constructedDBName = ConstructNamespaceDBName(chainName, namespace)
	testutil.AssertEquals(t, len(constructedDBName), expectedDBNameLength)
	testutil.AssertEquals(t, constructedDBName, expectedDBName)
}
