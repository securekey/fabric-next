/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package statecouchdb

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
)

const (
	binaryWrapper = "valueBytes"
	idField       = "_id"
	revField      = "_rev"
	versionField  = "~version"
	deletedField  = "_deleted"
)

var reservedFields = [4]string{idField, revField, versionField, deletedField}

type keyValue struct {
	key string
	*statedb.VersionedValue
}

type jsonValue map[string]interface{}

func tryCastingToJSON(b []byte) (isJSON bool, val jsonValue) {
	var jsonVal map[string]interface{}
	err := json.Unmarshal(b, &jsonVal)
	return err == nil, jsonValue(jsonVal)
}

func castToJSON(b []byte) (jsonValue, error) {
	var jsonVal map[string]interface{}
	err := json.Unmarshal(b, &jsonVal)
	return jsonVal, err
}

func (v jsonValue) checkReservedFieldsNotPresent() error {
	for _, fieldName := range reservedFields {
		if _, fieldFound := v[fieldName]; fieldFound {
			return fmt.Errorf("The field %s is present", fieldName)
		}
	}
	return nil
}

func (v jsonValue) removeRevField() {
	delete(v, revField)
}

func (v jsonValue) toBytes() ([]byte, error) {
	return json.Marshal(v)
}

func couchDocToKeyValue(doc *couchdb.CouchDoc) (*keyValue, error) {
	// initialize the return value
	var returnValue []byte
	var err error
	// create a generic map unmarshal the json
	jsonResult := make(map[string]interface{})
	decoder := json.NewDecoder(bytes.NewBuffer(doc.JSONValue))
	decoder.UseNumber()
	if err = decoder.Decode(&jsonResult); err != nil {
		return nil, err
	}
	// verify the version field exists
	if _, fieldFound := jsonResult[versionField]; !fieldFound {
		return nil, fmt.Errorf("The version field %s was not found", versionField)
	}
	key := jsonResult[idField].(string)
	// create the return version from the version field in the JSON
	returnVersion := createVersionHeightFromVersionString(jsonResult[versionField].(string))
	// remove the _id, _rev and version fields
	delete(jsonResult, idField)
	delete(jsonResult, revField)
	delete(jsonResult, versionField)

	// handle binary or json data
	if doc.Attachments != nil { // binary attachment
		// get binary data from attachment
		for _, attachment := range doc.Attachments {
			if attachment.Name == binaryWrapper {
				returnValue = attachment.AttachmentBytes
			}
		}
	} else {
		// marshal the returned JSON data.
		if returnValue, err = json.Marshal(jsonResult); err != nil {
			return nil, err
		}
	}
	return &keyValue{key, &statedb.VersionedValue{Value: returnValue, Version: returnVersion}}, nil
}

func keyValToCouchDoc(kv *keyValue, revision string) (*couchdb.CouchDoc, error) {
	type kvType int32
	const (
		kvTypeDelete = iota
		kvTypeJSON
		kvTypeAttachment
	)
	key, value, version := kv.key, kv.VersionedValue.Value, kv.VersionedValue.Version
	jsonMap := make(jsonValue)

	var kvtype kvType
	switch {
	case value == nil:
		kvtype = kvTypeDelete
	case json.Unmarshal(value, &jsonMap) == nil:
		kvtype = kvTypeJSON
		if err := jsonMap.checkReservedFieldsNotPresent(); err != nil {
			return nil, err
		}
	default:
		kvtype = kvTypeAttachment
	}

	// add the version, id, revision, and delete marker (if needed)
	jsonMap[versionField] = fmt.Sprintf("%v:%v", version.BlockNum, version.TxNum)
	jsonMap[idField] = key
	if revision != "" {
		jsonMap[revField] = revision
	}
	if kvtype == kvTypeDelete {
		jsonMap[deletedField] = true
	}
	jsonBytes, err := jsonMap.toBytes()
	if err != nil {
		return nil, err
	}
	couchDoc := &couchdb.CouchDoc{JSONValue: jsonBytes}
	if kvtype == kvTypeAttachment {
		attachment := &couchdb.AttachmentInfo{}
		attachment.AttachmentBytes = value
		attachment.ContentType = "application/octet-stream"
		attachment.Name = binaryWrapper
		attachments := append([]*couchdb.AttachmentInfo{}, attachment)
		couchDoc.Attachments = attachments
	}
	return couchDoc, nil
}

// couchSavepointData data for couchdb
type couchSavepointData struct {
	BlockNum uint64 `json:"BlockNum"`
	TxNum    uint64 `json:"TxNum"`
}

func encodeSavepoint(height *version.Height) (*couchdb.CouchDoc, error) {
	var err error
	var savepointDoc couchSavepointData
	// construct savepoint document
	savepointDoc.BlockNum = height.BlockNum
	savepointDoc.TxNum = height.TxNum
	savepointDocJSON, err := json.Marshal(savepointDoc)
	if err != nil {
		logger.Errorf("Failed to create savepoint data %s\n", err.Error())
		return nil, err
	}
	return &couchdb.CouchDoc{JSONValue: savepointDocJSON, Attachments: nil}, nil
}

func decodeSavepoint(couchDoc *couchdb.CouchDoc) (*version.Height, error) {
	savepointDoc := &couchSavepointData{}
	if err := json.Unmarshal(couchDoc.JSONValue, &savepointDoc); err != nil {
		logger.Errorf("Failed to unmarshal savepoint data %s\n", err.Error())
		return nil, err
	}
	return &version.Height{BlockNum: savepointDoc.BlockNum, TxNum: savepointDoc.TxNum}, nil
}

func createVersionHeightFromVersionString(encodedVersion string) *version.Height {
	versionArray := strings.Split(fmt.Sprintf("%s", encodedVersion), ":")
	// convert the blockNum from String to unsigned int
	blockNum, _ := strconv.ParseUint(versionArray[0], 10, 64)
	// convert the txNum from String to unsigned int
	txNum, _ := strconv.ParseUint(versionArray[1], 10, 64)
	return version.NewHeight(blockNum, txNum)
}

func validateValue(value []byte) error {
	isJSON, jsonVal := tryCastingToJSON(value)
	if !isJSON {
		return nil
	}
	return jsonVal.checkReservedFieldsNotPresent()
}

// removeJSONRevision removes the "_rev" if this is a JSON
func removeJSONRevision(jsonValue *[]byte) error {
	jsonVal, err := castToJSON(*jsonValue)
	if err != nil {
		logger.Errorf("Failed to unmarshal couchdb JSON data %s\n", err.Error())
		return err
	}
	jsonVal.removeRevField()
	if *jsonValue, err = jsonVal.toBytes(); err != nil {
		logger.Errorf("Failed to marshal couchdb JSON data %s\n", err.Error())
	}
	return err
}
