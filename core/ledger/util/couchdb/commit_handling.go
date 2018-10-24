/*
Copyright IBM Corp, SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package couchdb

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	revField      = "_rev"
	versionField  = "~version"
)

//batchableDocument defines a document for a batch
type batchableDocument struct {
	CouchDoc *CouchDoc
	Deleted  bool
}

type docMetadataWithDelete struct {
	DocMetadata
	Deleted         bool `json:"_deleted"`
}

//CommitDocuments - commits documents into CouchDB using bulk API followed by individual inserts on error.
func (dbclient *CouchDatabase) CommitDocuments(documents []*CouchDoc) (map[string]string, error) {
	var docMap map[string]*batchableDocument
	var revMap map[string]string

	// TODO: make this more efficient
	for _, doc := range documents {
		var docMetadata docMetadataWithDelete

		//unmarshal the JSON component of the CouchDoc into the document
		err := json.Unmarshal(doc.JSONValue, &docMetadata)
		if err != nil {
			return nil, err
		}

		docMap[docMetadata.ID] = &batchableDocument{
			CouchDoc: doc,
			Deleted: docMetadata.Deleted,
		}
	}

	// Do the bulk update into couchdb. Note that this will do retries if the entire bulk update fails or times out
	batchUpdateResp, err := dbclient.BatchUpdateDocuments(documents)
	if err != nil {
		return nil, err
	}
	// IF INDIVIDUAL DOCUMENTS IN THE BULK UPDATE DID NOT SUCCEED, TRY THEM INDIVIDUALLY
	// iterate through the response from CouchDB by document
	for _, respDoc := range batchUpdateResp {
		// If the document returned an error, retry the individual document
		if respDoc.Ok != true {
			batchUpdateDocument := docMap[respDoc.ID]
			var err error
			var rev string
			//Remove the "_rev" from the JSON before saving
			//this will allow the CouchDB retry logic to retry revisions without encountering
			//a mismatch between the "If-Match" and the "_rev" tag in the JSON
			if batchUpdateDocument.CouchDoc.JSONValue != nil {
				err = removeJSONRevision(&batchUpdateDocument.CouchDoc.JSONValue)
				if err != nil {
					return nil, err
				}
			}
			// Check to see if the document was added to the batch as a delete type document
			if batchUpdateDocument.Deleted {
				logger.Warningf("CouchDB batch document delete encountered an problem. Retrying delete for document ID:%s", respDoc.ID)
				// If this is a deleted document, then retry the delete
				// If the delete fails due to a document not being found (404 error),
				// the document has already been deleted and the DeleteDoc will not return an error
				err = dbclient.DeleteDoc(respDoc.ID, "")
			} else {
				logger.Warningf("CouchDB batch document update encountered an problem. Retrying update for document ID:%s", respDoc.ID)
				// Save the individual document to couchdb
				// Note that this will do retries as needed
				rev, err = dbclient.SaveDoc(respDoc.ID, "", batchUpdateDocument.CouchDoc)
			}

			// If the single document update or delete returns an error, then throw the error
			if err != nil {
				errorString := fmt.Sprintf("Error occurred while saving document ID = %v  Error: %s  Reason: %s\n",
					respDoc.ID, respDoc.Error, respDoc.Reason)

				logger.Errorf(errorString)
				return nil, fmt.Errorf(errorString)
			}
			revMap[respDoc.ID] = rev
		} else {
			revMap[respDoc.ID] = respDoc.Rev
		}
	}
	return revMap, nil
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

type jsonValue map[string]interface{}

func castToJSON(b []byte) (jsonValue, error) {
	var jsonVal map[string]interface{}
	err := json.Unmarshal(b, &jsonVal)
	return jsonVal, err
}

func (v jsonValue) checkReservedFieldsNotPresent() error {
	for fieldName := range v {
		if fieldName == versionField || strings.HasPrefix(fieldName, "_") {
			return fmt.Errorf("The field [%s] is not valid for the CouchDB state database", fieldName)
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