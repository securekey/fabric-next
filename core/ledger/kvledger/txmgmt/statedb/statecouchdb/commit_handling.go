/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/
package statecouchdb

import (
	"fmt"

	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/statedb/statekeyindex"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
	"github.com/pkg/errors"
)

// nsCommittersBuilder implements `batch` interface. Each batch operates on a specific namespace in the updates and
// builds one or more batches of type subNsCommitter.
type nsCommittersBuilder struct {
	updates         map[string]*statedb.VersionedValue
	db              *couchdb.CouchDatabase
	revisions       map[string]string
	subNsCommitters []batch
	ns              string
	keyIndex        statekeyindex.StateKeyIndex
}

// subNsCommitter implements `batch` interface. Each batch commits the portion of updates within a namespace assigned to it
type subNsCommitter struct {
	// The namespace of the keys that this subNsCommitter will commit to the db.
	namespace      string
	db             *couchdb.CouchDatabase
	batchUpdateMap map[string]*batchableDocument
	// StateKeyIndex used to index metadata.
	statekeyindex statekeyindex.StateKeyIndex
	// The order to start committing once the statekeyindex is ready is received through this chan.
	startCommitting chan struct{}
}

// buildCommitters build the batches of type subNsCommitter. This functions processes different namespaces in parallel
func (vdb *VersionedDB) buildCommitters(updates *statedb.UpdateBatch, blockNum uint64) ([]batch, error) {
	keyIndex, err := statekeyindex.NewProvider().OpenStateKeyIndex(vdb.chainName)
	if err != nil {
		return nil, err
	}
	namespaces := updates.GetUpdatedNamespaces()
	var nsCommitterBuilder []batch
	for _, ns := range namespaces {
		nsUpdates := updates.GetUpdates(ns)
		db, err := vdb.getNamespaceDBHandle(ns)
		if err != nil {
			return nil, err
		}
		nsRevs := vdb.committedDataCache.revs[ns]
		if nsRevs == nil {
			nsRevs = make(nsRevisions)
		}
		vdb.GetWSetCacheLock().RLock()
		if committedWSetDataCache := vdb.committedWSetDataCache[blockNum]; committedWSetDataCache != nil {
			nsWSetRevs := committedWSetDataCache.revs[ns]
			for k, v := range nsWSetRevs {
				nsRevs[k] = v
			}
		}
		vdb.GetWSetCacheLock().RUnlock()
		// for each namespace, construct one builder with the corresponding couchdb handle and couch revisions
		// that are already loaded into cache (during validation phase)
		nsCommitterBuilder = append(nsCommitterBuilder, &nsCommittersBuilder{ns: ns, updates: nsUpdates, db: db, revisions: nsRevs, keyIndex: keyIndex})
	}
	vdb.GetWSetCacheLock().Lock()
	vdb.committedWSetDataCache[blockNum] = nil
	vdb.GetWSetCacheLock().Unlock()
	if err := executeBatches(nsCommitterBuilder); err != nil {
		return nil, err
	}
	// accumulate results across namespaces (one or more batches of `subNsCommitter` for a namespace from each builder)
	var combinedSubNsCommitters []batch
	var committers []*subNsCommitter
	for _, b := range nsCommitterBuilder {
		combinedSubNsCommitters = append(combinedSubNsCommitters, b.(*nsCommittersBuilder).subNsCommitters...)
		for _, committer := range b.(*nsCommittersBuilder).subNsCommitters {
			committers = append(committers, committer.(*subNsCommitter))
		}
	}
	go vdb.letThemKnowWhenStateKeyIndexIsReady(committers)
	return combinedSubNsCommitters, nil
}

// execute implements the function in `batch` interface. This function builds one or more `subNsCommitter`s that
// cover the updates for a namespace
func (builder *nsCommittersBuilder) execute() error {
	// TODO: Perform couchdb revision load in the background earlier.
	if err := addRevisionsForMissingKeys(builder.ns, builder.keyIndex, builder.revisions, builder.db, builder.updates); err != nil {
		return err
	}
	maxBacthSize := ledgerconfig.GetMaxBatchUpdateSize()
	batchUpdateMap := make(map[string]*batchableDocument)
	for key, vv := range builder.updates {
		couchDoc, err := keyValToCouchDoc(&keyValue{key: key, VersionedValue: vv}, builder.revisions[key])
		if err != nil {
			return err
		}
		// TODO: I removed the copy of the couch document here. (It isn't clear why a copy is needed).
		batchUpdateMap[key] = &batchableDocument{CouchDoc: couchDoc, Deleted: vv.Value == nil}
		if len(batchUpdateMap) == maxBacthSize {
			builder.subNsCommitters = append(builder.subNsCommitters, &subNsCommitter{builder.ns, builder.db, batchUpdateMap, builder.keyIndex, make(chan struct{})})
			batchUpdateMap = make(map[string]*batchableDocument)
		}
	}
	if len(batchUpdateMap) > 0 {
		builder.subNsCommitters = append(builder.subNsCommitters, &subNsCommitter{builder.ns, builder.db, batchUpdateMap, builder.keyIndex, make(chan struct{})})
	}
	return nil
}

// execute implements the function in `batch` interface. This function commits the updates managed by a `subNsCommitter`
// TODO: this should be refactored to use a common commit function in the CouchDB package.
func (committer *subNsCommitter) execute() error {
	select {
	case <-committer.startCommitting:
		logger.Debug("committer received order to begin committing...")
		//The statekeyindex's metadata for each key will later be updated with CouchDB's _rev.
		keyMetadata := make(map[string]*statekeyindex.Metadata)
		//Add the documents to the batch update array
		batchUpdateDocs := []*couchdb.CouchDoc{}
		for _, updateDocument := range committer.batchUpdateMap {
			batchUpdateDocument := updateDocument
			batchUpdateDocs = append(batchUpdateDocs, batchUpdateDocument.CouchDoc)
			keyval, err := couchDocToKeyValue(updateDocument.CouchDoc)
			if err != nil {
				return errors.Wrapf(err, "failed to convert couchDoc to keyValue: couchDoc=[%+v]", updateDocument.CouchDoc)
			}
			compKey := &statekeyindex.CompositeKey{Key: keyval.key, Namespace: committer.namespace}
			metadata, found, err := committer.statekeyindex.GetMetadata(compKey)
			if found {
				keyMetadata[keyval.key] = &metadata
			} else {
				logger.Warningf("metadata for key [%+v] was unexpectedly not found in the statekeyindex", compKey)
			}
		}
		// Do the bulk update into couchdb. Note that this will do retries if the entire bulk update fails or times out
		batchUpdateResp, err := committer.db.BatchUpdateDocuments(batchUpdateDocs)
		if err != nil {
			return err
		}
		for _, respDoc := range batchUpdateResp {
			if respDoc.Ok {
				if md, found := keyMetadata[respDoc.ID]; found {
					md.DBTag = respDoc.Rev
				} else {
					logger.Warningf("could not set CouchDB _rev for document with ID=%s because its metadata was not found", respDoc.ID)
				}
			} else {
				// If the document returned an error, retry the individual document
				// iterate through the response from CouchDB by document
				batchUpdateDocument := committer.batchUpdateMap[respDoc.ID]
				var err error
				//Remove the "_rev" from the JSON before saving
				//this will allow the CouchDB retry logic to retry revisions without encountering
				//a mismatch between the "If-Match" and the "_rev" tag in the JSON
				if batchUpdateDocument.CouchDoc.JSONValue != nil {
					err = removeJSONRevision(&batchUpdateDocument.CouchDoc.JSONValue)
					if err != nil {
						return err
					}
				}
				// Check to see if the document was added to the batch as a delete type document
				if batchUpdateDocument.Deleted {
					logger.Warningf("CouchDB batch document delete encountered an problem. Retrying delete for document ID:%s", respDoc.ID)
					// If this is a deleted document, then retry the delete
					// If the delete fails due to a document not being found (404 error),
					// the document has already been deleted and the DeleteDoc will not return an error
					err = committer.db.DeleteDoc(respDoc.ID, "")
				} else {
					logger.Warningf("CouchDB batch document update encountered an problem. Retrying update for document ID:%s", respDoc.ID)
					// Save the individual document to couchdb
					// Note that this will do retries as needed
					var rev string
					rev, err = committer.db.SaveDoc(respDoc.ID, "", batchUpdateDocument.CouchDoc)
					md, found := keyMetadata[respDoc.ID]
					if found {
						md.DBTag = rev
					}
				}
				// If the single document update or delete returns an error, then throw the error
				if err != nil {
					errorString := fmt.Sprintf("Error occurred while saving document ID = %v  Error: %s  Reason: %s\n",
						respDoc.ID, respDoc.Error, respDoc.Reason)
					logger.Errorf(errorString)
					return fmt.Errorf(errorString)
				}
			}
		}
		var indexUpdates []*statekeyindex.IndexUpdate
		for key, metadata := range keyMetadata {
			indexUpdates = append(
				indexUpdates,
				&statekeyindex.IndexUpdate{
					Key: statekeyindex.CompositeKey{
						Namespace: committer.namespace,
						Key:       key,
					},
					Value: *metadata,
				},
			)
		}
		if len(indexUpdates) > 0 {
			return committer.statekeyindex.AddIndex(indexUpdates)
		}
	}
	return nil
}
func (vdb *VersionedDB) warmupAllIndexes(dbs []*couchdb.CouchDatabase) {
	for _, db := range dbs {
		db.WarmUpAllIndexes()
	}
}
func addRevisionsForMissingKeys(ns string, keyIndex statekeyindex.StateKeyIndex, revisions map[string]string, db *couchdb.CouchDatabase, nsUpdates map[string]*statedb.VersionedValue) error {
	var missingKeys []string
	for key := range nsUpdates {
		_, ok := revisions[key]
		if !ok {
			logger.Debugf("key %s not found in revisions going to search in keyIndex", key)
			_, exists, err := keyIndex.GetMetadata(&statekeyindex.CompositeKey{Namespace: ns, Key: key})
			if err != nil {
				return err
			}
			if !exists {
				revisions[key] = ""
			} else {
				missingKeys = append(missingKeys, key)
			}
		}
	}
	if len(missingKeys) > 0 {
		logger.Warningf("Pulling revisions for the keys [%s] for namsespace [%s] that were not part of the readset", missingKeys, db.DBName)
	}
	retrievedMetadata, err := retrieveNsMetadata(db, missingKeys, false)
	if err != nil {
		return err
	}
	for _, metadata := range retrievedMetadata {
		revisions[metadata.ID] = metadata.Rev
	}
	return nil
}

//batchableDocument defines a document for a batch
type batchableDocument struct {
	CouchDoc *couchdb.CouchDoc
	Deleted  bool
}

// Forwards the notification that the statekeyindex is ready to the given committers ONCE.
func (vdb *VersionedDB) letThemKnowWhenStateKeyIndexIsReady(committers []*subNsCommitter) {
	select{
		case ready := <-vdb.stateKeyIndexReadyCh:
			logger.Debugf("received notification that statekeyindex is ready. Forwarding to %d committers.", len(committers))
			for _, committer := range committers {
				committer.startCommitting <- ready
			}
	}
}
