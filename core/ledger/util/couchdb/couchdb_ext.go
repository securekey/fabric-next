/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/
package couchdb

import (
	"github.com/hyperledger/fabric/common/util/retry"
	"github.com/pkg/errors"
)

// CreateIndexWithRetry method provides a function creating an index but retries on failure
func (dbclient *CouchDatabase) CreateIndexWithRetry(indexdefinition string) (*CreateIndexResponse, error) {
	// TODO: Make configurable
	const maxAttempts = 10

	respUT, err := retry.Invoke(
		func() (interface{}, error) {
			return dbclient.CreateIndex(indexdefinition)
		},
		retry.WithMaxAttempts(maxAttempts),
	)

	if err != nil {
		return nil, err
	}

	resp := respUT.(*CreateIndexResponse)
	return resp, nil
}

// CreateNewIndexWithRetry method provides a function creating an index but retries on failure
func (dbclient *CouchDatabase) CreateNewIndexWithRetry(indexdefinition string, designDoc string) (*CreateIndexResponse, error) {
	// TODO: Make configurable
	const maxAttempts = 10

	respUT, err := retry.Invoke(
		func() (interface{}, error) {
			exists, err := dbclient.IndexDesignDocExists(designDoc)
			if err != nil {
				return nil, err
			}
			if exists {
				return nil, nil
			}

			return dbclient.CreateIndex(indexdefinition)
		},
		retry.WithMaxAttempts(maxAttempts),
	)

	if err != nil {
		return nil, err
	}

	resp := respUT.(*CreateIndexResponse)
	return resp, nil
}

// Exists determines if the database exists
func (dbclient *CouchDatabase) Exists() (bool, error) {
	dbInfo, couchDBReturn, err := dbclient.GetDatabaseInfo()
	if err != nil {
		return false, err
	}

	//If the dbInfo returns populated and status code is 200, then the database exists
	if dbInfo == nil || couchDBReturn.StatusCode != 200 {
		return false, errors.Errorf("DB not found: [%s]", dbclient.DBName)
	}

	return true, nil
}

// ExistsWithRetry determines if the database exists, but retries until it does
func (dbclient *CouchDatabase) ExistsWithRetry() (bool, error) {
	// TODO: Make configurable
	const maxAttempts = 10

	_, err := retry.Invoke(
		func() (interface{}, error) {
			dbExists, err := dbclient.Exists()
			if err != nil {
				return nil, err
			}
			if !dbExists {
				return nil, errors.Errorf("DB not found: [%s]", dbclient.DBName)
			}

			// DB exists
			return nil, nil
		},
		retry.WithMaxAttempts(maxAttempts),
	)

	if err != nil {
		return false, err
	}

	return true, nil
}

// IndexDesignDocExists determines if all the passed design documents exists in the database.
func (dbclient *CouchDatabase) IndexDesignDocExists(designDocs ...string) (bool, error) {
	designDocExists := make([]bool, len(designDocs))

	indices, err := dbclient.ListIndex()
	if err != nil {
		return false, errors.WithMessage(err, "retrieval of DB index list failed")
	}

	for _, dbIndexDef := range indices {
		for j, docName := range designDocs {
			if dbIndexDef.DesignDocument == docName {
				designDocExists[j] = true
			}
		}
	}

	for _, exists := range designDocExists {
		if !exists {
			return false, nil
		}
	}

	return true, nil
}

// IndexDesignDocExists determines if all the passed design documents exists in the database.
func (dbclient *CouchDatabase) IndexDesignDocExistsWithRetry(designDocs ...string) (bool, error) {
	// TODO: Make configurable
	const maxAttempts = 10

	_, err := retry.Invoke(
		func() (interface{}, error) {
			indexExists, err := dbclient.IndexDesignDocExists(designDocs...)
			if err != nil {
				return nil, err
			}
			if !indexExists {
				return nil, errors.Errorf("DB index not found: [%s]", dbclient.DBName)
			}

			// DB index exists
			return nil, nil
		},
		retry.WithMaxAttempts(maxAttempts),
	)

	if err != nil {
		return false, err
	}

	return true, nil
}
