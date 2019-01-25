/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/
package couchdb

import (
	"strings"
	"time"

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
func (dbclient *CouchDatabase) CreateNewIndexWithRetry(indexdefinition string, designDoc string) error {
	// TODO: Make configurable
	const maxAttempts = 10

	_, err := retry.Invoke(
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
	return err
}

// Exists determines if the database exists
func (dbclient *CouchDatabase) Exists() (bool, error) {
	_, couchDBReturn, err := dbclient.GetDatabaseInfo()
	if err != nil {
		if couchDBReturn == nil || couchDBReturn.StatusCode != 404 {
			return false, err
		}
	}
	return true, nil
}

var errDBNotFound = errors.Errorf("DB not found")

func isPvtDataDB(dbName string) bool {
	return strings.Contains(dbName, "$$h") || strings.Contains(dbName, "$$p")
}

func (dbclient *CouchDatabase) shouldRetry(err error) bool {
	return err == errDBNotFound && !isPvtDataDB(dbclient.DBName)
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
				return nil, errDBNotFound
			}

			// DB exists
			return nil, nil
		},
		retry.WithMaxAttempts(maxAttempts),
		retry.WithBeforeRetry(func(err error, attempt int, backoff time.Duration) bool {
			if dbclient.shouldRetry(err) {
				logger.Debugf("Got error [%s] checking if DB [%s] exists on attempt #%d. Will retry in %s.", err, dbclient.DBName, attempt, backoff)
				return true
			}
			logger.Debugf("Got error [%s] checking if DB [%s] exists on attempt #%d. Will NOT retry", err, dbclient.DBName, attempt)
			return false
		}),
	)

	if err != nil {
		if err == errDBNotFound {
			return false, nil
		}
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
