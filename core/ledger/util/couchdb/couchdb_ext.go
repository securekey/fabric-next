/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package couchdb

// CreateNewIndexWithRetry added to match Fabric 2.0 but not used in 1.4.1
func (dbclient *CouchDatabase) CreateNewIndexWithRetry(indexdefinition string, designDoc string) error {
	return nil
}
