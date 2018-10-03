/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package cdbtransientdata

import (
	"github.com/hyperledger/fabric/core/ledger/util/couchdb"
)

type store struct {
	db *couchdb.CouchDatabase
}

func newStore(db *couchdb.CouchDatabase) (*store, error) {
	s := store{
		db: db,
	}
	return &s, nil
}
