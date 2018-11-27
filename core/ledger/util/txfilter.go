/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package util

// TxFilter determines which transactions are to be validated
type TxFilter func(txIdx int) bool

// TxFilterAcceptAll accepts all transactions
var TxFilterAcceptAll = func(int) bool {
	return true
}
