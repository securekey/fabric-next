/*
Copyright IBM Corp. 2016 All Rights Reserved.

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
/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package ldbblkindex

import "path/filepath"

const (
	// ChainsDir is the name of the directory containing the channel ledgers.
	ChainsDir = "chains"
	// IndexDir is the name of the directory containing all transaction indexes across ledgers.
	IndexDir  = "txindex"
)

// Conf encapsulates all the configurations for `FsBlockStore`
type Conf struct {
	blockStorageDir  string
}

// NewConf constructs new `Conf`.
// blockStorageDir is the top level folder under which `FsBlockStore` manages its data
func NewConf(blockStorageDir string) *Conf {
	return &Conf{blockStorageDir}
}

func (conf *Conf) getIndexDir() string {
	return filepath.Join(conf.blockStorageDir, IndexDir)
}

func (conf *Conf) getChainsDir() string {
	return filepath.Join(conf.blockStorageDir, ChainsDir)
}

func (conf *Conf) getLedgerBlockDir(ledgerid string) string {
	return filepath.Join(conf.getChainsDir(), ledgerid)
}