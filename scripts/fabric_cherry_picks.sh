#!/bin/bash
#
# Copyright SecureKey Technologies Inc. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
set -e


mkdir -p $GOPATH/src/github.com/hyperledger/
cd $GOPATH/src/github.com/hyperledger/
git clone https://gerrit.hyperledger.org/r/fabric
cd fabric
git config advice.detachedHead false
git checkout d7656e2bff0a368cda09d9e2b190ed8702938219

#cd $GOPATH/src/github.com/hyperledger/fabric

#git config user.name "jenkins"
#git config user.email jenkins@jenkins.com
