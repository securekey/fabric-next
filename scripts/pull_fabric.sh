#!/bin/bash
#
# Copyright SecureKey Technologies Inc. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
set -e




rm -rf $GOPATH/src/github.com/hyperledger/fabric
mkdir -p $GOPATH/src/github.com/hyperledger/
cd $GOPATH/src/github.com/hyperledger/
git clone https://gerrit.hyperledger.org/r/fabric
cd fabric
git checkout 4f7a7c8d696e866d06780e14b10704614a68564b

$GOPATH/src/github.com/hyperledger/fabric-next/scripts/fabric_cherry_picks.sh

make clean
DOCKER_DYNAMIC_LINK=true BASE_DOCKER_NS=d1vyank make docker
