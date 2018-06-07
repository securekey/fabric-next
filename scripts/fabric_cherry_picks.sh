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
git checkout e57b717689986d2a974d74f0ec78a384c2d05c87

cd $GOPATH/src/github.com/hyperledger/fabric

git config user.name "jenkins"
git config user.email jenkins@jenkins.com


#https://gerrit.hyperledger.org/r/c/12169/ [DRAFT] Remote EP11 BCCSP
#git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/69/12169/7 && git cherry-pick FETCH_HEAD

# https://gerrit.hyperledger.org/r/c/22553/ - Open - Gossip deadlock issue
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/53/22553/1 && git cherry-pick FETCH_HEAD

##TODO cherry pick service discovery##
