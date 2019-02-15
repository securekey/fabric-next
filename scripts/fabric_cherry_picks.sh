#!/bin/bash
#
# Copyright SecureKey Technologies Inc. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
set -e

# NEEDED for GREP11 patch
MY_PATH="`dirname \"$0\"`"              # relative
MY_PATH="`( cd \"$MY_PATH\" && pwd )`"  # absolutized and normalized
if [ -z "$MY_PATH" ] ; then
  # error; for some reason, the path is not accessible
  # to the script (e.g. permissions re-evaled after suid)
  exit 1  # fail
fi


mkdir -p $GOPATH/src/github.com/hyperledger/
cd $GOPATH/src/github.com/hyperledger/
git clone https://gerrit.hyperledger.org/r/fabric
cd fabric
git config advice.detachedHead false
# Fabric release-1.4 (Jan 24, 2019)
git checkout 1fdb6917c71bc4bfbd2cddde9a6d285a2a8ee390

cd $GOPATH/src/github.com/hyperledger/fabric

git config user.name "jenkins"
git config user.email jenkins@jenkins.com


#apply patch for GREP11
git am $MY_PATH/../patches/0001-GREP11-Remote-EP11-BCCSP.patch
git am $MY_PATH/../patches/0001-Apply-go-modules.patch

#[FAB-12982] & [FAB-13534] cherry picks are required to [FAB-12914] gossip private data metrics

#[FAB-12982] spray pvt across maximum eligible orgs
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/15/27815/10 && git cherry-pick FETCH_HEAD
#[FAB-13534] cache pushAckTimeout in pvt data
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/26/28526/2 && git cherry-pick FETCH_HEAD

#cherry pick metrics

#[FAB-12916] gossip state metrics
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/92/28692/10 && git cherry-pick FETCH_HEAD

#[FAB-12915] gossip leader election metrics
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/79/28779/12 && git cherry-pick FETCH_HEAD

#[FAB-12917] gossip comm metrics
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/84/28784/17 && git cherry-pick FETCH_HEAD

#[FAB-12918] gossip channel membership metrics
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/51/28851/17 && git cherry-pick FETCH_HEAD

#[FAB-12914] gossip private data metrics
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/38/28938/7 && git cherry-pick FETCH_HEAD

