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
# Fabric v1.4.0 (Jan 09, 2019)
git checkout v1.4.0

cd $GOPATH/src/github.com/hyperledger/fabric

git config user.name "jenkins"
git config user.email jenkins@jenkins.com

# **Temporary** metrics collection
# TODO DEV-11471
#git am $MY_PATH/../patches/0001-Ledger-metrics.patch

#apply patch for GREP11 (5151f212d3edd89fbabc12fbe702cecea0cb4b3a + local fixes)
git am $MY_PATH/../patches/0001-GREP11-Remote-EP11-BCCSP.patch
#git am $MY_PATH/../patches/0001-GREP11-Remote-EP11-BCCSP-metrics.patch

#apply cherry pick
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/34/28534/1 && git cherry-pick FETCH_HEAD

#cherry pick metrics

#[FAB-12916] gossip state metrics
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/92/28692/7 && git cherry-pick FETCH_HEAD

#add endorser metrics
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/54/27854/12 && git cherry-pick FETCH_HEAD

#[FAB-12915] gossip leader election metrics
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/79/28779/1 && git cherry-pick FETCH_HEAD

