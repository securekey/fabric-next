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
git checkout v1.2.0

cd $GOPATH/src/github.com/hyperledger/fabric

git config user.name "jenkins"
git config user.email jenkins@jenkins.com

#apply patch for GREP11 (5151f212d3edd89fbabc12fbe702cecea0cb4b3a + local fixes)
git am $MY_PATH/../patches/0001-GREP11-Remote-EP11-BCCSP.patch

git am $MY_PATH/../patches/0001-FAB-11247-Add-configuration-to-create-_global_change.patch

git am $MY_PATH/../patches/0001-Ledger-metrics.patch

git am $MY_PATH/../patches/0001-FAB-8622-Red.-exclusive-lock-dur.-during-commit.patch



git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/17/24217/1 && git cherry-pick FETCH_HEAD
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/57/25357/1 && git cherry-pick FETCH_HEAD
#git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/77/25477/1 && git cherry-pick FETCH_HEAD

# [FAB-11648] Release closeMarkerLock earlier
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/59/25659/2 && git cherry-pick FETCH_HEAD