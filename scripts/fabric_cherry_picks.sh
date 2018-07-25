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

git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/19/24619/2 && git cherry-pick FETCH_HEAD
