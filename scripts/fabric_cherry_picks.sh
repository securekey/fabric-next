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
# Fabric release-1.4.1 (Apr 11, 2019) - https://github.com/hyperledger/fabric/releases/tag/v1.4.1
git checkout 87074a73f3f6fcdff7e82707c9b476596d030d04

cd $GOPATH/src/github.com/hyperledger/fabric

git config user.name "jenkins"
git config user.email jenkins@jenkins.com


#apply patch for GREP11
git am $MY_PATH/../patches/0001-GREP11-Remote-EP11-BCCSP.patch


# [FAB-14646] Update dependency github.com/opencontainers/runc
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/94/30094/1 && git cherry-pick FETCH_HEAD


#apply trustbloc/Fabric-Mod transient data changes with fabric 1.4.1 in three steps
# step 1 apply go mod (to match dependencies in trustbloc/Fabric-Mod)
git am $MY_PATH/../patches/0001-Apply-go-modules-for-src-1.4.1.patch
# step 2 apply backport of transient data
git am $MY_PATH/../patches/0002-DEV-13700-Backport-transient-data-to-Fabric-1.4.1.patch
# step 3 apply gossip protos upgrade to Fab 2.0 to match trustbloc dependencies
git am $MY_PATH/../patches/0003-upgrade-gossip-protos-to-2.0.patch

