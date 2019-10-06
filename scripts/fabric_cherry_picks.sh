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
# https://github.com/hyperledger/fabric/releases/tag/v1.4.3
git checkout v1.4.3

cd $GOPATH/src/github.com/hyperledger/fabric

git config user.name "jenkins"
git config user.email jenkins@jenkins.com

#apply patch for GREP11
git am $MY_PATH/../patches/0001-GREP11-Remote-EP11-BCCSP.patch

#apply patch for PKCS11 Thales
git am $MY_PATH/../patches/PKCS11-Thales.patch

# [FAB-14646] Update dependency github.com/opencontainers/runc
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/94/30094/1 && git cherry-pick FETCH_HEAD


# step 1 apply gossip protos extensions to match trustbloc/fabric-peer-ext package names dependencies
git am $MY_PATH/../patches/0001-gossip-protos-extensions-refactoring.patch
# step 2 apply transient data changes from trustbloc version of Fabric
git am $MY_PATH/../patches/0001-Backport-Transient-Data.patch
# step 3 apply transient data fix BLOC-1741 Fix for deadlock
git am $MY_PATH/../patches/0001-BLOC-1741-Fix-deadlock-when-retrieving-transient-dat.patch
# step 4 apply transient data fix BLOC-1814 Missing transient data when mutiple keys in write-set
git am $MY_PATH/../patches/0001-BLOC-1814-Missing-transient-data-if-multiple-keys.patch
# step 5 apply collection validation fix BLOC-1827 Prevent modification of collection type during upgrade
git am $MY_PATH/../patches/0001-BLOC-1827-Prevent-modification-of-collection-type-du.patch
# step 6 apply concurrent map write fix BLOC-1833 Concurrent writes to roles map
git am $MY_PATH/../patches/0001-BLOC-1833-Concurrent-writes-to-roles-map.patch
# step 7 apply patch for BLOC-1836 Performance improvement for pulling private data
git am $MY_PATH/../patches/0001-BLOC-1836-Performance-improvement-for-pulling-privat.patch
