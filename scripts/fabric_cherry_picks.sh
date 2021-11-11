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

# git clone and checkout Hyperledger Fabric source code
mkdir -p $GOPATH/src/github.com/hyperledger/
git clone https://github.com/hyperledger/fabric $GOPATH/src/github.com/hyperledger/fabric
cd $GOPATH/src/github.com/hyperledger/fabric

git config advice.detachedHead false
git config user.name "jenkins"
git config user.email jenkins@jenkins.com


git checkout v1.4.12

# apply patch for GREP11
git am $MY_PATH/../patches/0001-GREP11-Remote-EP11-BCCSP.patch

# apply patch for PKCS11 Thales
git am $MY_PATH/../patches/PKCS11-Thales.patch

# [FAB-14646] Update dependency github.com/opencontainers/runc
git am $MY_PATH/../patches/runc.patch

# Change Dockerfiles
git apply $MY_PATH/../patches/fabric_build.patch

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
# step 8 apply patch for DEV-16266 Remove throttle
git am $MY_PATH/../patches/0001-DEV-16266-Remove-throttle.patch
# step 9 apply patch for DEV-18114 Add standby role to peer
git am $MY_PATH/../patches/0001-DEV-18114-Add-standby-role-to-peer.patch
# step 10 apply patch for gotools make target to fix manifest-tool build
git am $MY_PATH/../patches/0001-Override-for-manifest-tool.patch
# step 11 apply patch for PKCS11 resilience
git am $MY_PATH/../patches/0001-PKCS11-resilience.patch
