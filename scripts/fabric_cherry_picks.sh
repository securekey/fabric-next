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
# Tip of release-1.2 (September 6, 2018)
git checkout 78a3a8dadf2a82c2e535bcacaeb95fbdd4fbafde

cd $GOPATH/src/github.com/hyperledger/fabric

git config user.name "jenkins"
git config user.email jenkins@jenkins.com

# Cherry pick [FAB-11777] Expose LevelDB configuration
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/29/25929/4 && git cherry-pick FETCH_HEAD

#apply patch for GREP11 (5151f212d3edd89fbabc12fbe702cecea0cb4b3a + local fixes)
git am $MY_PATH/../patches/0001-GREP11-Remote-EP11-BCCSP.patch

# [FAB-11247] Add configuration to create _global_changes
git am $MY_PATH/../patches/0001-FAB-11247-Add-configuration-to-create-_global_change.patch

# [FAB-8622] Reduce exclusive lock duration during commit
#git am $MY_PATH/../patches/0001-FAB-8622-Red.-exclusive-lock-dur.-during-commit.patch

#git am $MY_PATH/../patches/0001-FAB-11779-Move-goleveldb-version.patch

# **Temporary** metrics collection
git am $MY_PATH/../patches/0001-Ledger-metrics.patch
