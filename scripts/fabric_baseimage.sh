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
git clone https://gerrit.hyperledger.org/r/fabric-baseimage
cd fabric-baseimage
git config advice.detachedHead false
# https://github.com/hyperledger/fabric-baseimage/releases/tag/v0.4.5
git checkout v${1}

cd $GOPATH/src/github.com/hyperledger/fabric-baseimage

#apply patch for CouchDB
git am $MY_PATH/../patches/0001-VMEENG-1622-change-couchdb-build.patch

git config user.name "jenkins"
git config user.email jenkins@jenkins.com
