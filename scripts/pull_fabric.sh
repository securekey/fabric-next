#!/bin/bash
#
# Copyright SecureKey Technologies Inc. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
set -e


MY_PATH="`dirname \"$0\"`"              # relative
MY_PATH="`( cd \"$MY_PATH\" && pwd )`"  # absolutized and normalized
if [ -z "$MY_PATH" ] ; then
  # error; for some reason, the path is not accessible
  # to the script (e.g. permissions re-evaled after suid)
  exit 1  # fail
fi


TMP=`mktemp -d 2>/dev/null || mktemp -d -t 'mytmpdir'`

GOPATH=$TMP

mkdir -p $GOPATH/src/github.com/hyperledger/
cd $GOPATH/src/github.com/hyperledger/
git clone https://gerrit.hyperledger.org/r/fabric
cd fabric
git checkout 4f7a7c8d696e866d06780e14b10704614a68564b

$MY_PATH/fabric_cherry_picks.sh

make clean
DOCKER_DYNAMIC_LINK=true BASE_DOCKER_NS=d1vyank make docker

rm -Rf $TMP

