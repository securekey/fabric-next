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
git clone https://gerrit.hyperledger.org/r/fabric-ca
cd fabric-ca
git config advice.detachedHead false
# https://github.com/hyperledger/fabric/releases/tag/v1.4.2
git checkout v1.4.2

cd $GOPATH/src/github.com/hyperledger/fabric-ca

git config user.name "jenkins"
git config user.email jenkins@jenkins.com


#apply patch for GREP11
git am --directory vendor/github.com/hyperledger/fabric/ $MY_PATH/../patches/0001-GREP11-Remote-EP11-BCCSP.patch

# fetching grpc for grep11 patch
rm -rf vendor/google.golang.org/grpc
git clone https://github.com/grpc/grpc-go/ vendor/google.golang.org/grpc/
cd vendor/google.golang.org/grpc/
git checkout 1f1a4999ca75ba4fd6d5c91233383a170034a1a5

rm -rf vendor/golang.org/x/net
git clone https://github.com/golang/net vendor/google.golang.org/x/net
cd vendor/google.golang.org/x/net/
git checkout ba9fcec4b297b415637633c5a6e8fa592e4a16c3

cd $GOPATH/src/github.com/hyperledger/fabric-ca
