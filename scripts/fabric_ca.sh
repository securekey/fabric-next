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
git clone https://github.com/hyperledger/fabric-ca
cd fabric-ca
git config advice.detachedHead false
git checkout v${1}

cd $GOPATH/src/github.com/hyperledger/fabric-ca

git config user.name "jenkins"
git config user.email jenkins@jenkins.com

#apply patch for GREP11
git am --directory vendor/github.com/hyperledger/fabric/ $MY_PATH/../patches/0001-GREP11-Remote-EP11-BCCSP.patch -3

#apply patch for Thales PKCS11
git am --directory vendor/github.com/hyperledger/fabric/ $MY_PATH/../patches/PKCS11-Thales-CA.patch

# apply FAB-17517
git am --directory vendor/github.com/hyperledger/fabric/ $MY_PATH/../patches/0001-FAB-17517-Only-Initialize-specified-provider-fabric-ca.patch

# fetching and update vendored packages for grep11 patch
rm -rf vendor/google.golang.org/grpc
git clone https://github.com/grpc/grpc-go/ vendor/google.golang.org/grpc/
cd vendor/google.golang.org/grpc/
git checkout b5071124392bfd416c713e6595ac149b387f7186

cd $GOPATH/src/github.com/hyperledger/fabric-ca
rm -rf vendor/golang.org/x/net
git clone https://github.com/golang/net vendor/golang.org/x/net
cd vendor/golang.org/x/net/
git checkout ba9fcec4b297b415637633c5a6e8fa592e4a16c3

cd $GOPATH/src/github.com/hyperledger/fabric-ca
rm -rf vendor/golang.org/x/text
git clone https://github.com/golang/text vendor/golang.org/x/text
cd vendor/golang.org/x/text/
git checkout 6f44c5a2ea40ee3593d98cdcc905cc1fdaa660e2

cd $GOPATH/src/github.com/hyperledger/fabric-ca
rm -rf vendor/github.com/google/go-genproto
git clone https://github.com/googleapis/go-genproto vendor/github.com/google/go-genproto
cd  vendor/github.com/google/go-genproto
git checkout b0a3dcfcd1a9bd48e63634bd8802960804cf8315

cd $GOPATH/src/github.com/hyperledger/fabric-ca
rm -rf vendor/github.com/golang/protobuf
git clone https://github.com/golang/protobuf vendor/github.com/golang/protobuf
cd vendor/github.com/golang/protobuf
git checkout v1.3.0
