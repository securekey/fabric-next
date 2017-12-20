#!/bin/bash
#
# Copyright SecureKey Technologies Inc. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
set -e

# Using images built from the tip of https://github.com/hyperledger/fabric-baseimage until hyperledger tags new images with Go1.9.2
declare -x FABRIC_BASE_OS_IMAGE=securekey/fabric-baseos
declare -x FABRIC_BASE_IMAGE=securekey/fabric-baseimage
declare -x BASE_VERSION=0.4.3-snapshot-168b4d5

declare -x BASE_NAMESPACE=securekey
# This must match the version of fabric that is being cherry-picked
declare -x BASE_OUTPUT_VERSION=0.4.2
declare -x ARCH=$(uname -m)

# Build base images to enable dynamic build
docker build -f ./images/fabric-baseos/Dockerfile --no-cache -t ${BASE_NAMESPACE}/fabric-baseos:${ARCH}-${BASE_OUTPUT_VERSION} \
--build-arg FABRIC_BASE_OS_IMAGE=${FABRIC_BASE_OS_IMAGE} \
--build-arg ARCH=${ARCH} \
--build-arg FABRIC_BASE_VERSION=${BASE_VERSION} .

docker build -f ./images/fabric-baseimage/Dockerfile --no-cache -t ${BASE_NAMESPACE}/fabric-baseimage:${ARCH}-${BASE_OUTPUT_VERSION} \
--build-arg FABRIC_BASE_IMAGE=${FABRIC_BASE_IMAGE} \
--build-arg ARCH=${ARCH} \
--build-arg FABRIC_BASE_VERSION=${BASE_VERSION} .


MY_PATH="`dirname \"$0\"`"              # relative
MY_PATH="`( cd \"$MY_PATH\" && pwd )`"  # absolutized and normalized
if [ -z "$MY_PATH" ] ; then
  # error; for some reason, the path is not accessible
  # to the script (e.g. permissions re-evaled after suid)
  exit 1  # fail
fi

TMP=`mktemp -d 2>/dev/null || mktemp -d -t 'mytmpdir'`
echo "Build tmp directory is $TMP ..."

export GOPATH=$TMP

$MY_PATH/fabric_cherry_picks.sh

cd $GOPATH/src/github.com/hyperledger/fabric
#Mutual TLS fix for peer CLI
git apply $MY_PATH/../patches/peerCLITLS.patch
make clean
DOCKER_DYNAMIC_LINK=true BASE_DOCKER_NS=$BASE_NAMESPACE make docker

rm -Rf $TMP
