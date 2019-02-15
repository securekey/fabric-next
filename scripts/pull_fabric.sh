#!/bin/bash
#
# Copyright SecureKey Technologies Inc. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
set -e

./check_version.sh
if [ $? != 0 ]; then
  exit 1
fi

source ../.ci-properties

declare -x FABRIC_BASE_OS_IMAGE=hyperledger/fabric-baseos
declare -x FABRIC_BASE_IMAGE=hyperledger/fabric-baseimage
declare -x BASE_VERSION=0.4.14

declare -x BASE_NAMESPACE=securekey

# This must match the version of fabric that is being cherry-picked
declare -x BASE_OUTPUT_VERSION=0.4.14
declare -x ARCH=$(go env GOARCH)

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
make clean
GO111MODULE=on DOCKER_DYNAMIC_LINK=true BASE_DOCKER_NS=$BASE_NAMESPACE EXPERIMENTAL=false GO_TAGS="pkcs11 pluginsenabled" make docker

chmod -R +rw $TMP

rm -Rf $TMP

cd $MY_PATH

# Build softhsm peer
docker build -f ./images/fabric-peer-softhsm/Dockerfile \
 --build-arg ARCH=${ARCH} \
 -t ${BASE_NAMESPACE}/fabric-peer-softhsm:${FABRIC_NEXT_IMAGE_TAG} \
 ./images/fabric-peer-softhsm

# Fabric ccenv image
declare -x FABRIC_CCENV_IMAGE=hyperledger/fabric-ccenv
# Use latest tag as that was the image produced by the fabric build above
declare -x FABRIC_CCENV_TAG=latest


# Build cross compile image
# Note cross compile currently uses fabric-ccenv image for now
if [[ "amd64" = "${ARCH}" ]]; then
  docker build -f ./images/fabric-cross-compile/Dockerfile --no-cache -t ${BASE_NAMESPACE}/fabric-cross-compile:${FABRIC_NEXT_IMAGE_TAG} \
  --build-arg FABRIC_CCENV_IMAGE=${FABRIC_CCENV_IMAGE} \
  --build-arg FABRIC_CCENV_TAG=${FABRIC_CCENV_TAG} .
  docker tag ${BASE_NAMESPACE}/fabric-cross-compile:${FABRIC_NEXT_IMAGE_TAG} ${BASE_NAMESPACE}/fabric-cross-compile:${ARCH}-latest
fi
