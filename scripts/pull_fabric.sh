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

declare -x FABRIC_BASE_OS_IMAGE=repo.onetap.ca:8444/next/hyperledger/fabric-baseos
declare -x FABRIC_BASE_IMAGE=repo.onetap.ca:8444/next/hyperledger/fabric-baseimage
declare -x FABRIC_COUCHDB_IMAGE=repo.onetap.ca:8444/next/hyperledger/fabric-couchdb
declare -x BASE_VERSION=0.4.22

# this is where the baseimage builder job drops off the fresh images
declare -x BASE_NAMESPACE=repo.onetap.ca:8444/next/hyperledger

# this is what is being used to prefix the built images
declare -x DOCKER_NS=repo.onetap.ca:8444/next/hyperledger

# This must match the version of fabric that is being cherry-picked
declare -x BASE_OUTPUT_VERSION=${BASE_VERSION}
declare -x ARCH=$(go env GOARCH)

# SETTING variables
declare -x FABRIC_CA_VERSION=1.4.9

MY_PATH="`dirname \"$0\"`"              # relative
MY_PATH="`( cd \"$MY_PATH\" && pwd )`"  # absolutized and normalized
if [ -z "$MY_PATH" ] ; then
  # error; for some reason, the path is not accessible
  # to the script (e.g. permissions re-evaled after suid)
  exit 1  # fail
fi

############################################
#            Fabric Build                  #
############################################

TMP=`mktemp -d 2>/dev/null || mktemp -d -t 'mytmpdir'`
echo "Build tmp directory is $TMP ..."

export GOPATH=$TMP

$MY_PATH/fabric_cherry_picks.sh

cd $GOPATH/src/github.com/hyperledger/fabric
make clean

# Building all the images
DOCKER_DYNAMIC_LINK=true BASE_DOCKER_NS=$BASE_NAMESPACE EXPERIMENTAL=false GO_TAGS="pkcs11 pluginsenabled" make docker

rm -Rf $TMP

cd $MY_PATH

# Build softhsm peer
docker build -f ./images/fabric-peer-softhsm/Dockerfile \
 --build-arg ARCH=${ARCH} \
 -t ${BASE_NAMESPACE}/fabric-peer-softhsm:${FABRIC_NEXT_IMAGE_TAG} \
 ./images/fabric-peer-softhsm

# Fabric ccenv image
# declare -x FABRIC_CCENV_IMAGE=${DOCKER_NS}/fabric-ccenv
# Use latest tag as that was the image produced by the fabric build above
# declare -x FABRIC_CCENV_TAG=latest


# Build cross compile image
# Note cross compile currently uses fabric-ccenv image for now
# if [[ "amd64" = "${ARCH}" ]]; then
#   docker build -f ./images/fabric-cross-compile/Dockerfile --no-cache -t ${BASE_NAMESPACE}/fabric-cross-compile:${FABRIC_NEXT_IMAGE_TAG} \
#   --build-arg FABRIC_CCENV_IMAGE=${FABRIC_CCENV_IMAGE} \
#   --build-arg FABRIC_CCENV_TAG=${FABRIC_CCENV_TAG} .
#   docker tag ${BASE_NAMESPACE}/fabric-cross-compile:${FABRIC_NEXT_IMAGE_TAG} ${BASE_NAMESPACE}/fabric-cross-compile:${ARCH}-latest
# fi

############################################
#            Fabric CA                     #
############################################

TMP=`mktemp -d 2>/dev/null || mktemp -d -t 'mytmpdir'`
echo "Build tmp directory is $TMP ..."

export GOPATH=$TMP

$MY_PATH/fabric_ca.sh ${FABRIC_CA_VERSION}

cd $GOPATH/src/github.com/hyperledger/fabric-ca
make clean

# Building fabric-ca-client
GO_TAGS="pkcs11 netgo caclient" FABRIC_CA_DYNAMIC_LINK=true EXPERIMENTAL=false make fabric-ca-client
# Building fabric-ca-server
GO_TAGS="pkcs11 netgo caclient" FABRIC_CA_DYNAMIC_LINK=true EXPERIMENTAL=false make fabric-ca-server

# Putting the binaries to be picked up by make docker
mkdir -p build/docker/
cp -rav bin build/docker/

# Making docker image
FABRIC_CA_DYNAMIC_LINK=true BASE_DOCKER_TAG=amd64-${BASE_VERSION} BASE_DOCKER_NS=$BASE_NAMESPACE EXPERIMENTAL=false make docker

rm -Rf $TMP
