#
# Copyright SecureKey Technologies Inc. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
ARG FABRIC_ZOOKEEPER_IMAGE=hyperledger/fabric-zookeeper
ARG ARCH=amd64
ARG FABRIC_BASE_VERSION=0.4.8
FROM ${FABRIC_ZOOKEEPER_IMAGE}:${ARCH}-${FABRIC_BASE_VERSION}
LABEL maintainer=sk-dev-team

RUN apt-get update -qq \
    && apt-get install -y aria2 \
    && apt-get -y --print-uris dist-upgrade | egrep -o -e "http://[^\']+" | aria2c -c -d /var/cache/apt/archives -i -; apt-get -y dist-upgrade \
    && apt-get clean autoclean \
    && apt-get autoremove -y \
    && rm -rf /var/lib/cache /var/lib/log
