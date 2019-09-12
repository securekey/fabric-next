#
# Copyright SecureKey Technologies Inc. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

ARG FABRIC_COUCHDB_IMAGE=hyperledger/fabric-couchdb
ARG ARCH=amd64
ARG FABRIC_BASE_VERSION=0.4.8
FROM ${FABRIC_COUCHDB_IMAGE}:${ARCH}-${FABRIC_BASE_VERSION}
LABEL maintainer=sk-dev-team

# System package update
RUN apt-get update -qq \
    && apt-get -y dist-upgrade \
    && apt-get clean autoclean \
    && apt-get autoremove -y \
    && rm -rf /var/lib/cache /var/lib/log /var/lib/apt/lists/*
