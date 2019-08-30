#
# Copyright SecureKey Technologies Inc. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
ARG FABRIC_BASE_IMAGE=hyperledger/fabric-baseimage
ARG ARCH=amd64
ARG FABRIC_BASE_VERSION=0.4.8
FROM ${FABRIC_BASE_IMAGE}:${ARCH}-${FABRIC_BASE_VERSION}
LABEL maintainer=sk-dev-team
RUN apt-get update -qq \
    && apt-get -y upgrade \
    && apt-get clean autoclean \
    && apt-get autoremove -y \
    && rm -rf /var/lib/cache /var/lib/log

# Remove node npm
# https://stackoverflow.com/a/11178106
RUN ["/bin/bash", "-c", "rm -rf /usr/local/{lib/node{,/.npm,_modules},bin,share/man}/{npm*,node*,man1/node*}"]
