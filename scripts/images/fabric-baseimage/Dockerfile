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

# Remove node npm
# https://stackoverflow.com/a/11178106
RUN ["/bin/bash", "-c", "rm -rf /usr/local/{lib/node{,/.npm,_modules},bin,share/man}/{npm*,node*,man1/node*}"]

# Remove python
RUN apt-get -y purge python

# Faking npm because of this commit
# https://github.com/hyperledger/fabric/commit/a7ca2fc55ba6b4b6fa9f9a4c591dab2e79366d05
RUN touch /usr/local/bin/npm \
    && chmod +x /usr/local/bin/npm

# System package Update
RUN apt-get update -qq \
    && apt-get -y dist-upgrade \
    && apt-get clean autoclean \
    && apt-get autoremove -y \
    && rm -rf /var/lib/cache /var/lib/log /var/lib/apt/lists/*
