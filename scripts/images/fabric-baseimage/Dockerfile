#
# Copyright SecureKey Technologies Inc. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

ARG FABRIC_BASE_IMAGE=hyperledger/fabric-baseimage
ARG ARCH=x86_64
ARG FABRIC_BASE_VERSION=0.4.2

FROM ${FABRIC_BASE_IMAGE}:${ARCH}-${FABRIC_BASE_VERSION}
LABEL maintainer=sk-dev-team

# Install Go 1.9.2 temporarily. This will Go away once hyperledger tags new images
RUN rm -rf opt/go
RUN wget --quiet --no-check-certificate https://storage.googleapis.com/golang/go1.9.2.linux-amd64.tar.gz
RUN tar -xf go1.9.2.linux-amd64.tar.gz && mv go $GOROOT && chmod 775 $GOROOT
