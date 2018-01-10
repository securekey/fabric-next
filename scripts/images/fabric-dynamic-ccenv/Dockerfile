#
# Copyright SecureKey Technologies Inc.
# This file contains software code that is the intellectual property of SecureKey.
# SecureKey reserves all rights in the code and you may not use it without written permission from SecureKey.
#

ARG FABRIC_CCENV_IMAGE=hyperledger/fabric-ccenv
ARG FABRIC_CCENV_TAG=latest
FROM ${FABRIC_CCENV_IMAGE}:${FABRIC_CCENV_TAG}
COPY ./images/fabric-dynamic-ccenv/go /usr/local/bin
COPY ./images/fabric-dynamic-ccenv/patches/linker.patch /tmp/scripts/patches/linker.patch
RUN apt-get update
RUN apt-get install -y libltdl7
RUN patch -p1 $GOROOT/src/cmd/link/internal/ld/data.go /tmp/scripts/patches/linker.patch
RUN go install -a cmd
RUN go install -a std
