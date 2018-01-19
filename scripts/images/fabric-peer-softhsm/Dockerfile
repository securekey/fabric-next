#
# Copyright SecureKey Technologies Inc. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
# This image is for TESTING purposes only.
# Not production grade.
#

ARG   FABRIC_PEER_BASE_IMAGE=hyperledger/fabric-peer
ARG   FABRIC_PEER_TAG=latest

FROM  ${FABRIC_PEER_BASE_IMAGE}:${FABRIC_PEER_TAG}

ARG   go_version=1.9.2
ARG   gopath=/opt/gopath
ARG   goroot=/opt/go
ARG   pkcs11_tool_url=https://github.com/hyperledger/fabric-sdk-go
ARG   ARCH=x86_64

LABEL maintainer=sk-service-eng-team

ENV   GOROOT=${goroot} \
      GOPATH=${gopath} \
      PATH=${PATH}:${gopath}/bin:${goroot}/bin

RUN   set -xe; \
      apt-get update && \
      apt-get install -y --no-install-recommends softhsm2 curl git gcc g++ libtool libltdl-dev && \
      rm -rf /var/lib/apt/lists/* && \
      mkdir -p /var/lib/softhsm/tokens/ && \
      softhsm2-util --init-token --slot 0 --label "ForFabric" --so-pin 1234 --pin 98765432 && \
      mkdir -p ${GOROOT} ${GOPATH}/src && \
      export ARCH_URL=amd64 && \
      if [ "${ARCH}" = "s390x" ]; then export ARCH_URL=s390x; fi && \
      GO_URL=https://storage.googleapis.com/golang/go${go_version}.linux-${ARCH_URL}.tar.gz; \
      curl -o /tmp/go.tar.gz ${GO_URL} && \
      tar -xvzf /tmp/go.tar.gz -C /opt/ && \
      rm -rf /tmp/go.tar.gz && \
      go get -u github.com/golang/dep/cmd/dep && \
      git clone ${pkcs11_tool_url} /tmp/pk11 && \
      cp -rp /tmp/pk11/scripts/_go/src/pkcs11helper ${GOPATH}/src/ && \
      rm -rf /tmp/pk11 && \
      cd ${GOPATH}/src/pkcs11helper && dep ensure -vendor-only -v && \
      go install pkcs11helper/cmd/pkcs11helper
