#
# Copyright SecureKey Technologies Inc. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#


ARG FABRIC_CCENV_IMAGE=hyperledger/fabric-ccenv
ARG FABRIC_CCENV_TAG=latest
FROM ${FABRIC_CCENV_IMAGE}:${FABRIC_CCENV_TAG}
RUN mkdir -p /chaincode/input/src/github.com/hyperledger/fabric
RUN mv  $GOPATH/src/github.com/hyperledger/fabric /chaincode/input/src/github.com/hyperledger/
ENV GOPATH="/chaincode/input"