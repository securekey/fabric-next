#!/bin/bash
#
# Copyright SecureKey Technologies Inc. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

export CORE_CHAINCODE_ID_NAME=snap1:1.0.0-rc1
export CORE_PEER_ADDRESS=peer0.org1.example.com:7051
export CORE_CHAINCODE_LOGGING_LEVEL=debug
export CORE_PEER_TLS_ENABLED=true
export CORE_PEER_TLS_SERVERHOSTOVERRIDE=peer0.org1.example.com
export CORE_PEER_TLS_ROOTCERT_FILE=$GOPATH/src/github.com/hyperledger/fabric-sdk-go/test/fixtures/channel/crypto-config/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/cacerts/org1.example.com-cert.pem
go run github.com/extscc_cc/extscc_cc.go
