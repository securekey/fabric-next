#!/bin/bash
#
# Copyright SecureKey Technologies Inc. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
set -e


mkdir -p $GOPATH/src/github.com/hyperledger/
cd $GOPATH/src/github.com/hyperledger/
git clone https://gerrit.hyperledger.org/r/fabric
cd fabric
git config advice.detachedHead false
git checkout 3178dbf3e5bc1d21a3e0297f393facc7d0e69d0a

cd $GOPATH/src/github.com/hyperledger/fabric

git config user.name "jenkins"
git config user.email jenkins@jenkins.com


#https://gerrit.hyperledger.org/r/c/12169/ - Abandoned - [DRAFT] Remote EP11 BCCSP
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/69/12169/6 && git cherry-pick FETCH_HEAD

#Purge of private data based on block-to-live
#https://gerrit.hyperledger.org/r/#/c/14347/ - Open - [FAB-6552] Block-to-live policy for pvtdata
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/47/14347/6 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14349/ - Open - [FAB-6553] Ledger bookkeeping provider
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/49/14349/5 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14351/ - Open - [FAB-6554] Expiry schedule keeper for pvt-statedb data
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/51/14351/5 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14353/ - Open - [FAB-6555] Purge manager for pvt statedb data
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/53/14353/5 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14355/ - Open - [FAB-6556] Enable purge from pvt statedb
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/55/14355/5 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14511/ - Open - [FAB-6619] purge pvtdata from pvt block store
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/11/14511/5 && git cherry-pick FETCH_HEAD

#Other Private Data
#https://gerrit.hyperledger.org/r/#/c/14769/ - Open - [FAB-6600] Sample chaincode for prvt data
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/69/14769/7 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14791/ - Open - [FAB-6717] - Implement Private Data Funcs in MockStub
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/91/14791/5 && git cherry-pick FETCH_HEAD

# Filtered Block Events
#https://gerrit.hyperledger.org/r/c/16569/ - Open - [FAB-7644]  Generalize deliver API handler
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/69/16569/12 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/c/16645/ - Open - [FAB-7419] Filtering block to levarage deliver impl.
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/45/16645/16 && git cherry-pick FETCH_HEAD


##TODO cherry pick service discovery##
