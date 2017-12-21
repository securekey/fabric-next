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
git checkout 417b17c7cbbcf181ea7e7c3f9a999f8314d94e9f

cd $GOPATH/src/github.com/hyperledger/fabric

git config user.name "jenkins"
git config user.email jenkins@jenkins.com


##Private Data:

#Collection Store and Collection configuration upon instantiate:
#https://gerrit.hyperledger.org/r/#/c/14367/ - Open - [FAB-5871] VSCC to ensure no collection exists
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/67/14367/21 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14371/ - Open - [FAB-6563] CLI support to specify collections
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/71/14371/27 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14519/ - Open - [FAB-6620] Prevent private data send in instantiate
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/19/14519/19 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/c/14961/  - Open - [FAB-6671] call VSCC for tx with pvt writes only
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/61/14961/7 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/c/16403/  - Open - [FAB-7497] fix typos in previous commit
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/03/16403/2 && git cherry-pick FETCH_HEAD

#Purge of private data based on block-to-live:
#https://gerrit.hyperledger.org/r/#/c/14347/ - Open - [FAB-6552] Block-to-live policy for pvtdata
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/47/14347/5 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14349/ - Open - [FAB-6553] Ledger bookkeeping provider
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/49/14349/4 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14351/ - Open - [FAB-6554] Expiry schedule keeper for pvt-statedb data
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/51/14351/4 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14353/ - Open - [FAB-6555] Purge manager for pvt statedb data
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/53/14353/4 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14355/ - Open - [FAB-6556] Enable purge from pvt statedb
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/55/14355/4 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14511/ - Open - [FAB-6619] purge pvtdata from pvt block store
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/11/14511/4 && git cherry-pick FETCH_HEAD

#https://gerrit.hyperledger.org/r/c/16343/ - Open -[FAB-7522] Customize private data push ack timeout
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/43/16343/3 && git cherry-pick FETCH_HEAD

#Other:
#https://gerrit.hyperledger.org/r/#/c/14769/ - Open - [FAB-6600] Sample chaincode for prvt data
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/69/14769/7 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14791/ - Open - [FAB-6717] - Implement Private Data Funcs in MockStub
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/91/14791/5 && git cherry-pick FETCH_HEAD

# Temp workaround for tlsCertHash issue
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/99/16399/1 && git cherry-pick FETCH_HEAD

# Filtered Block Events:
#https://gerrit.hyperledger.org/r/c/16341/ - Open - [FAB-7521] Lookup correct policy name
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/41/16341/4 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/c/16179/ - Open - [FAB-7419] FilteredBlock events
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/79/16179/22 && git cherry-pick FETCH_HEAD

##TODO cherry pick service discovery##
