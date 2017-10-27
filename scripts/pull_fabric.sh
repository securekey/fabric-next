#!/bin/bash
#
# Copyright SecureKey Technologies Inc. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

rm -rf $GOPATH/src/github.com/hyperledger/fabric
mkdir -p $GOPATH/src/github.com/hyperledger/
cd $GOPATH/src/github.com/hyperledger/
git clone https://gerrit.hyperledger.org/r/fabric
cd fabric
git checkout 448d384e50ba1d929648bbdcd1eaafe00d6750f7

##Private Data:

#Collection Store and Collection configuration upon instantiate:
#https://gerrit.hyperledger.org/r/#/c/14515/ - Open - [FAB-6574] Integrate simpleCollectionStore for gossip
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/15/14515/6 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14517/ - Open - [FAB-6563] Merge External and Internal peers in config
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/17/14517/4 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14291/ - Open - [FAB-5871] create collections at deploy time
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/91/14291/22 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14367/ - Open - [FAB-5871] VSCC to ensure no collection exists
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/67/14367/13 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14371/ - Open - [FAB-6563] CLI support to specify collections
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/71/14371/16 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14519/ - Open - [FAB-6620] Prevent private data send in instantiate
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/19/14519/7 && git cherry-pick FETCH_HEAD

#Purge of private data based on block-to-live:
#https://gerrit.hyperledger.org/r/#/c/14347/ - Open - [FAB-6552] Block-to-live policy for pvtdata
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/47/14347/2 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14349/ - Open - [FAB-6553] Ledger bookkeeping provider
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/49/14349/1 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14351/ - Open - [FAB-6554] Expiry schedule keeper for pvt-statedb data
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/51/14351/1 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14353/ - Open - [FAB-6555] Purge manager for pvt statedb data
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/53/14353/1 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14355/ - Open - [FAB-6556] Enable purge from pvt statedb
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/55/14355/1 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14511/ - Open - [FAB-6619] purge pvtdata from pvt block store
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/11/14511/2 && git cherry-pick FETCH_HEAD

#Other:
#https://gerrit.hyperledger.org/r/#/c/14769/ - Open - [FAB-6600] Sample chaincode for prvt data
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/69/14769/2 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14791/ - Open - [FAB-6717] - Implement Private Data Funcs in MockStub
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/91/14791/1 && git cherry-pick FETCH_HEAD


##Filtered Channel Events:
#https://gerrit.hyperledger.org/r/#/c/12375/ - [FAB-5737] Implement server logic for Channel Service (cherry pick)
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/75/12375/44 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/12377/ - [FAB-5738] Implement client logic for Channel Service (cherry pick
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/77/12377/44 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/12379/ - [FAB-5744]Improve UT coverage for Channel Service svr
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/79/12379/44 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/12381/ - [FAB-5742] Add channel service listener sample
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/81/12381/45 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/12483/ - [FAB-5785] add SignedEvent based valid. to RSCC
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/83/12483/38 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/13663/ - [FAB-6249] add SignedEvent valid. to def. ACL provider
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/63/13663/20 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/12609/ - [FAB-5784] Implement ACL on Channel Service server (cherry pick)
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/09/12609/38 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/13001/ - [FAB-5977] Channel service listener with TLS enabled
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/01/13001/33 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14337/ - [FAB-6163] Add interest to reg. for channel service
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/37/14337/13 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14657/ - [FAB-6565] BLOCKORFILTEREDBLOCK interest channel serv
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/57/14657/7 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14889/ - [FAB-6422]Add timestamp and timewindow to channel serv
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/89/14889/3 && git cherry-pick FETCH_HEAD


##System chaincode plugins:
#https://gerrit.hyperledger.org/r/#/c/14753/ - [FAB-6719] Allow system chaincode plugins
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/53/14753/4 && git cherry-pick FETCH_HEAD

##Allow SCC to invoke another SCC:
#https://gerrit.hyperledger.org/r/#/c/14897/ - [FAB-5487] Allow SCC to invoke another SCC
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/97/14897/3 && git cherry-pick FETCH_HEAD

##TODO cherry pick service discovery##


make clean docker
