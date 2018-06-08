#!/bin/bash
#
# Copyright SecureKey Technologies Inc. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
set -e

# NEEDED for ACL 1.2 FAB-9401 patch
MY_PATH="`dirname \"$0\"`"              # relative
MY_PATH="`( cd \"$MY_PATH\" && pwd )`"  # absolutized and normalized
if [ -z "$MY_PATH" ] ; then
  # error; for some reason, the path is not accessible
  # to the script (e.g. permissions re-evaled after suid)
  exit 1  # fail
fi

mkdir -p $GOPATH/src/github.com/hyperledger/
cd $GOPATH/src/github.com/hyperledger/
git clone https://gerrit.hyperledger.org/r/fabric
cd fabric
git config advice.detachedHead false
git checkout v1.1.0

cd $GOPATH/src/github.com/hyperledger/fabric

git config user.name "jenkins"
git config user.email jenkins@jenkins.com


#https://gerrit.hyperledger.org/r/#/c/22763/ [FAB-10538]: Fix gossip scatter of pvt data
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/63/22763/1 && git cherry-pick FETCH_HEAD


#https://gerrit.hyperledger.org/r/c/12169/ [DRAFT] Remote EP11 BCCSP
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/69/12169/7 && git cherry-pick FETCH_HEAD

#Purge of private data based on block-to-live
#https://gerrit.hyperledger.org/r/#/c/14347/ - Open - [FAB-6552] Block-to-live policy for pvtdata
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/47/14347/8 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14349/ - Open - [FAB-6553] Ledger bookkeeping provider
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/49/14349/7 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14351/ - Open - [FAB-6554] Expiry schedule keeper for pvt-statedb data
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/51/14351/7 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14353/ - Open - [FAB-6555] Purge manager for pvt statedb data
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/53/14353/8 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14355/ - Open - [FAB-6556] Enable purge from pvt statedb
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/55/14355/8 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14511/ - Open - [FAB-6619] purge pvtdata from pvt block store
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/11/14511/8 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/c/16971/ - Open - [FAB-7810] Enable BTL via collection config
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/71/16971/6 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/c/18003/ - Open - [FAB-8347] Fix re-entrant lock isuue
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/03/18003/1 && git cherry-pick FETCH_HEAD

#Other Private Data
#https://gerrit.hyperledger.org/r/#/c/14769/ - Open - [FAB-6600] Sample chaincode for prvt data
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/69/14769/7 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/#/c/14791/ - Open - [FAB-6717] - Implement Private Data Funcs in MockStub
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/91/14791/5 && git cherry-pick FETCH_HEAD

#Deliver Service bug fix
#https://gerrit.hyperledger.org/r/c/20239/ - Closed - [FAB-9389] peer deliver client crashes when it is started and stopped immediately after
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/39/20239/1 && git cherry-pick FETCH_HEAD

# https://gerrit.hyperledger.org/r/c/22553/ - Open - Gossip deadlock issue
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/53/22553/1 && git cherry-pick FETCH_HEAD

# Fabric v1.2 ACL implementation (see FAB-8727)
#https://gerrit.hyperledger.org/r/c/19967/ - Merged - [FAB-9252] Add ImplicitMetaPolicy parser
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/67/19967/2 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/c/20117/ - Merged - [FAB-9254] Specify policies in configtx.yaml
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/17/20117/1 && git cherry-pick -Xours FETCH_HEAD
#https://gerrit.hyperledger.org/r/c/20119/ - Merged - [FAB-9255] configtxgen encode policy specs
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/19/20119/2 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/c/20225/ - Merged - [FAB-9409] add ACL spec to configtx
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/25/20225/7 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/c/20323/ - Merged - [FAB-9401] sanitize resource names and add doc
#git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/23/20323/8 && git cherry-pick FETCH_HEAD
git am $MY_PATH/../patches/0001-FAB-9401-sanitize-resource-names-and-add-doc.patch
#https://gerrit.hyperledger.org/r/c/20519/ - Merged - [FAB-9014] Add new config element for peer ACLs
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/19/20519/3 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/c/20541/ - Merged - [FAB-9507] replace "." from resource names
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/41/20541/7 && git cherry-pick FETCH_HEAD
#https://gerrit.hyperledger.org/r/c/20621/ - Merged - [FAB-9531] implement ACL in channelconfig
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/21/20621/9 && git cherry-pick FETCH_HEAD

#https://gerrit.hyperledger.org/r/c/19281/ - Merged - [FAB-8918] Save viper lookup in GetLocalMSP
git fetch https://gerrit.hyperledger.org/r/fabric refs/changes/81/19281/6 && git cherry-pick FETCH_HEAD

##TODO cherry pick service discovery##

# https://jira.hyperledger.org/browse/FAB-10576 - Open - [FAB-10576] Prevent peer from crashing while processing pull request in absense of collection config
git fetch https://gerrit.hyperledger.org:29418/fabric refs/changes/71/22871/1 && git cherry-pick FETCH_HEAD

# latest 1.1 branch release commit including previous bug fixes that were not cherry picked before
# https://jira.hyperledger.org/browse/FAB-10521 - Merged - [FAB-10521] Block-cutter should refetch config
git fetch https://gerrit.hyperledger.org:29418/fabric refs/changes/55/22855/2 && git cherry-pick FETCH_HEAD
