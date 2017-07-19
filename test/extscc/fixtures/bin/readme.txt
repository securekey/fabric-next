# Build the CC golang source package from this directory
# IMPORTANT: This example has the version (-v option) set to 0. Make sure to increase the version if the CC is changed!!!
../../../../build/bin/peer chaincode package -S -n extscc1 -p github.com/hyperledger/fabric/test/extscc/fixtures/src/github.com/extscc_cc -v 0 ../config/extsysccs/extscc1.golang
