# Build the CC golang source package from this directory
# IMPORTANT: This example has the version (-v option) set to 1.0.0. Make sure this version changes with the fabric version. Current Fabric version is 1.0.0
../../../../build/bin/peer chaincode package -S -n extscc1 -p github.com/hyperledger/fabric/test/extscc/fixtures/src/github.com/extscc_cc -v 1.0.0 ../deploy/extscc1.golang && chmod 755 ../deploy/extscc1.golang

# Build the CC golang binary package from this directory
# IMPORTANT: This example has the version (-v option) set to 1.0.0. Make sure this version changes with the fabric version. Current Fabric version is 1.0.0
../../../../build/bin/peer chaincode package -S -n extscc3 -p ../src/github.com/extscc_cc_binary -v 1.0.0 ../deploy/extscc3.golang -l binary && chmod 755 ../deploy/extscc3.golang

# Build the ext SCC callers source package from this directory
# IMPORTANT: This example has the version (-v option) set to 1.0.0. Make sure this version changes with the fabric version. Current Fabric version is 1.0.0
../../../../build/bin/peer chaincode package -S -n extscccaller -p github.com/hyperledger/fabric/test/extscc/fixtures/src/github.com/extscccaller_cc -v 1.0.0 ../deploy/extscccaller.golang && chmod 755 ../deploy/extscccaller.golang
../../../../build/bin/peer chaincode package -S -n extscccaller2 -p github.com/hyperledger/fabric/test/extscc/fixtures/src/github.com/extscccaller_cc2 -v 1.0.0 ../deploy/extscccaller2.golang && chmod 755 ../deploy/extscccaller2.golang

##### STEPS TO GENERATE EXECUTABLE BINARY###########
* Go to directory where your external system chaincode is located
For example: github.com/hyperledger/fabric/test/extscc/fixtures/src/github.com/extscc_cc

* run 'go build' command on your external system chaincode
For example :
go build -ldflags "-linkmode external -extldflags '-static'" -o ../extscc_cc_binary/extscc_cc  extscc_cc.go

#Note: Above command has to be run in target platform, for example "Vagrant" for mac
#####################################################


