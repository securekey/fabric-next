 extscc_calls examples demonstrate the invocation of external system chaincodes.

 read the section 'Configuration and Installation of EXT SCCs' below to configure new EXT SCCs in Fabric

 extscc.go and extscc/client.go simulate a client that will invoke external SCC
 the example chaincodes used are:
     1. in peer process open source external SCC called: extscc1 (source code is from: extscc_cc) invoked by extscccaller_cc
     2. docker private binary ext SCC called: extscc3 (using same source code as above, but as a pre built binary) invoked by extscccaller_cc2

 All ext SCC source code files are found in this path:
cd $GOPATH/src/github.com/hyperledger/fabric/test/extscc/fixtures/src

 extscc_cc is the main example external chaincode. It will be invoked by other external CCs
 extscccaller_cc is invoking extscc1 deployed from source code (which is a copy of extscc_cc)
 extscccaller_cc2 is invoking extscc3 deployed from binary (which is also a copy of extscc_cc)
 all external SCCs are defined and configured in the docker compose yaml file found in the fixtures folder mentioned below.


 open a command shell window and navigate to the test docker compose yaml found at this path:
cd $GOPATH/src/github.com/hyperledger/fabric/test/extscc/fixtures


 start docker compose by running this command:
(source .env && docker-compose down && docker-compose up --force-recreate)

 Once peers are up, open another command shell
 and navigate to the path of this readme file to run the following examples:
cd $GOPATH/src/github.com/hyperledger/fabric/examples/extscc_calls


 to run extcaller_cc on peer0 for org1, execute the follwing command:
go run extscc.go -p localhost:7051 --peer.tls.rootcert.file $GOPATH/src/github.com/hyperledger/fabric/test/extscc/fixtures/channel/crypto-config/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/cacerts/org1.example.com-cert.pem -i Org1MSP --peer.tls.serverhostoverride peer0.org1.example.com -m $GOPATH/src/github.com/hyperledger/fabric/test/extscc/fixtures/channel/crypto-config/peerOrganizations/org1.example.com/peers/peer0.org1.example.com --peer.tls.enabled=true

 this will run the client that will invoke extcaller_cc which will in turn invoke extscc1 without joining a channel.
 to invoke the external SCC on an already existing channel joined by peer0 add this argument to the command above:
--channel=mychannel
or
-c mychannel

 where 'mychannel' is the channel name joined by peer0.


 to invoke the external SCC for org2, replace org1 by org2 in the command above and replace the peer port from 7051 to 8051 to communicate with peer1 instead. Org1MSP must also be switched to Ogr2MSP.

 to invoke extcaller_cc2 that invokes the binary extscc3 (simulating private ext scc), add this argument to the same command mentioned above:
--invoke.binary=true
or
-b true


########################################################
#
#  Configuration and Installation of EXT SCCs
#
########################################################

 External SCCs are treated by Fabric as in-process SCCs with the only difference is the former are configurable and pluggable while the latter are hard coded.

 Each Peer must be configured with the EXT SCCs it needs to support.

 To enable the EXT SCC feature in Fabric, make sure to set this flag to true under the peer's 'environment' map in the docker compose yaml file:
 - CORE_CHAINCODE_SYSTEMEXT_ENABLED=true

 if set to false, the peer will ignore any EXT SCC configuration

 Each EXT SCC must be packaged using the 'peer package' cli command into a CC Deployment Specification or CDS package.
 See the following readme.txt file for examples on how to generate these packages:
cd $GOPATH/fabric/test/extscc/fixtures/bin/readme.txt

 extscc1 is for an open source EXT SCC compiled from source. It's CDS package will contain the source code.
 extscc3 is for a binary (pre-compiled) EXT SCC that shows an example of a private/closed source EXT SCC. It's CDS package contains the pre-compiled binary without the source code.
 extscccaller is used to show an example of one EXT SCC invoking another EXT SCC. It calls extscc1
 extscccaller2 is also used to invoke another EXT SCC but for a pre-compiled binary one. It invokes extscc3

 In order to read the generated CDS package for your EXT SCC, make sure to add it in the path defined under the peer's 'environment' map in docker compose yaml file under this variable:
     # path of External SCCs to read files with CodeDeploymentSpec object
     - CORE_CHAINCODE_SYSTEMEXT_CDS_PATH=/opt/extsysccs/deploy
 Fabric will read all CDS files found in the above path and will load the EXT SCCs as defined in the docker compose yaml configs.

 Each EXT SCC must be defined under the peer's 'environment' map in the docker compose yaml file. Here's an example config:
      # EXTSCC1
      - CORE_CHAINCODE_SYSTEMEXT_EXTSCC1_ENABLED=true
      - CORE_CHAINCODE_SYSTEMEXT_EXTSCC1_EXECENV=DOCKER
      - CORE_CHAINCODE_SYSTEMEXT_EXTSCC1_INVOKABLEEXTERNAL=true
      - CORE_CHAINCODE_SYSTEMEXT_EXTSCC1_INVOKABLECC2CC=true
      - CORE_CHAINCODE_SYSTEMEXT_EXTSCC1_CONFIGPATH=/opt/extsysccs/config/extscc1


 All EXT SCC configuration entries must be prefixed with 'CORE_CHAINCODE_SYSTEMEXT_' following the SCC name (in the above example 'EXTSCC1') then following
 the conventional SCC's variables defined in SystemChaincode struct under $GOPATH/fabric/core/scc/sysccapi.go

 CONFIGPATH variable was added to enable loading specific configs for the EXT SCC via yaml/viper
 EXECENV variable defines the SCC execution environment - in the same container alongside the peer process (SYSTEM_EXT) or in a diffrent container (DOCKER)