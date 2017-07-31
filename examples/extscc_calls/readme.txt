#
# extscc_calls examples demonstrate the invocation of external system chaincodes.
#
# extscc.go and extscc/client.go simulate a client that will invoke external SCC
# the example chaincodes used are:
#     1. in peer process open source external SCC called: extscc1 (source code is from: extscc_cc) invoked by extscccaller_cc
#     2. docker private binary ext SCC called: extscc3 (using same source code as above, but as a pre built binary) invoked by extscccaller_cc2
#
# All ext SCC source code files are found in this path:
cd $GOPATH/src/github.com/hyperledger/fabric/test/extscc/fixtures/src

# extscc_cc is the main example external chaincode. It will be invoked by other external CCs
# extscccaller_cc is invoking extscc1 deployed from source code (which is a copy of extscc_cc)
# extscccaller_cc2 is invoking extscc3 deployed from binary (which is also a copy of extscc_cc)
# all external SCCs are defined and configured in the docker compose yaml file found in the fixtures folder mentioned below.

#
# open a command shell window and navigate to the test docker compose yaml found at this path:
cd $GOPATH/src/github.com/hyperledger/fabric/test/extscc/fixtures

#
# start docker compose by running this command:
(source .env && docker-compose down && docker-compose up --force-recreate)

# Once peers are up, open another command shell
# and navigate to the path of this readme file to run the following examples:
cd $GOPATH/src/github.com/hyperledger/fabric/examples/extscc_calls

#
# to run extcaller_cc on peer0 for org1, execute the follwing command:
go run extscc.go -p localhost:7051 --peer.tls.rootcert.file $GOPATH/src/github.com/hyperledger/fabric-sdk-go/test/fixtures/channel/crypto-config/peerOrganizations/org1.example.com/peers/peer0.org1.example.com/cacerts/org1.example.com-cert.pem -i Org1MSP --peer.tls.serverhostoverride peer0.org1.example.com -m $GOPATH/src/github.com/hyperledger/fabric-sdk-go/test/fixtures/channel/crypto-config/peerOrganizations/org1.example.com/peers/peer0.org1.example.com --peer.tls.enabled=true

# this will run the client that will invoke extcaller_cc which will in turn invoke extscc1 without joining a channel.
# to invoke the external SCC on a already existing channel joined by peer0 add this argument to the command above:
--channel=mychannel
or
-c mychannel

# where 'mychannel' is the channel name joined by peer0.
#
# make sure to checkout fabrick-sdk-go project for the MSPs and certs needed by Fabric.
#
# to invoke the external SCC for org2, replace org1 by org2 in the command above and replace the peer port from 7051 to 8051 to communicate with peer1.
#
# to invoke extcaller_cc2 that invokes the binary extscc3 (simulating private ext scc), add this argument to the same command mentioned above:
--invoke.binary=true
or
-b true