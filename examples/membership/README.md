# Membership Example Client

The Membership client connects to a peer and invokes the Membership system chaincode to retrieve informaton about the peer's membership.

# To Run

To retrieve all peers connected to the local peer:

go run membership.go -p localhost:7051

To retrieve all peers connected to the local peer and joined to mychannel:

go run membership.go -p localhost:7051 -c mychannel

To list all available command-line options:

go run membership.go --help
