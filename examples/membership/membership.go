/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/hyperledger/fabric/core/comm"
	"github.com/hyperledger/fabric/core/config"
	"github.com/hyperledger/fabric/core/scc/mscc/protos"
	"github.com/hyperledger/fabric/examples/membership/membership"
	"github.com/hyperledger/fabric/peer/common"
	"github.com/spf13/cobra"
)

const (
	defaultPeerAddress    = "localhost:7051"
	defaultMspID          = "DEFAULT"
	defaultPeerTLSEnabled = false
)

var peerAddress string
var peerTLSEnabled bool
var peerServerOverride string
var mspID string
var mspMgrConfigDir string
var peerTLSRootCertFile string
var channelID string

var mainCmd = &cobra.Command{
	Use: "",
	Run: func(cmd *cobra.Command, args []string) {
		if err := invoke(); err != nil {
			fmt.Println(err.Error())
		}
	},
}

func main() {
	mainFlags := mainCmd.PersistentFlags()

	defaultMspDir, err := config.GetDevMspDir()
	if err != nil {
		panic(err.Error())
	}
	mainFlags.StringVarP(&mspMgrConfigDir, "mspcfgdir", "m", defaultMspDir, "Path to MSP dir")
	mainFlags.StringVarP(&mspID, "mspid", "i", defaultMspID, "MSP ID")
	mainFlags.StringVarP(&peerAddress, "peer.address", "p", defaultPeerAddress, "Peer address in the form host:port")
	mainFlags.BoolVarP(&peerTLSEnabled, "peer.tls.enabled", "", defaultPeerTLSEnabled, "Peer TLS enabled")
	mainFlags.StringVarP(&peerServerOverride, "peer.tls.serverhostoverride", "", "", "Peer TLS server host override")
	mainFlags.StringVarP(&peerTLSRootCertFile, "peer.tls.rootcert.file", "", "", "Peer TLS root certificate file path")
	mainFlags.StringVarP(&channelID, "channel", "c", "", "Channel ID")

	if mainCmd.Execute() != nil {
		os.Exit(1)
	}
}

func invoke() error {
	err := common.InitCrypto(mspMgrConfigDir, mspID)
	if err != nil {
		return fmt.Errorf("Error initializing crypto: %s", err)
	}

	conn, err := newPeerConnection()
	if err != nil {
		return fmt.Errorf("Error connecting to peer: %s", err)
	}
	defer conn.Close()

	membershipClient := membership.NewClient(conn)

	if channelID == "" {
		allPeers, err := membershipClient.GetAllPeers()
		if err != nil {
			return fmt.Errorf("Error getting all peers: %s", err)
		}
		print(allPeers, "All peers:")
	} else {
		peersOfChannel, err := membershipClient.GetPeersOfChannel(channelID)
		if err != nil {
			return fmt.Errorf("Error getting peers for channel %s: %s", channelID, err)
		}
		print(peersOfChannel, "Peers for channel [%s]", channelID)
	}

	return nil
}

func newPeerConnection() (*grpc.ClientConn, error) {
	if !peerTLSEnabled {
		return comm.NewClientConnectionWithAddress(peerAddress, true, false, nil)
	}

	creds, err := credentials.NewClientTLSFromFile(peerTLSRootCertFile, peerServerOverride)
	if err != nil {
		return nil, fmt.Errorf("Failed to create TLS credentials %v", err)
	}
	return comm.NewClientConnectionWithAddress(peerAddress, true, true, creds)
}

func print(endpoints []*protos.PeerEndpoint, title string, args ...interface{}) {
	fmt.Printf(title, args...)
	fmt.Println()
	for _, peer := range endpoints {
		fmt.Printf("Peer: %s, MSP ID: %s\n", peer.Endpoint, peer.MSPid)
	}
}
