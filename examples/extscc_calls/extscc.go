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

	"strconv"

	"github.com/hyperledger/fabric/core/comm"
	"github.com/hyperledger/fabric/core/config"
	"github.com/hyperledger/fabric/core/scc/mscc/protos"
	"github.com/hyperledger/fabric/examples/extscc_calls/extscc"
	"github.com/hyperledger/fabric/peer/common"
	"github.com/op/go-logging"
	"github.com/spf13/cobra"
)

const (
	defaultPeerAddress    = "localhost:7051"
	defaultMspID          = "DEFAULT"
	defaultPeerTLSEnabled = false
	defaultChannelID      = "" //"mychannel"
	defaultInvokeBinary   = false
)

var peerAddress string
var peerTLSEnabled bool
var peerServerOverride string
var mspID string
var mspMgrConfigDir string
var peerTLSRootCertFile string
var channelID string
var isInvokeBin bool

var logger = logging.MustGetLogger("extscc-client-main")

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
	mainFlags.BoolVarP(&peerTLSEnabled, "peer.tls.enabled", "t", defaultPeerTLSEnabled, "Peer TLS enabled")
	mainFlags.StringVarP(&peerServerOverride, "peer.tls.serverhostoverride", "", "", "Peer TLS server host override")
	mainFlags.StringVarP(&peerTLSRootCertFile, "peer.tls.rootcert.file", "", "", "Peer TLS root certificate file path")
	mainFlags.StringVarP(&channelID, "channel", "c", defaultChannelID, "Channel ID")
	mainFlags.BoolVarP(&isInvokeBin, "invoke.binary", "b", defaultInvokeBinary, "Invoke Ext SCC binary or source code")

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

	extsccClient := extscc.NewClient(conn)
	args := [][]byte{[]byte("Hello from client")}
	args = append(args, []byte(channelID))
	args = append(args, []byte(strconv.FormatBool(isInvokeBin)))
	err = extsccClient.InvokeExtScc(args)
	if err != nil {
		return fmt.Errorf("Error invoking extScc caller: %s", err)
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
		logger.Infof("Peer: %s, MSP ID: %s\n", peer.Endpoint, peer.MSPid)
	}
}
