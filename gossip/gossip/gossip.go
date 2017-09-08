/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package gossip

import (
	"crypto/tls"
	"time"

	"github.com/hyperledger/fabric/gossip/api"
	"github.com/hyperledger/fabric/gossip/comm"
	"github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/gossip/discovery"
	"github.com/hyperledger/fabric/gossip/filter"
	proto "github.com/hyperledger/fabric/protos/gossip"
)

// Gossip is the interface of the gossip component
type Gossip interface {

	// Send sends a message to remote peers
	Send(msg *proto.GossipMessage, peers ...*comm.RemotePeer)

	// SendByCriteria sends a given message to all peers that match the given SendCriteria
	SendByCriteria(*proto.SignedGossipMessage, SendCriteria) error

	// GetPeers returns the NetworkMembers considered alive
	Peers() []discovery.NetworkMember

	// PeersOfChannel returns the NetworkMembers considered alive
	// and also subscribed to the channel given
	PeersOfChannel(common.ChainID) []discovery.NetworkMember

	// UpdateMetadata updates the self metadata of the discovery layer
	// the peer publishes to other peers
	UpdateMetadata(metadata []byte)

	// UpdateChannelMetadata updates the self metadata the peer
	// publishes to other peers about its channel-related state
	UpdateChannelMetadata(metadata []byte, chainID common.ChainID)

	// Gossip sends a message to other peers to the network
	Gossip(msg *proto.GossipMessage)

	// PeerFilter receives a SubChannelSelectionCriteria and returns a RoutingFilter that selects
	// only peer identities that match the given criteria, and that they published their channel participation
	PeerFilter(channel common.ChainID, messagePredicate api.SubChannelSelectionCriteria) (filter.RoutingFilter, error)

	// Accept returns a dedicated read-only channel for messages sent by other nodes that match a certain predicate.
	// If passThrough is false, the messages are processed by the gossip layer beforehand.
	// If passThrough is true, the gossip layer doesn't intervene and the messages
	// can be used to send a reply back to the sender
	Accept(acceptor common.MessageAcceptor, passThrough bool) (<-chan *proto.GossipMessage, <-chan proto.ReceivedMessage)

	// JoinChan makes the Gossip instance join a channel
	JoinChan(joinMsg api.JoinChannelMessage, chainID common.ChainID)

	// SuspectPeers makes the gossip instance validate identities of suspected peers, and close
	// any connections to peers with identities that are found invalid
	SuspectPeers(s api.PeerSuspector)

	// Stop stops the gossip component
	Stop()

	// GetOrgOfPeer returns the organization for a peer, given the peer's PKI ID
	GetOrgOfPeer(PKIID common.PKIidType) api.OrgIdentityType
}

// SendCriteria defines how to send a specific message
type SendCriteria struct {
	Timeout    time.Duration        // Timeout defines the time to wait for acknowledgements
	MinAck     int                  // MinAck defines the amount of peers to collect acknowledgements from
	MaxPeers   int                  // MaxPeers defines the maximum number of peers to send the message to
	IsEligible filter.RoutingFilter // IsEligible defines whether a specific peer is eligible of receiving the message
	Channel    common.ChainID       // Channel specifies a channel to send this message on. \
	// Only peers that joined the channel would receive this message
}

// Config is the configuration of the gossip component
type Config struct {
	BindPort            int      // Port we bind to, used only for tests
	ID                  string   // ID of this instance
	BootstrapPeers      []string // Peers we connect to at startup
	PropagateIterations int      // Number of times a message is pushed to remote peers
	PropagatePeerNum    int      // Number of peers selected to push messages to

	MaxBlockCountToStore int // Maximum count of blocks we store in memory

	MaxPropagationBurstSize    int           // Max number of messages stored until it triggers a push to remote peers
	MaxPropagationBurstLatency time.Duration // Max time between consecutive message pushes

	PullInterval time.Duration // Determines frequency of pull phases
	PullPeerNum  int           // Number of peers to pull from

	SkipBlockVerification bool // Should we skip verifying block messages or not

	PublishCertPeriod        time.Duration    // Time from startup certificates are included in Alive messages
	PublishStateInfoInterval time.Duration    // Determines frequency of pushing state info messages to peers
	RequestStateInfoInterval time.Duration    // Determines frequency of pulling state info messages from peers
	TLSServerCert            *tls.Certificate // TLS certificate of the peer

	InternalEndpoint string // Endpoint we publish to peers in our organization
	ExternalEndpoint string // Peer publishes this endpoint instead of SelfEndpoint to foreign organizations
}
