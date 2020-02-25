/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package protoext

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/common/util"
	"github.com/hyperledger/fabric/gossip/api"
	"github.com/hyperledger/fabric/gossip/common"
	"github.com/hyperledger/fabric/protos/gossip"
)

// NewGossipMessageComparator creates a MessageReplacingPolicy given a maximum number of blocks to hold
func NewGossipMessageComparator(dataBlockStorageSize int) common.MessageReplacingPolicy {
	return (&msgComparator{dataBlockStorageSize: dataBlockStorageSize}).getMsgReplacingPolicy()
}

type msgComparator struct {
	dataBlockStorageSize int
}

func (mc *msgComparator) getMsgReplacingPolicy() common.MessageReplacingPolicy {
	return func(this interface{}, that interface{}) common.InvalidationResult {
		return mc.invalidationPolicy(this, that)
	}
}

func (mc *msgComparator) invalidationPolicy(this interface{}, that interface{}) common.InvalidationResult {
	thisMsg := this.(*SignedGossipMessage)
	thatMsg := that.(*SignedGossipMessage)

	if IsAliveMsg(thisMsg.GossipMessage) && IsAliveMsg(thatMsg.GossipMessage) {
		return aliveInvalidationPolicy(thisMsg.GetAliveMsg(), thatMsg.GetAliveMsg())
	}

	if IsDataMsg(thisMsg.GossipMessage) && IsDataMsg(thatMsg.GossipMessage) {
		return mc.dataInvalidationPolicy(thisMsg.GetDataMsg(), thatMsg.GetDataMsg())
	}

	if IsStateInfoMsg(thisMsg.GossipMessage) && IsStateInfoMsg(thatMsg.GossipMessage) {
		return mc.stateInvalidationPolicy(thisMsg.GetStateInfo(), thatMsg.GetStateInfo())
	}

	if IsIdentityMsg(thisMsg.GossipMessage) && IsIdentityMsg(thatMsg.GossipMessage) {
		return mc.identityInvalidationPolicy(thisMsg.GetPeerIdentity(), thatMsg.GetPeerIdentity())
	}

	if IsLeadershipMsg(thisMsg.GossipMessage) && IsLeadershipMsg(thatMsg.GossipMessage) {
		return leaderInvalidationPolicy(thisMsg.GetLeadershipMsg(), thatMsg.GetLeadershipMsg())
	}

	return common.MessageNoAction
}

func (mc *msgComparator) stateInvalidationPolicy(thisStateMsg *gossip.StateInfo, thatStateMsg *gossip.StateInfo) common.InvalidationResult {
	if !bytes.Equal(thisStateMsg.PkiId, thatStateMsg.PkiId) {
		return common.MessageNoAction
	}
	return compareTimestamps(thisStateMsg.Timestamp, thatStateMsg.Timestamp)
}

func (mc *msgComparator) identityInvalidationPolicy(thisIdentityMsg *gossip.PeerIdentity, thatIdentityMsg *gossip.PeerIdentity) common.InvalidationResult {
	if bytes.Equal(thisIdentityMsg.PkiId, thatIdentityMsg.PkiId) {
		return common.MessageInvalidated
	}

	return common.MessageNoAction
}

func (mc *msgComparator) dataInvalidationPolicy(thisDataMsg *gossip.DataMessage, thatDataMsg *gossip.DataMessage) common.InvalidationResult {
	if thisDataMsg.Payload.SeqNum == thatDataMsg.Payload.SeqNum {
		return common.MessageInvalidated
	}

	diff := abs(thisDataMsg.Payload.SeqNum, thatDataMsg.Payload.SeqNum)
	if diff <= uint64(mc.dataBlockStorageSize) {
		return common.MessageNoAction
	}

	if thisDataMsg.Payload.SeqNum > thatDataMsg.Payload.SeqNum {
		return common.MessageInvalidates
	}
	return common.MessageInvalidated
}

func aliveInvalidationPolicy(thisMsg *gossip.AliveMessage, thatMsg *gossip.AliveMessage) common.InvalidationResult {
	if !bytes.Equal(thisMsg.Membership.PkiId, thatMsg.Membership.PkiId) {
		return common.MessageNoAction
	}

	return compareTimestamps(thisMsg.Timestamp, thatMsg.Timestamp)
}

func leaderInvalidationPolicy(thisMsg *gossip.LeadershipMessage, thatMsg *gossip.LeadershipMessage) common.InvalidationResult {
	if !bytes.Equal(thisMsg.PkiId, thatMsg.PkiId) {
		return common.MessageNoAction
	}

	return compareTimestamps(thisMsg.Timestamp, thatMsg.Timestamp)
}

func compareTimestamps(thisTS *gossip.PeerTime, thatTS *gossip.PeerTime) common.InvalidationResult {
	if thisTS.IncNum == thatTS.IncNum {
		if thisTS.SeqNum > thatTS.SeqNum {
			return common.MessageInvalidates
		}

		return common.MessageInvalidated
	}
	if thisTS.IncNum < thatTS.IncNum {
		return common.MessageInvalidated
	}
	return common.MessageInvalidates
}

// IsAliveMsg returns whether this GossipMessage is an AliveMessage
func IsAliveMsg(m *gossip.GossipMessage) bool {
	return m.GetAliveMsg() != nil
}

// IsDataMsg returns whether this GossipMessage is a data message
func IsDataMsg(m *gossip.GossipMessage) bool {
	return m.GetDataMsg() != nil
}

// IsStateInfoPullRequestMsg returns whether this GossipMessage is a stateInfoPullRequest
func IsStateInfoPullRequestMsg(m *gossip.GossipMessage) bool {
	return m.GetStateInfoPullReq() != nil
}

// IsStateInfoSnapshot returns whether this GossipMessage is a stateInfo snapshot
func IsStateInfoSnapshot(m *gossip.GossipMessage) bool {
	return m.GetStateSnapshot() != nil
}

// IsStateInfoMsg returns whether this GossipMessage is a stateInfo message
func IsStateInfoMsg(m *gossip.GossipMessage) bool {
	return m.GetStateInfo() != nil
}

// IsPullMsg returns whether this GossipMessage is a message that belongs
// to the pull mechanism
func IsPullMsg(m *gossip.GossipMessage) bool {
	return m.GetDataReq() != nil || m.GetDataUpdate() != nil ||
		m.GetHello() != nil || m.GetDataDig() != nil
}

// IsRemoteStateMessage returns whether this GossipMessage is related to state synchronization
func IsRemoteStateMessage(m *gossip.GossipMessage) bool {
	return m.GetStateRequest() != nil || m.GetStateResponse() != nil
}

// GetPullMsgType returns the phase of the pull mechanism this GossipMessage belongs to
// for example: Hello, Digest, etc.
// If this isn't a pull message, PullMsgType_UNDEFINED is returned.
func GetPullMsgType(m *gossip.GossipMessage) gossip.PullMsgType {
	if helloMsg := m.GetHello(); helloMsg != nil {
		return helloMsg.MsgType
	}

	if digMsg := m.GetDataDig(); digMsg != nil {
		return digMsg.MsgType
	}

	if reqMsg := m.GetDataReq(); reqMsg != nil {
		return reqMsg.MsgType
	}

	if resMsg := m.GetDataUpdate(); resMsg != nil {
		return resMsg.MsgType
	}

	return gossip.PullMsgType_UNDEFINED
}

// IsChannelRestricted returns whether this GossipMessage should be routed
// only in its channel
func IsChannelRestricted(m *gossip.GossipMessage) bool {
	return m.Tag == gossip.GossipMessage_CHAN_AND_ORG || m.Tag == gossip.GossipMessage_CHAN_ONLY || m.Tag == gossip.GossipMessage_CHAN_OR_ORG
}

// IsOrgRestricted returns whether this GossipMessage should be routed only
// inside the organization
func IsOrgRestricted(m *gossip.GossipMessage) bool {
	return m.Tag == gossip.GossipMessage_CHAN_AND_ORG || m.Tag == gossip.GossipMessage_ORG_ONLY
}

// IsIdentityMsg returns whether this GossipMessage is an identity message
func IsIdentityMsg(m *gossip.GossipMessage) bool {
	return m.GetPeerIdentity() != nil
}

// IsDataReq returns whether this GossipMessage is a data request message
func IsDataReq(m *gossip.GossipMessage) bool {
	return m.GetDataReq() != nil
}

// IsPrivateDataMsg returns whether this message is related to private data
func IsPrivateDataMsg(m *gossip.GossipMessage) bool {
	return m.GetPrivateReq() != nil || m.GetPrivateRes() != nil || m.GetPrivateData() != nil
}

// IsAck returns whether this GossipMessage is an acknowledgement
func IsAck(m *gossip.GossipMessage) bool {
	return m.GetAck() != nil
}

// IsDataUpdate returns whether this GossipMessage is a data update message
func IsDataUpdate(m *gossip.GossipMessage) bool {
	return m.GetDataUpdate() != nil
}

// IsHelloMsg returns whether this GossipMessage is a hello message
func IsHelloMsg(m *gossip.GossipMessage) bool {
	return m.GetHello() != nil
}

// IsDigestMsg returns whether this GossipMessage is a digest message
func IsDigestMsg(m *gossip.GossipMessage) bool {
	return m.GetDataDig() != nil
}

// IsLeadershipMsg returns whether this GossipMessage is a leadership (leader election) message
func IsLeadershipMsg(m *gossip.GossipMessage) bool {
	return m.GetLeadershipMsg() != nil
}

// MsgConsumer invokes code given a SignedGossipMessage
type MsgConsumer func(message *SignedGossipMessage)

// IdentifierExtractor extracts from a SignedGossipMessage an identifier
type IdentifierExtractor func(*SignedGossipMessage) string

// IsTagLegal checks the GossipMessage tags and inner type
// and returns an error if the tag doesn't match the type.
func IsTagLegal(m *gossip.GossipMessage) error {
	if m.Tag == gossip.GossipMessage_UNDEFINED {
		return fmt.Errorf("Undefined tag")
	}
	if IsDataMsg(m) {
		if m.Tag != gossip.GossipMessage_CHAN_AND_ORG {
			return fmt.Errorf("Tag should be %s", gossip.GossipMessage_Tag_name[int32(gossip.GossipMessage_CHAN_AND_ORG)])
		}
		return nil
	}

	if IsAliveMsg(m) || m.GetMemReq() != nil || m.GetMemRes() != nil {
		if m.Tag != gossip.GossipMessage_EMPTY {
			return fmt.Errorf("Tag should be %s", gossip.GossipMessage_Tag_name[int32(gossip.GossipMessage_EMPTY)])
		}
		return nil
	}

	if IsIdentityMsg(m) {
		if m.Tag != gossip.GossipMessage_ORG_ONLY {
			return fmt.Errorf("Tag should be %s", gossip.GossipMessage_Tag_name[int32(gossip.GossipMessage_ORG_ONLY)])
		}
		return nil
	}

	if IsPullMsg(m) {
		switch GetPullMsgType(m) {
		case gossip.PullMsgType_BLOCK_MSG:
			if m.Tag != gossip.GossipMessage_CHAN_AND_ORG {
				return fmt.Errorf("Tag should be %s", gossip.GossipMessage_Tag_name[int32(gossip.GossipMessage_CHAN_AND_ORG)])
			}
			return nil
		case gossip.PullMsgType_IDENTITY_MSG:
			if m.Tag != gossip.GossipMessage_EMPTY {
				return fmt.Errorf("Tag should be %s", gossip.GossipMessage_Tag_name[int32(gossip.GossipMessage_EMPTY)])
			}
			return nil
		default:
			return fmt.Errorf("Invalid PullMsgType: %s", gossip.PullMsgType_name[int32(GetPullMsgType(m))])
		}
	}

	if IsStateInfoMsg(m) || IsStateInfoPullRequestMsg(m) || IsStateInfoSnapshot(m) || IsRemoteStateMessage(m) {
		if m.Tag != gossip.GossipMessage_CHAN_OR_ORG {
			return fmt.Errorf("Tag should be %s", gossip.GossipMessage_Tag_name[int32(gossip.GossipMessage_CHAN_OR_ORG)])
		}
		return nil
	}

	if IsLeadershipMsg(m) {
		if m.Tag != gossip.GossipMessage_CHAN_AND_ORG {
			return fmt.Errorf("Tag should be %s", gossip.GossipMessage_Tag_name[int32(gossip.GossipMessage_CHAN_AND_ORG)])
		}
		return nil
	}

	if m.GetCollDataReq() != nil || m.GetCollDataRes() != nil {
		if m.Tag != gossip.GossipMessage_CHAN_ONLY {
			return fmt.Errorf("Tag should be %s", gossip.GossipMessage_Tag_name[int32(gossip.GossipMessage_CHAN_ONLY)])
		}
		return nil
	}

	return fmt.Errorf("Unknown message type: %v", m)
}

// Verifier receives a peer identity, a signature and a message
// and returns nil if the signature on the message could be verified
// using the given identity.
type Verifier func(peerIdentity []byte, signature, message []byte) error

// Signer signs a message, and returns (signature, nil)
// on success, and nil and an error on failure.
type Signer func(msg []byte) ([]byte, error)

// ReceivedMessage is a GossipMessage wrapper that
// enables the user to send a message to the origin from which
// the ReceivedMessage was sent from.
// It also allows to know the identity of the sender,
// to obtain the raw bytes the GossipMessage was un-marshaled from,
// and the signature over these raw bytes.
type ReceivedMessage interface {

	// Respond sends a GossipMessage to the origin from which this ReceivedMessage was sent from
	Respond(msg *gossip.GossipMessage)

	// GetGossipMessage returns the underlying GossipMessage
	GetGossipMessage() *SignedGossipMessage

	// GetSourceMessage Returns the Envelope the ReceivedMessage was
	// constructed with
	GetSourceEnvelope() *gossip.Envelope

	// GetConnectionInfo returns information about the remote peer
	// that sent the message
	GetConnectionInfo() *ConnectionInfo

	// Ack returns to the sender an acknowledgement for the message
	// An ack can receive an error that indicates that the operation related
	// to the message has failed
	Ack(err error)
}

// ConnectionInfo represents information about
// the remote peer that sent a certain ReceivedMessage
type ConnectionInfo struct {
	ID       common.PKIidType
	Auth     *AuthInfo
	Identity api.PeerIdentityType
	Endpoint string
}

// String returns a string representation of this ConnectionInfo
func (c *ConnectionInfo) String() string {
	return fmt.Sprintf("%s %v", c.Endpoint, c.ID)
}

// AuthInfo represents the authentication
// data that was provided by the remote peer
// at the connection time
type AuthInfo struct {
	SignedData []byte
	Signature  []byte
}

// Sign signs a GossipMessage with given Signer.
// Returns an Envelope on success,
// panics on failure.
func (m *SignedGossipMessage) Sign(signer Signer) (*gossip.Envelope, error) {
	// If we have a secretEnvelope, don't override it.
	// Back it up, and restore it later
	var secretEnvelope *gossip.SecretEnvelope
	if m.Envelope != nil {
		secretEnvelope = m.Envelope.SecretEnvelope
	}
	m.Envelope = nil
	payload, err := proto.Marshal(m.GossipMessage)
	if err != nil {
		return nil, err
	}
	sig, err := signer(payload)
	if err != nil {
		return nil, err
	}

	e := &gossip.Envelope{
		Payload:        payload,
		Signature:      sig,
		SecretEnvelope: secretEnvelope,
	}
	m.Envelope = e
	return e, nil
}

// NoopSign creates a SignedGossipMessage with a nil signature
func NoopSign(m *gossip.GossipMessage) (*SignedGossipMessage, error) {
	signer := func(msg []byte) ([]byte, error) {
		return nil, nil
	}
	sMsg := &SignedGossipMessage{
		GossipMessage: m,
	}
	_, err := sMsg.Sign(signer)
	return sMsg, err
}

// Verify verifies a signed GossipMessage with a given Verifier.
// Returns nil on success, error on failure.
func (m *SignedGossipMessage) Verify(peerIdentity []byte, verify Verifier) error {
	if m.Envelope == nil {
		return errors.New("Missing envelope")
	}
	if len(m.Envelope.Payload) == 0 {
		return errors.New("Empty payload")
	}
	if len(m.Envelope.Signature) == 0 {
		return errors.New("Empty signature")
	}
	payloadSigVerificationErr := verify(peerIdentity, m.Envelope.Signature, m.Envelope.Payload)
	if payloadSigVerificationErr != nil {
		return payloadSigVerificationErr
	}
	if m.Envelope.SecretEnvelope != nil {
		payload := m.Envelope.SecretEnvelope.Payload
		sig := m.Envelope.SecretEnvelope.Signature
		if len(payload) == 0 {
			return errors.New("Empty payload")
		}
		if len(sig) == 0 {
			return errors.New("Empty signature")
		}
		return verify(peerIdentity, sig, payload)
	}
	return nil
}

// IsSigned returns whether the message
// has a signature in the envelope.
func (m *SignedGossipMessage) IsSigned() bool {
	return m.Envelope != nil && m.Envelope.Payload != nil && m.Envelope.Signature != nil
}

// EnvelopeToGossipMessage un-marshals a given envelope and creates a
// SignedGossipMessage out of it.
// Returns an error if un-marshaling fails.
func EnvelopeToGossipMessage(e *gossip.Envelope) (*SignedGossipMessage, error) {
	if e == nil {
		return nil, errors.New("nil envelope")
	}
	msg := &gossip.GossipMessage{}
	err := proto.Unmarshal(e.Payload, msg)
	if err != nil {
		return nil, fmt.Errorf("Failed unmarshaling GossipMessage from envelope: %v", err)
	}
	return &SignedGossipMessage{
		GossipMessage: msg,
		Envelope:      e,
	}, nil
}

// SignSecret signs the secret payload and creates
// a secret envelope out of it.
func SignSecret(e *gossip.Envelope, signer Signer, secret *gossip.Secret) error {
	payload, err := proto.Marshal(secret)
	if err != nil {
		return err
	}
	sig, err := signer(payload)
	if err != nil {
		return err
	}
	e.SecretEnvelope = &gossip.SecretEnvelope{
		Payload:   payload,
		Signature: sig,
	}
	return nil
}

// InternalEndpoint returns the internal endpoint
// in the secret envelope, or an empty string
// if a failure occurs.
func InternalEndpoint(s *gossip.SecretEnvelope) string {
	if s == nil {
		return ""
	}
	secret := &gossip.Secret{}
	if err := proto.Unmarshal(s.Payload, secret); err != nil {
		return ""
	}
	return secret.GetInternalEndpoint()
}

// SignedGossipMessage contains a GossipMessage
// and the Envelope from which it came from
type SignedGossipMessage struct {
	*gossip.Envelope
	*gossip.GossipMessage
}

// String returns a string representation
// of a SignedGossipMessage
func (m *SignedGossipMessage) String() string {
	env := "No envelope"
	if m.Envelope != nil {
		var secretEnv string
		if m.SecretEnvelope != nil {
			pl := len(m.SecretEnvelope.Payload)
			sl := len(m.SecretEnvelope.Signature)
			secretEnv = fmt.Sprintf(" Secret payload: %d bytes, Secret Signature: %d bytes", pl, sl)
		}
		env = fmt.Sprintf("%d bytes, Signature: %d bytes%s", len(m.Envelope.Payload), len(m.Envelope.Signature), secretEnv)
	}
	gMsg := "No gossipMessage"
	if m.GossipMessage != nil {
		var isSimpleMsg bool
		if m.GetStateResponse() != nil {
			gMsg = fmt.Sprintf("StateResponse with %d items", len(m.GetStateResponse().Payloads))
		} else if IsDataMsg(m.GossipMessage) && m.GetDataMsg().Payload != nil {
			gMsg = PayloadToString(m.GetDataMsg().Payload)
		} else if IsDataUpdate(m.GossipMessage) {
			update := m.GetDataUpdate()
			gMsg = fmt.Sprintf("DataUpdate: %s", DataUpdateToString(update))
		} else if m.GetMemRes() != nil {
			gMsg = MembershipResponseToString(m.GetMemRes())
		} else if IsStateInfoSnapshot(m.GossipMessage) {
			gMsg = StateInfoSnapshotToString(m.GetStateSnapshot())
		} else if m.GetPrivateRes() != nil {
			gMsg = RemovePvtDataResponseToString(m.GetPrivateRes())
		} else if m.GetAliveMsg() != nil {
			gMsg = AliveMessageToString(m.GetAliveMsg())
		} else if m.GetMemReq() != nil {
			gMsg = MembershipRequestToString(m.GetMemReq())
		} else if m.GetStateInfoPullReq() != nil {
			gMsg = StateInfoPullRequestToString(m.GetStateInfoPullReq())
		} else if m.GetStateInfo() != nil {
			gMsg = StateInfoToString(m.GetStateInfo())
		} else if m.GetDataDig() != nil {
			gMsg = DataDigestToString(m.GetDataDig())
		} else if m.GetDataReq() != nil {
			gMsg = DataRequestToString(m.GetDataReq())
		} else if m.GetLeadershipMsg() != nil {
			gMsg = LeadershipMessageToString(m.GetLeadershipMsg())
		} else {
			gMsg = m.GossipMessage.String()
			isSimpleMsg = true
		}
		if !isSimpleMsg {
			desc := fmt.Sprintf("Channel: %s, nonce: %d, tag: %s", string(m.Channel), m.Nonce, gossip.GossipMessage_Tag_name[int32(m.Tag)])
			gMsg = fmt.Sprintf("%s %s", desc, gMsg)
		}
	}
	return fmt.Sprintf("GossipMessage: %v, Envelope: %s", gMsg, env)
}

func FormattedDigestsFromDataRequest(dd *gossip.DataRequest) []string {
	if dd.MsgType == gossip.PullMsgType_IDENTITY_MSG {
		return digestsToHex(dd.Digests)
	}

	return digestsAsStrings(dd.Digests)
}

func FormattedDigests(dd *gossip.DataDigest) []string {
	if dd.MsgType == gossip.PullMsgType_IDENTITY_MSG {
		return digestsToHex(dd.Digests)
	}

	return digestsAsStrings(dd.Digests)
}

// Hash returns the SHA256 representation of the PvtDataDigest's bytes
func Hash(dig *gossip.PvtDataDigest) (string, error) {
	b, err := proto.Marshal(dig)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(util.ComputeSHA256(b)), nil
}

// ToString returns a string representation of this RemotePvtDataResponse
func ToString(res *gossip.RemotePvtDataResponse) string {
	a := make([]string, len(res.Elements))
	for i, el := range res.Elements {
		a[i] = fmt.Sprintf("%s with %d elements", el.Digest.String(), len(el.Payload))
	}
	return fmt.Sprintf("%v", a)
}

func digestsAsStrings(digests [][]byte) []string {
	a := make([]string, len(digests))
	for i, dig := range digests {
		a[i] = string(dig)
	}
	return a
}

func digestsToHex(digests [][]byte) []string {
	a := make([]string, len(digests))
	for i, dig := range digests {
		a[i] = hex.EncodeToString(dig)
	}
	return a
}

// LedgerHeight returns the ledger height that is specified
// in the StateInfo message
func LedgerHeight(msg *gossip.StateInfo) (uint64, error) {
	if msg.Properties != nil {
		return msg.Properties.LedgerHeight, nil
	}
	return 0, errors.New("properties undefined")
}

// Abs returns abs(a-b)
func abs(a, b uint64) uint64 {
	if a > b {
		return a - b
	}
	return b - a
}
