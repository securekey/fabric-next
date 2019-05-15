/*
Copyright SecureKey Technologies Inc. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package dispatcher

import (
	"time"

	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/core/common/privdata"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/extensions/collections/api/store"
	storeapi "github.com/hyperledger/fabric/extensions/collections/api/store"
	"github.com/hyperledger/fabric/extensions/common"
	"github.com/hyperledger/fabric/extensions/common/discovery"
	"github.com/hyperledger/fabric/extensions/common/requestmgr"
	supp "github.com/hyperledger/fabric/extensions/common/support"
	kgossipapi "github.com/hyperledger/fabric/extensions/gossip/api"
	gossipapi "github.com/hyperledger/fabric/gossip/api"
	gcommon "github.com/hyperledger/fabric/gossip/common"
	gdiscovery "github.com/hyperledger/fabric/gossip/discovery"
	cb "github.com/hyperledger/fabric/protos/common"
	gproto "github.com/hyperledger/fabric/protos/gossip"
	proto "github.com/hyperledger/fabric/protos/gossip"
	"github.com/pkg/errors"
	"go.uber.org/zap/zapcore"
)

var logger = flogging.MustGetLogger("kevlar_gossip_state")

type gossipAdapter interface {
	PeersOfChannel(gcommon.ChainID) []gdiscovery.NetworkMember
	SelfMembershipInfo() gdiscovery.NetworkMember
	IdentityInfo() gossipapi.PeerIdentitySet
}

type blockPublisher interface {
	AddCCUpgradeHandler(handler kgossipapi.ChaincodeUpgradeHandler)
}

type ccRetriever interface {
	Config(ns, coll string) (*cb.StaticCollectionConfig, error)
	Policy(ns, coll string) (privdata.CollectionAccessPolicy, error)
}

// New returns a new Gossip message dispatcher
func New(
	channelID string,
	dataStore storeapi.Store,
	gossipAdapter gossipAdapter,
	ledger ledger.PeerLedger,
	blockPublisher blockPublisher) *Dispatcher {
	return &Dispatcher{
		ccRetriever: supp.NewCollectionConfigRetriever(channelID, ledger, blockPublisher),
		channelID:   channelID,
		reqMgr:      requestmgr.Get(channelID),
		dataStore:   dataStore,
		discovery:   discovery.New(channelID, gossipAdapter),
	}
}

// Dispatcher is a Gossip message dispatcher
type Dispatcher struct {
	ccRetriever
	channelID string
	reqMgr    requestmgr.RequestMgr
	dataStore storeapi.Store
	discovery *discovery.Discovery
}

// Dispatch handles the message and returns true if the message was handled; false if the message is unrecognized
func (s *Dispatcher) Dispatch(msg proto.ReceivedMessage) bool {
	switch {
	case msg.GetGossipMessage().GetCollDataReq() != nil:
		logger.Debug("Handling collection data request message")
		s.handleDataRequest(msg)
		return true
	case msg.GetGossipMessage().GetCollDataRes() != nil:
		logger.Debug("Handling collection data response message")
		s.handleDataResponse(msg)
		return true
	default:
		logger.Debug("Not handling msg")
		return false
	}
}

func (s *Dispatcher) handleDataRequest(msg proto.ReceivedMessage) {
	if logger.IsEnabledFor(zapcore.DebugLevel) {
		logger.Debugf("[ENTER] -> handleDataRequest")
		defer logger.Debug("[EXIT] ->  handleDataRequest")
	}

	req := msg.GetGossipMessage().GetCollDataReq()
	if len(req.Digests) == 0 {
		logger.Warning("Got nil digests in CollDataRequestMsg")
		return
	}

	reqMSPID, ok := s.discovery.GetMSPID(msg.GetConnectionInfo().ID)
	if !ok {
		logger.Warningf("Unable to get MSP ID from PKI ID of remote endpoint [%s]", msg.GetConnectionInfo().Endpoint)
		return
	}

	responses, err := s.getData(reqMSPID, req)
	if err != nil {
		logger.Warning("[%s] Error processing request for data: %s", s.channelID, err.Error())
		return
	}

	logger.Debugf("[%s] Responding with collection data for request %d", s.channelID, req.Nonce)

	msg.Respond(&gproto.GossipMessage{
		// Copy nonce field from the request, so it will be possible to match response
		Nonce:   msg.GetGossipMessage().Nonce,
		Tag:     gproto.GossipMessage_CHAN_ONLY,
		Channel: []byte(s.channelID),
		Content: &gproto.GossipMessage_CollDataRes{
			CollDataRes: &gproto.RemoteCollDataResponse{
				Nonce:    req.Nonce,
				Elements: responses,
			},
		},
	})
}

func (s *Dispatcher) handleDataResponse(msg proto.ReceivedMessage) {
	if logger.IsEnabledFor(zapcore.DebugLevel) {
		logger.Debug("[ENTER] -> handleDataResponse")
		defer logger.Debug("[EXIT] ->  handleDataResponse")
	}

	res := msg.GetGossipMessage().GetCollDataRes()

	mspID, ok := s.discovery.GetMSPID(msg.GetConnectionInfo().ID)
	if !ok {
		logger.Errorf("Unable to get MSP ID from PKI ID")
		return
	}

	var elements []*requestmgr.Element
	for _, e := range res.Elements {
		d := e.Digest
		logger.Debugf("[%s] Coll data response for request %d - [%s:%s:%s] received", s.channelID, res.Nonce, d.Namespace, d.Collection, d.Key)

		element := &requestmgr.Element{
			Namespace:  d.Namespace,
			Collection: d.Collection,
			Key:        d.Key,
			Value:      e.Value,
		}

		if e.ExpiryTime != nil {
			element.Expiry = time.Unix(e.ExpiryTime.Seconds, 0)
		}

		elements = append(elements, element)
	}

	s.reqMgr.Respond(
		res.Nonce,
		&requestmgr.Response{
			Endpoint: msg.GetConnectionInfo().Endpoint,
			MSPID:    mspID,
			// FIXME: Should the message be signed?
			//Signature:   element.Signature,
			//Identity:    element.Identity,
			Data: elements,
		},
	)
}

func (s *Dispatcher) getData(reqMSPID string, req *gproto.RemoteCollDataRequest) ([]*gproto.CollDataElement, error) {
	var responses []*gproto.CollDataElement
	for _, digest := range req.Digests {
		if digest == nil {
			return nil, errors.New("got nil digest in CollDataRequestMsg")
		}

		key := store.NewKey(digest.EndorsedAtTxID, digest.Namespace, digest.Collection, digest.Key)

		e := &gproto.CollDataElement{
			Digest: digest,
		}

		authorized, err := s.isAuthorized(reqMSPID, digest.Namespace, digest.Collection)
		if err != nil {
			return nil, err
		}

		if authorized {
			logger.Debugf("[%s] Getting data for key [%s]", s.channelID, key)
			value, err := s.getDataForKey(key)
			if err != nil {
				return nil, errors.Wrapf(err, "error getting data for [%s]", key)
			}
			if value != nil {
				e.Value = value.Value
				e.ExpiryTime = common.ToTimestamp(value.Expiry)
			}
		} else {
			logger.Infof("[%s] Requesting MSP [%s] is not authorized to read data for key [%s]", s.channelID, reqMSPID, key)
		}

		responses = append(responses, e)
	}
	return responses, nil
}

func (s *Dispatcher) getDataForKey(key *storeapi.Key) (*storeapi.ExpiringValue, error) {
	logger.Debugf("[%s] Getting config for [%s:%s]", s.channelID, key.Namespace, key.Collection)
	config, err := s.Config(key.Namespace, key.Collection)
	if err != nil {
		return nil, err
	}

	switch config.Type {
	case cb.CollectionType_COL_TRANSIENT:
		logger.Debugf("[%s] Getting transient data for key [%s]", s.channelID, key)
		return s.dataStore.GetTransientData(key)
	case cb.CollectionType_COL_DCAS:
		fallthrough
	case cb.CollectionType_COL_OFFLEDGER:
		logger.Debugf("[%s] Getting off-ledger data for key [%s]", s.channelID, key)
		return s.dataStore.GetData(key)
	default:
		return nil, errors.Errorf("unsupported collection type: [%s]", config.Type)
	}
}

// isAuthorized determines whether the given MSP ID is authorized to read data from the given collection
func (s *Dispatcher) isAuthorized(mspID string, ns, coll string) (bool, error) {
	policy, err := s.Policy(ns, coll)
	if err != nil {
		return false, errors.Wrapf(err, "unable to get policy for collection [%s:%s]", ns, coll)
	}

	for _, memberMSPID := range policy.MemberOrgs() {
		if memberMSPID == mspID {
			return true, nil
		}
	}

	return false, nil
}
