/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package peer

import (
	"fmt"
	"net"
	"runtime"
	"sync"

	"github.com/hyperledger/fabric/common/channelconfig"
	cc "github.com/hyperledger/fabric/common/config"
	"github.com/hyperledger/fabric/common/configtx"
	"github.com/hyperledger/fabric/common/deliver"
	"github.com/hyperledger/fabric/common/flogging"
	commonledger "github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/common/ledger/blockledger"
	fileledger "github.com/hyperledger/fabric/common/ledger/blockledger/file"
	"github.com/hyperledger/fabric/common/metrics"
	"github.com/hyperledger/fabric/common/policies"
	"github.com/hyperledger/fabric/core/chaincode/platforms"
	"github.com/hyperledger/fabric/core/comm"
	"github.com/hyperledger/fabric/core/committer"
	"github.com/hyperledger/fabric/core/committer/txvalidator"
	"github.com/hyperledger/fabric/core/common/ccprovider"
	"github.com/hyperledger/fabric/core/common/privdata"
	"github.com/hyperledger/fabric/core/common/sysccprovider"
	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/customtx"
	"github.com/hyperledger/fabric/core/ledger/ledgermgmt"
	"github.com/hyperledger/fabric/core/transientstore"
	storeapi "github.com/hyperledger/fabric/extensions/collections/api/store"
	"github.com/hyperledger/fabric/extensions/collections/storeprovider"
	"github.com/hyperledger/fabric/extensions/gossip/blockpublisher"
	"github.com/hyperledger/fabric/gossip/api"
	"github.com/hyperledger/fabric/gossip/service"
	"github.com/hyperledger/fabric/msp"
	mspmgmt "github.com/hyperledger/fabric/msp/mgmt"
	"github.com/hyperledger/fabric/protos/common"
	pb "github.com/hyperledger/fabric/protos/peer"
	"github.com/hyperledger/fabric/protos/utils"
	"github.com/hyperledger/fabric/token/tms/manager"
	"github.com/hyperledger/fabric/token/transaction"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"golang.org/x/sync/semaphore"
)

var peerLogger = flogging.MustGetLogger("peer")

var peerServers []*comm.GRPCServer

var configTxProcessor = newConfigTxProcessor()
var tokenTxProcessor = &transaction.Processor{
	TMSManager: &manager.Manager{
		IdentityDeserializerManager: &manager.FabricIdentityDeserializerManager{}}}
var ConfigTxProcessors = customtx.Processors{
	common.HeaderType_CONFIG:            configTxProcessor,
	common.HeaderType_TOKEN_TRANSACTION: tokenTxProcessor,
}

// singleton instance to manage credentials for the peer across channel config changes
var credSupport = comm.GetCredentialSupport()

//go:generate mockery -dir . -name OrdererOrg -case underscore -output mocks/

// OrdererOrg stores the per org orderer config.
type OrdererOrg interface {
	channelconfig.Org

	// Endpoints returns the endpoints of orderer nodes.
	Endpoints() []string
}

type gossipChannelConfig struct {
	ac channelconfig.Application
	oc channelconfig.Orderer
	configtx.Validator
	channelconfig.Channel
}

func (gcp *gossipChannelConfig) ApplicationOrgs() service.ApplicationOrgs {
	return gcp.ac.Organizations()
}

func (gcp *gossipChannelConfig) OrdererOrgs() []string {
	var res []string
	for _, org := range gcp.oc.Organizations() {
		res = append(res, org.MSPID())
	}
	return res
}

func (gcp *gossipChannelConfig) OrdererAddressesByOrgs() map[string][]string {
	res := make(map[string][]string)
	for _, ordererOrg := range gcp.oc.Organizations() {
		if len(ordererOrg.Endpoints()) == 0 {
			continue
		}
		res[ordererOrg.MSPID()] = ordererOrg.Endpoints()
	}
	return res
}

type chainSupport struct {
	bundleSource *channelconfig.BundleSource
	channelconfig.Resources
	channelconfig.Application
	ledger ledger.PeerLedger
}

var TransientStoreFactory = &storeProvider{stores: make(map[string]transientstore.Store)}

var collectionDataStoreFactory CollStoreProvider
var initCollDataStoreFactoryOnce sync.Once

// CollStoreProvider manages the collection stores for multiple channels
type CollStoreProvider interface {
	StoreForChannel(channelID string) storeapi.Store
	OpenStore(channelID string) (storeapi.Store, error)
}

// CollectionDataStoreFactory returns transient data stores by channel ID
func CollectionDataStoreFactory() CollStoreProvider {
	initCollDataStoreFactoryOnce.Do(func() {
		collectionDataStoreFactory = storeprovider.NewProviderFactory()
	})
	return collectionDataStoreFactory
}

// publisher manages the block publishers for all channels
var BlockPublisher = blockpublisher.NewProvider()

type storeProvider struct {
	stores map[string]transientstore.Store
	transientstore.StoreProvider
	sync.RWMutex
}

func (sp *storeProvider) StoreForChannel(channel string) transientstore.Store {
	sp.RLock()
	defer sp.RUnlock()
	return sp.stores[channel]
}

func (sp *storeProvider) OpenStore(ledgerID string) (transientstore.Store, error) {
	sp.Lock()
	defer sp.Unlock()
	if sp.StoreProvider == nil {
		sp.StoreProvider = transientstore.NewStoreProvider()
	}
	store, err := sp.StoreProvider.OpenStore(ledgerID)
	if err == nil {
		sp.stores[ledgerID] = store
	}
	return store, err
}

func (cs *chainSupport) Apply(configtx *common.ConfigEnvelope) error {
	err := cs.ConfigtxValidator().Validate(configtx)
	if err != nil {
		return err
	}

	// If the chainSupport is being mocked, this field will be nil
	if cs.bundleSource != nil {
		bundle, err := channelconfig.NewBundle(cs.ConfigtxValidator().ChainID(), configtx.Config)
		if err != nil {
			return err
		}

		channelconfig.LogSanityChecks(bundle)

		err = cs.bundleSource.ValidateNew(bundle)
		if err != nil {
			return err
		}

		capabilitiesSupportedOrPanic(bundle)

		cs.bundleSource.Update(bundle)
	}
	return nil
}

func capabilitiesSupportedOrPanic(res channelconfig.Resources) {
	ac, ok := res.ApplicationConfig()
	if !ok {
		peerLogger.Panicf("[channel %s] does not have application config so is incompatible", res.ConfigtxValidator().ChainID())
	}

	if err := ac.Capabilities().Supported(); err != nil {
		peerLogger.Panicf("[channel %s] incompatible: %s", res.ConfigtxValidator().ChainID(), err)
	}

	if err := res.ChannelConfig().Capabilities().Supported(); err != nil {
		peerLogger.Panicf("[channel %s] incompatible: %s", res.ConfigtxValidator().ChainID(), err)
	}
}

func (cs *chainSupport) Ledger() ledger.PeerLedger {
	return cs.ledger
}

func (cs *chainSupport) GetMSPIDs(cid string) []string {
	return GetMSPIDs(cid)
}

// Sequence passes through to the underlying configtx.Validator
func (cs *chainSupport) Sequence() uint64 {
	sb := cs.bundleSource.StableBundle()
	return sb.ConfigtxValidator().Sequence()
}

// Reader returns an iterator to read from the ledger
func (cs *chainSupport) Reader() blockledger.Reader {
	return fileledger.NewFileLedger(fileLedgerBlockStore{cs.ledger})
}

// Errored returns a channel that can be used to determine
// if a backing resource has errored. At this point in time,
// the peer does not have any error conditions that lead to
// this function signaling that an error has occurred.
func (cs *chainSupport) Errored() <-chan struct{} {
	// If this is ever updated to return a real channel, the error message
	// in deliver.go around this channel closing should be updated.
	return nil
}

// chain is a local struct to manage objects in a chain
type chain struct {
	cs        *chainSupport
	cb        *common.Block
	committer committer.Committer
}

// chains is a local map of chainID->chainObject
var chains = struct {
	sync.RWMutex
	list map[string]*chain
}{list: make(map[string]*chain)}

var chainInitializer func(string)

var pluginMapper txvalidator.PluginMapper

var mockMSPIDGetter func(string) []string

func MockSetMSPIDGetter(mspIDGetter func(string) []string) {
	mockMSPIDGetter = mspIDGetter
}

// validationWorkersSemaphore is the semaphore used to ensure that
// there are not too many concurrent tx validation goroutines
var validationWorkersSemaphore *semaphore.Weighted

// Initialize sets up any chains that the peer has from the persistence. This
// function should be called at the start up when the ledger and gossip
// ready
func Initialize(init func(string), ccp ccprovider.ChaincodeProvider, sccp sysccprovider.SystemChaincodeProvider,
	pm txvalidator.PluginMapper, pr *platforms.Registry, deployedCCInfoProvider ledger.DeployedChaincodeInfoProvider,
	membershipProvider ledger.MembershipInfoProvider, metricsProvider metrics.Provider,
	collDataProvider storeapi.Provider) {
	nWorkers := viper.GetInt("peer.validatorPoolSize")
	if nWorkers <= 0 {
		nWorkers = runtime.NumCPU()
	}
	validationWorkersSemaphore = semaphore.NewWeighted(int64(nWorkers))

	pluginMapper = pm
	chainInitializer = init

	var cb *common.Block
	var ledger ledger.PeerLedger
	ledgermgmt.Initialize(&ledgermgmt.Initializer{
		CustomTxProcessors:            ConfigTxProcessors,
		PlatformRegistry:              pr,
		DeployedChaincodeInfoProvider: deployedCCInfoProvider,
		MembershipInfoProvider:        membershipProvider,
		MetricsProvider:               metricsProvider,
		CollDataProvider:              collDataProvider,
	})
	ledgerIds, err := ledgermgmt.GetLedgerIDs()
	if err != nil {
		panic(fmt.Errorf("Error in initializing ledgermgmt: %s", err))
	}
	for _, cid := range ledgerIds {
		peerLogger.Infof("Loading chain %s", cid)
		if ledger, err = ledgermgmt.OpenLedger(cid); err != nil {
			peerLogger.Errorf("Failed to load ledger %s(%s)", cid, err)
			peerLogger.Debugf("Error while loading ledger %s with message %s. We continue to the next ledger rather than abort.", cid, err)
			continue
		}
		if cb, err = getCurrConfigBlockFromLedger(ledger); err != nil {
			peerLogger.Errorf("Failed to find config block on ledger %s(%s)", cid, err)
			peerLogger.Debugf("Error while looking for config block on ledger %s with message %s. We continue to the next ledger rather than abort.", cid, err)
			continue
		}
		// Create a chain if we get a valid ledger with config block
		if err = createChain(cid, ledger, cb, ccp, sccp, pm); err != nil {
			peerLogger.Errorf("Failed to load chain %s(%s)", cid, err)
			peerLogger.Debugf("Error reloading chain %s with message %s. We continue to the next chain rather than abort.", cid, err)
			continue
		}

		InitChain(cid)
	}
}

// InitChain takes care to initialize chain after peer joined, for example deploys system CCs
func InitChain(cid string) {
	if chainInitializer != nil {
		// Initialize chaincode, namely deploy system CC
		peerLogger.Debugf("Initializing channel %s", cid)
		chainInitializer(cid)
	}
}

func getCurrConfigBlockFromLedger(ledger ledger.PeerLedger) (*common.Block, error) {
	peerLogger.Debugf("Getting config block")

	// get last block.  Last block number is Height-1
	blockchainInfo, err := ledger.GetBlockchainInfo()
	if err != nil {
		return nil, err
	}
	lastBlock, err := ledger.GetBlockByNumber(blockchainInfo.Height - 1)
	if err != nil {
		return nil, err
	}

	// get most recent config block location from last block metadata
	configBlockIndex, err := utils.GetLastConfigIndexFromBlock(lastBlock)
	if err != nil {
		return nil, err
	}

	// get most recent config block
	configBlock, err := ledger.GetBlockByNumber(configBlockIndex)
	if err != nil {
		return nil, err
	}

	peerLogger.Debugf("Got config block[%d]", configBlockIndex)
	return configBlock, nil
}

// createChain creates a new chain object and insert it into the chains
func createChain(
	cid string,
	ledger ledger.PeerLedger,
	cb *common.Block,
	ccp ccprovider.ChaincodeProvider,
	sccp sysccprovider.SystemChaincodeProvider,
	pm txvalidator.PluginMapper,
) error {

	chanConf, err := retrievePersistedChannelConfig(ledger)
	if err != nil {
		return err
	}

	var bundle *channelconfig.Bundle

	if chanConf != nil {
		bundle, err = channelconfig.NewBundle(cid, chanConf)
		if err != nil {
			return err
		}
	} else {
		// Config was only stored in the statedb starting with v1.1 binaries
		// so if the config is not found there, extract it manually from the config block
		envelopeConfig, err := utils.ExtractEnvelope(cb, 0)
		if err != nil {
			return err
		}

		bundle, err = channelconfig.NewBundleFromEnvelope(envelopeConfig)
		if err != nil {
			return err
		}
	}

	capabilitiesSupportedOrPanic(bundle)

	channelconfig.LogSanityChecks(bundle)

	gossipEventer := service.GetGossipService().NewConfigEventer()

	gossipCallbackWrapper := func(bundle *channelconfig.Bundle) {
		ac, ok := bundle.ApplicationConfig()
		if !ok {
			// TODO, handle a missing ApplicationConfig more gracefully
			ac = nil
		}

		oc, ok := bundle.OrdererConfig()
		if !ok {
			// TODO: handle a missing OrdererConfig more gracefully
			oc = nil
		}

		gossipEventer.ProcessConfigUpdate(&gossipChannelConfig{
			Validator: bundle.ConfigtxValidator(),
			ac:        ac,
			oc:        oc,
			Channel:   bundle.ChannelConfig(),
		})
		service.GetGossipService().SuspectPeers(func(identity api.PeerIdentityType) bool {
			// TODO: this is a place-holder that would somehow make the MSP layer suspect
			// that a given certificate is revoked, or its intermediate CA is revoked.
			// In the meantime, before we have such an ability, we return true in order
			// to suspect ALL identities in order to validate all of them.
			return true
		})
	}

	trustedRootsCallbackWrapper := func(bundle *channelconfig.Bundle) {
		updateTrustedRoots(bundle)
	}

	mspCallback := func(bundle *channelconfig.Bundle) {
		// TODO remove once all references to mspmgmt are gone from peer code
		mspmgmt.XXXSetMSPManager(cid, bundle.MSPManager())
	}

	ac, ok := bundle.ApplicationConfig()
	if !ok {
		ac = nil
	}

	cs := &chainSupport{
		Application: ac, // TODO, refactor as this is accessible through Manager
		ledger:      ledger,
	}

	peerSingletonCallback := func(bundle *channelconfig.Bundle) {
		ac, ok := bundle.ApplicationConfig()
		if !ok {
			ac = nil
		}
		cs.Application = ac
		cs.Resources = bundle
	}

	cp := &capabilityProvider{}

	cs.bundleSource = channelconfig.NewBundleSource(
		bundle,
		gossipCallbackWrapper,
		trustedRootsCallbackWrapper,
		mspCallback,
		peerSingletonCallback,
		cp.updateChannelConfig,
	)

	vcs := struct {
		*chainSupport
		*semaphore.Weighted
	}{cs, validationWorkersSemaphore}
	validator := txvalidator.NewTxValidator(cid, vcs, sccp, pm)
	blockPublisher := BlockPublisher.ForChannel(cid)
	c := committer.NewLedgerCommitterReactive(ledger, func(block *common.Block) error {
		// Updating CSCC with new configuration block
		if utils.IsConfigBlock(block) {
			logger.Debug("Received configuration update, calling CSCC ConfigUpdate")
			err := SetCurrConfigBlock(block, cid)
			if err != nil {
				return err
			}
		}
		// Inform applicable registered handlers of the new block
		blockPublisher.Publish(block)
		return nil
	})

	oc, ok := bundle.OrdererConfig()
	if !ok {
		return errors.New("no orderer config in bundle")
	}

	ordererAddressesByOrg := make(map[string][]string)
	var ordererOrganizations []string
	for _, ordererOrg := range oc.Organizations() {
		ordererOrganizations = append(ordererOrganizations, ordererOrg.MSPID())
		if len(ordererOrg.Endpoints()) == 0 {
			continue
		}
		ordererAddressesByOrg[ordererOrg.MSPID()] = ordererOrg.Endpoints()
	}

	ordererAddresses := bundle.ChannelConfig().OrdererAddresses()
	if len(ordererAddresses) == 0 && len(ordererAddressesByOrg) == 0 {
		return errors.New("no ordering service endpoint provided in configuration block")
	}

	ordererAddressOverrides, err := GetOrdererAddressOverrides()
	if err != nil {
		return errors.Errorf("failed to get override addresses: %s", err)
	}

	// TODO: does someone need to call Close() on the transientStoreFactory at shutdown of the peer?
	store, err := TransientStoreFactory.OpenStore(bundle.ConfigtxValidator().ChainID())
	if err != nil {
		return errors.Wrapf(err, "[channel %s] failed opening transient store", bundle.ConfigtxValidator().ChainID())
	}

	collDataStore, err := CollectionDataStoreFactory().OpenStore(bundle.ConfigtxValidator().ChainID())
	if err != nil {
		return errors.Wrapf(err, "[channel %s] failed opening transient data store", bundle.ConfigtxValidator().ChainID())
	}

	csStoreSupport := &CollectionSupport{
		PeerLedger: ledger,
	}
	simpleCollectionStore := privdata.NewSimpleCollectionStore(csStoreSupport)

	oac := service.OrdererAddressConfig{
		Addresses:        ordererAddresses,
		AddressesByOrg:   ordererAddressesByOrg,
		Organizations:    ordererOrganizations,
		AddressOverrides: ordererAddressOverrides,
	}
	service.GetGossipService().InitializeChannel(bundle.ConfigtxValidator().ChainID(), oac, service.Support{
		Validator:            validator,
		Committer:            c,
		Store:                store,
		CollDataStore:        collDataStore,
		Cs:                   simpleCollectionStore,
		IdDeserializeFactory: csStoreSupport,
		CapabilityProvider:   cp,
		Capabilities:         cs.Application.Capabilities(),
		Ledger:               ledger,
		BlockPublisher:       blockPublisher,
	})

	chains.Lock()
	defer chains.Unlock()
	chains.list[cid] = &chain{
		cs:        cs,
		cb:        cb,
		committer: c,
	}

	return nil
}

// CreateChainFromBlock creates a new chain from config block
func CreateChainFromBlock(cb *common.Block, ccp ccprovider.ChaincodeProvider, sccp sysccprovider.SystemChaincodeProvider) error {
	cid, err := utils.GetChainIDFromBlock(cb)
	if err != nil {
		return err
	}

	var l ledger.PeerLedger
	if l, err = ledgermgmt.CreateLedger(cb); err != nil {
		return errors.WithMessage(err, "cannot create ledger from genesis block")
	}

	return createChain(cid, l, cb, ccp, sccp, pluginMapper)
}

// GetLedger returns the ledger of the chain with chain ID. Note that this
// call returns nil if chain cid has not been created.
func GetLedger(cid string) ledger.PeerLedger {
	chains.RLock()
	defer chains.RUnlock()
	if c, ok := chains.list[cid]; ok {
		return c.cs.ledger
	}
	return nil
}

// GetStableChannelConfig returns the stable channel configuration of the chain with channel ID.
// Note that this call returns nil if chain cid has not been created.
func GetStableChannelConfig(cid string) channelconfig.Resources {
	chains.RLock()
	defer chains.RUnlock()
	if c, ok := chains.list[cid]; ok {
		return c.cs.bundleSource.StableBundle()
	}
	return nil
}

// GetChannelConfig returns the channel configuration of the chain with channel ID. Note that this
// call returns nil if chain cid has not been created.
func GetChannelConfig(cid string) channelconfig.Resources {
	chains.RLock()
	defer chains.RUnlock()
	if c, ok := chains.list[cid]; ok {
		return c.cs
	}
	return nil
}

// GetPolicyManager returns the policy manager of the chain with chain ID. Note that this
// call returns nil if chain cid has not been created.
func GetPolicyManager(cid string) policies.Manager {
	chains.RLock()
	defer chains.RUnlock()
	if c, ok := chains.list[cid]; ok {
		return c.cs.PolicyManager()
	}
	return nil
}

// GetCurrConfigBlock returns the cached config block of the specified chain.
// Note that this call returns nil if chain cid has not been created.
func GetCurrConfigBlock(cid string) *common.Block {
	chains.RLock()
	defer chains.RUnlock()
	if c, ok := chains.list[cid]; ok {
		return c.cb
	}
	return nil
}

// updates the trusted roots for the peer based on updates to channels
func updateTrustedRoots(cm channelconfig.Resources) {
	// this is triggered on per channel basis so first update the roots for the channel
	peerLogger.Debugf("Updating trusted root authorities for channel %s", cm.ConfigtxValidator().ChainID())
	var serverConfig comm.ServerConfig
	var err error
	// only run is TLS is enabled
	serverConfig, err = GetServerConfig()
	if err == nil && serverConfig.SecOpts.UseTLS {
		buildTrustedRootsForChain(cm)

		// now iterate over all roots for all app and orderer chains
		trustedRoots := [][]byte{}
		credSupport.RLock()
		defer credSupport.RUnlock()
		for _, roots := range credSupport.AppRootCAsByChain {
			trustedRoots = append(trustedRoots, roots...)
		}
		// also need to append statically configured root certs
		if len(serverConfig.SecOpts.ClientRootCAs) > 0 {
			trustedRoots = append(trustedRoots, serverConfig.SecOpts.ClientRootCAs...)
		}
		if len(serverConfig.SecOpts.ServerRootCAs) > 0 {
			trustedRoots = append(trustedRoots, serverConfig.SecOpts.ServerRootCAs...)
		}

		for _, server := range peerServers {
			// now update the client roots for the peerServers
			err := server.SetClientRootCAs(trustedRoots)
			if err != nil {
				msg := "Failed to update trusted roots for peer from latest config " +
					"block.  This peer may not be able to communicate " +
					"with members of channel %s (%s)"
				peerLogger.Warningf(msg, cm.ConfigtxValidator().ChainID(), err)
			}
		}
	}
}

// populates the appRootCAs and orderRootCAs maps by getting the
// root and intermediate certs for all msps associated with the MSPManager
func buildTrustedRootsForChain(cm channelconfig.Resources) {
	credSupport.Lock()
	defer credSupport.Unlock()

	appOrgMSPs := make(map[string]struct{})
	ordOrgMSPs := make(map[string]struct{})

	if ac, ok := cm.ApplicationConfig(); ok {
		//loop through app orgs and build map of MSPIDs
		for _, appOrg := range ac.Organizations() {
			appOrgMSPs[appOrg.MSPID()] = struct{}{}
		}
	}

	if ac, ok := cm.OrdererConfig(); ok {
		//loop through orderer orgs and build map of MSPIDs
		for _, ordOrg := range ac.Organizations() {
			ordOrgMSPs[ordOrg.MSPID()] = struct{}{}
		}
	}

	var appRootCAs comm.CertificateBundle
	cid := cm.ConfigtxValidator().ChainID()
	peerLogger.Debugf("updating root CAs for channel [%s]", cid)
	msps, err := cm.MSPManager().GetMSPs()
	if err != nil {
		peerLogger.Errorf("Error getting root CAs for channel %s (%s)", cid, err)
	}
	ordererRootCAsPerOrg := make(map[string]comm.CertificateBundle)
	if err == nil {
		for k, v := range msps {
			var ordererRootCAs comm.CertificateBundle
			// check to see if this is a FABRIC MSP
			if v.GetType() == msp.FABRIC {
				for _, root := range v.GetTLSRootCerts() {
					// check to see of this is an app org MSP
					if _, ok := appOrgMSPs[k]; ok {
						peerLogger.Debugf("adding app root CAs for MSP [%s]", k)
						appRootCAs = append(appRootCAs, root)
					}
					// check to see of this is an orderer org MSP
					if _, ok := ordOrgMSPs[k]; ok {
						peerLogger.Debugf("adding orderer root CAs for MSP [%s]", k)
						ordererRootCAs = append(ordererRootCAs, root)
					}
				}
				for _, intermediate := range v.GetTLSIntermediateCerts() {
					// check to see of this is an app org MSP
					if _, ok := appOrgMSPs[k]; ok {
						peerLogger.Debugf("adding app root CAs for MSP [%s]", k)
						appRootCAs = append(appRootCAs, intermediate)
					}
					// check to see of this is an orderer org MSP
					if _, ok := ordOrgMSPs[k]; ok {
						peerLogger.Debugf("adding orderer root CAs for MSP [%s]", k)
						ordererRootCAs = append(ordererRootCAs, intermediate)
					}
				}
				ordererRootCAsPerOrg[k] = ordererRootCAs
			}
		}
		credSupport.AppRootCAsByChain[cid] = appRootCAs
		credSupport.OrdererRootCAsByChainAndOrg[cid] = ordererRootCAsPerOrg
	}
}

// GetMSPIDs returns the ID of each application MSP defined on this chain
func GetMSPIDs(cid string) []string {
	chains.RLock()
	defer chains.RUnlock()

	//if mock is set, use it to return MSPIDs
	//used for tests without a proper join
	if mockMSPIDGetter != nil {
		return mockMSPIDGetter(cid)
	}
	if c, ok := chains.list[cid]; ok {
		if c == nil || c.cs == nil {
			return nil
		}
		ac, ok := c.cs.ApplicationConfig()
		if !ok || ac.Organizations() == nil {
			return nil
		}

		orgs := ac.Organizations()
		toret := make([]string, len(orgs))
		i := 0
		for _, org := range orgs {
			toret[i] = org.MSPID()
			i++
		}

		return toret
	}
	return nil
}

// SetCurrConfigBlock sets the current config block of the specified channel
func SetCurrConfigBlock(block *common.Block, cid string) error {
	chains.Lock()
	defer chains.Unlock()
	if c, ok := chains.list[cid]; ok {
		c.cb = block
		return nil
	}
	return errors.Errorf("[channel %s] channel not associated with this peer", cid)
}

// GetLocalIP returns the non loopback local IP of the host
func GetLocalIP() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, address := range addrs {
		// check the address type and if it is not a loopback then display it
		if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String()
			}
		}
	}
	return ""
}

// GetChannelsInfo returns an array with information about all channels for
// this peer
func GetChannelsInfo() []*pb.ChannelInfo {
	// array to store metadata for all channels
	var channelInfoArray []*pb.ChannelInfo

	chains.RLock()
	defer chains.RUnlock()
	for key := range chains.list {
		channelInfo := &pb.ChannelInfo{ChannelId: key}

		// add this specific chaincode's metadata to the array of all chaincodes
		channelInfoArray = append(channelInfoArray, channelInfo)
	}

	return channelInfoArray
}

// NewChannelPolicyManagerGetter returns a new instance of ChannelPolicyManagerGetter
func NewChannelPolicyManagerGetter() policies.ChannelPolicyManagerGetter {
	return &channelPolicyManagerGetter{}
}

type channelPolicyManagerGetter struct{}

func (c *channelPolicyManagerGetter) Manager(channelID string) (policies.Manager, bool) {
	policyManager := GetPolicyManager(channelID)
	return policyManager, policyManager != nil
}

// NewPeerServer creates an instance of comm.GRPCServer
// This server is used for peer communications
func NewPeerServer(listenAddress string, serverConfig comm.ServerConfig) (*comm.GRPCServer, error) {
	peerServer, err := comm.NewGRPCServer(listenAddress, serverConfig)
	if err != nil {
		peerLogger.Errorf("Failed to create peer server (%s)", err)
		return nil, err
	}
	peerServers = append(peerServers, peerServer)
	return peerServer, nil
}

// TODO: Remove CollectionSupport and respective methonds on them.
// CollectionSupport is created per chain and is passed to the simple
// collection store and the gossip. As it is created per chain, there
// is no need to pass the channelID for both GetQueryExecutorForLedger()
// and GetIdentityDeserializer(). Note that the cid passed to
// GetQueryExecutorForLedger is never used. Instead, we can directly
// pass the ledger.PeerLedger and msp.IdentityDeserializer to the
// simpleCollectionStore and pass only the msp.IdentityDeserializer to
// the gossip in createChain() -- FAB-13037
type CollectionSupport struct {
	ledger.PeerLedger
}

func (cs *CollectionSupport) GetQueryExecutorForLedger(cid string) (ledger.QueryExecutor, error) {
	return cs.NewQueryExecutor()
}

func (*CollectionSupport) GetIdentityDeserializer(chainID string) msp.IdentityDeserializer {
	return mspmgmt.GetManagerForChain(chainID)
}

//
//  Deliver service support structs for the peer
//

// DeliverChainManager provides access to a channel for performing deliver
type DeliverChainManager struct {
}

func (DeliverChainManager) GetChain(chainID string) deliver.Chain {
	channel, ok := chains.list[chainID]
	if !ok {
		return nil
	}
	return channel.cs
}

// fileLedgerBlockStore implements the interface expected by
// common/ledger/blockledger/file to interact with a file ledger for deliver
type fileLedgerBlockStore struct {
	ledger.PeerLedger
}

func (flbs fileLedgerBlockStore) AddBlock(*common.Block) error {
	return nil
}

func (flbs fileLedgerBlockStore) RetrieveBlocks(startBlockNumber uint64) (commonledger.ResultsIterator, error) {
	return flbs.GetBlocksIterator(startBlockNumber)
}

// NewConfigSupport returns
func NewConfigSupport() cc.Manager {
	return &configSupport{}
}

type configSupport struct {
}

// GetChannelConfig returns an instance of a object that represents
// current channel configuration tree of the specified channel. The
// ConfigProto method of the returned object can be used to get the
// proto representing the channel configuration.
func (*configSupport) GetChannelConfig(channel string) cc.Config {
	chains.RLock()
	defer chains.RUnlock()
	chain := chains.list[channel]
	if chain == nil {
		peerLogger.Errorf("[channel %s] channel not associated with this peer", channel)
		return nil
	}
	return chain.cs.bundleSource.ConfigtxValidator()
}

type capabilityProvider struct {
	lock   sync.Mutex
	bundle *channelconfig.Bundle
}

func (cp *capabilityProvider) Capabilities() channelconfig.ApplicationCapabilities {
	cp.lock.Lock()
	defer cp.lock.Unlock()
	ac, ok := cp.bundle.ApplicationConfig()
	if !ok {
		return nil
	}
	return ac.Capabilities()
}

func (cp *capabilityProvider) updateChannelConfig(bundle *channelconfig.Bundle) {
	cp.lock.Lock()
	defer cp.lock.Unlock()
	cp.bundle = bundle
}
