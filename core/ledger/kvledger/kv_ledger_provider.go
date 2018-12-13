/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package kvledger

import (
	"errors"
	"fmt"

	"github.com/hyperledger/fabric/core/ledger"
	"github.com/hyperledger/fabric/core/ledger/confighistory"
	"github.com/hyperledger/fabric/core/ledger/confighistory/cdbconfighistory"
	"github.com/hyperledger/fabric/core/ledger/kvledger/bookkeeping"
	"github.com/hyperledger/fabric/core/ledger/kvledger/history/historydb"
	"github.com/hyperledger/fabric/core/ledger/kvledger/history/historydb/historydbprovider"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/privacyenabledstate"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/core/ledger/ledgerstorage"
	"github.com/hyperledger/fabric/core/ledger/util"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/hyperledger/fabric/protos/utils"
	"golang.org/x/net/context"
)

var (
	// ErrLedgerIDExists is thrown by a CreateLedger call if a ledger with the given id already exists
	ErrLedgerIDExists = errors.New("LedgerID already exists")
	// ErrNonExistingLedgerID is thrown by a OpenLedger call if a ledger with the given id does not exist
	ErrNonExistingLedgerID = errors.New("LedgerID does not exist")
	// ErrLedgerNotOpened is thrown by a CloseLedger call if a ledger with the given id has not been opened
	ErrLedgerNotOpened = errors.New("Ledger is not opened yet")
)

// Provider implements interface ledger.PeerLedgerProvider
type Provider struct {
	idStore             idStore
	ledgerStoreProvider *ledgerstorage.Provider
	vdbProvider         privacyenabledstate.DBProvider
	historydbProvider   historydb.HistoryDBProvider
	configHistoryMgr    confighistory.Mgr
	stateListeners      []ledger.StateListener
	bookkeepingProvider bookkeeping.Provider
}

// NewProvider instantiates a new Provider.
// This is not thread-safe and assumed to be synchronized be the caller
func NewProvider() (ledger.PeerLedgerProvider, error) {

	logger.Info("Initializing ledger provider")

	// Initialize the ID store (inventory of chainIds/ledgerIds)
	idStore, err := openIDStore()
	if err != nil {
		return nil, err
	}

	ledgerStoreProvider, err := ledgerstorage.NewProvider()
	if err != nil {
		return nil, err
	}

	// Initialize the versioned database (state database)
	vdbProvider, err := privacyenabledstate.NewCommonStorageDBProvider()
	if err != nil {
		return nil, err
	}

	// Initialize the history database (index for history of values by key)
	historydbProvider, err := historydbprovider.NewHistoryDBProvider()
	if err != nil {
		return nil, err
	}

	bookkeepingProvider := bookkeeping.NewProvider()

	configHistoryStorageConfig := ledgerconfig.GetConfigHistoryStoreProvider()
	var configHistoryMgr confighistory.Mgr
	switch configHistoryStorageConfig {
	case ledgerconfig.LevelDBConfigHistoryStorage:
		configHistoryMgr = confighistory.NewMgr()
	case ledgerconfig.CouchDBConfigHistoryStorage:
		configHistoryMgr, err = cdbconfighistory.NewMgr()
		if err != nil {
			return nil, err
		}
	}

	logger.Info("ledger provider Initialized")
	provider := &Provider{idStore, ledgerStoreProvider, vdbProvider, historydbProvider, configHistoryMgr, nil, bookkeepingProvider}
	provider.recoverUnderConstructionLedger()
	return provider, nil
}

// Initialize implements the corresponding method from interface ledger.PeerLedgerProvider
func (provider *Provider) Initialize(stateListeners []ledger.StateListener) {
	provider.stateListeners = stateListeners
}

// Create implements the corresponding method from interface ledger.PeerLedgerProvider
// This functions sets a under construction flag before doing any thing related to ledger creation and
// upon a successful ledger creation with the committed genesis block, removes the flag and add entry into
// created ledgers list (atomically). If a crash happens in between, the 'recoverUnderConstructionLedger'
// function is invoked before declaring the provider to be usable
func (provider *Provider) Create(genesisBlock *common.Block) (ledger.PeerLedger, error) {
	ledgerID, err := utils.GetChainIDFromBlock(genesisBlock)
	if err != nil {
		return nil, err
	}
	exists, err := provider.idStore.LedgerIDExists(ledgerID)
	if err != nil {
		return nil, err
	}
	if exists {
		return nil, ErrLedgerIDExists
	}
	if err = provider.idStore.SetUnderConstructionFlag(ledgerID); err != nil {
		return nil, err
	}
	lgr, err := provider.openInternal(ledgerID)
	if err != nil {
		logger.Errorf("Error in opening a new empty ledger. Unsetting under construction flag. Err: %s", err)
		panicOnErr(provider.runCleanup(ledgerID), "Error while running cleanup for ledger id [%s]", ledgerID)
		panicOnErr(provider.idStore.UnsetUnderConstructionFlag(), "Error while unsetting under construction flag")
		return nil, err
	}
	txFlags := util.TxValidationFlags(genesisBlock.Metadata.Metadata[common.BlockMetadataIndex_TRANSACTIONS_FILTER])
	if err := lgr.ValidateMVCC(context.Background(), genesisBlock, txFlags, util.TxFilterAcceptAll); err != nil {
		lgr.Close()
		return nil, err
	}
	if err := lgr.ValidateBlockWithPvtData(&ledger.BlockAndPvtData{
		Block: genesisBlock,
	}); err != nil {
		lgr.Close()
		return nil, err
	}
	if err := lgr.CommitWithPvtData(&ledger.BlockAndPvtData{
		Block: genesisBlock,
	}); err != nil {
		lgr.Close()
		return nil, err
	}
	panicOnErr(provider.idStore.CreateLedgerID(ledgerID, genesisBlock), "Error while marking ledger as created")
	return lgr, nil
}

// Open implements the corresponding method from interface ledger.PeerLedgerProvider
func (provider *Provider) Open(ledgerID string) (ledger.PeerLedger, error) {
	logger.Debugf("Open() opening kvledger: %s", ledgerID)
	// Check the ID store to ensure that the chainId/ledgerId exists
	exists, err := provider.idStore.LedgerIDExists(ledgerID)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, ErrNonExistingLedgerID
	}
	return provider.openInternal(ledgerID)
}

func (provider *Provider) openInternal(ledgerID string) (ledger.PeerLedger, error) {
	// Get the block store for a chain/ledger
	blockStore, err := provider.ledgerStoreProvider.Open(ledgerID)
	if err != nil {
		return nil, err
	}

	// Get the versioned database (state database) for a chain/ledger
	vDB, err := provider.vdbProvider.GetDBHandle(ledgerID)
	if err != nil {
		return nil, err
	}

	// Get the history database (index for history of values by key) for a chain/ledger
	historyDB, err := provider.historydbProvider.GetDBHandle(ledgerID)
	if err != nil {
		return nil, err
	}

	// Create a kvLedger for this chain/ledger, which encasulates the underlying data stores
	// (id store, blockstore, state database, history database)
	l, err := newKVLedger(ledgerID, blockStore, vDB, historyDB, provider.configHistoryMgr, provider.stateListeners, provider.bookkeepingProvider)
	if err != nil {
		return nil, err
	}
	return l, nil
}

// Exists implements the corresponding method from interface ledger.PeerLedgerProvider
func (provider *Provider) Exists(ledgerID string) (bool, error) {
	return provider.idStore.LedgerIDExists(ledgerID)
}

// List implements the corresponding method from interface ledger.PeerLedgerProvider
func (provider *Provider) List() ([]string, error) {
	return provider.idStore.GetAllLedgerIds()
}

// Close implements the corresponding method from interface ledger.PeerLedgerProvider
func (provider *Provider) Close() {
	provider.idStore.Close()
	provider.ledgerStoreProvider.Close()
	provider.vdbProvider.Close()
	provider.historydbProvider.Close()
	provider.bookkeepingProvider.Close()
	provider.configHistoryMgr.Close()
}

// recoverUnderConstructionLedger checks whether the under construction flag is set - this would be the case
// if a crash had happened during creation of ledger and the ledger creation could have been left in intermediate
// state. Recovery checks if the ledger was created and the genesis block was committed successfully then it completes
// the last step of adding the ledger id to the list of created ledgers. Else, it clears the under construction flag
func (provider *Provider) recoverUnderConstructionLedger() {
	logger.Debugf("Recovering under construction ledger")
	ledgerID, err := provider.idStore.GetUnderConstructionFlag()
	panicOnErr(err, "Error while checking whether the under construction flag is set")
	if ledgerID == "" {
		logger.Debugf("No under construction ledger found. Quitting recovery")
		return
	}
	logger.Infof("ledger [%s] found as under construction", ledgerID)
	ledger, err := provider.openInternal(ledgerID)
	panicOnErr(err, "Error while opening under construction ledger [%s]", ledgerID)
	bcInfo, err := ledger.GetBlockchainInfo()
	panicOnErr(err, "Error while getting blockchain info for the under construction ledger [%s]", ledgerID)
	ledger.Close()

	switch bcInfo.Height {
	case 0:
		logger.Infof("Genesis block was not committed. Hence, the peer ledger not created. unsetting the under construction flag")
		panicOnErr(provider.runCleanup(ledgerID), "Error while running cleanup for ledger id [%s]", ledgerID)
		panicOnErr(provider.idStore.UnsetUnderConstructionFlag(), "Error while unsetting under construction flag")
	case 1:
		logger.Infof("Genesis block was committed. Hence, marking the peer ledger as created")
		genesisBlock, err := ledger.GetBlockByNumber(0)
		panicOnErr(err, "Error while retrieving genesis block from blockchain for ledger [%s]", ledgerID)
		panicOnErr(provider.idStore.CreateLedgerID(ledgerID, genesisBlock), "Error while adding ledgerID [%s] to created list", ledgerID)
	default:
		panic(fmt.Errorf(
			"Data inconsistency: under construction flag is set for ledger [%s] while the height of the blockchain is [%d]",
			ledgerID, bcInfo.Height))
	}
	return
}

// runCleanup cleans up blockstorage, statedb, and historydb for what
// may have got created during in-complete ledger creation
func (provider *Provider) runCleanup(ledgerID string) error {
	// TODO - though, not having this is harmless for kv ledger.
	// If we want, following could be done:
	// - blockstorage could remove empty folders
	// - couchdb backed statedb could delete the database if got created
	// - leveldb backed statedb and history db need not perform anything as it uses a single db shared across ledgers
	return nil
}

func panicOnErr(err error, mgsFormat string, args ...interface{}) {
	if err == nil {
		return
	}
	args = append(args, err)
	panic(fmt.Sprintf(mgsFormat+" Err:%s ", args...))
}