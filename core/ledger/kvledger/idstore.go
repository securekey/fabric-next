package kvledger

import (
	"github.com/hyperledger/fabric/core/ledger/kvledger/inventory/cdbid"
	"github.com/hyperledger/fabric/core/ledger/kvledger/inventory/leveldbid"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	"github.com/hyperledger/fabric/protos/common"
	"github.com/pkg/errors"
)

type idStore interface {
	SetUnderConstructionFlag(string) error
	UnsetUnderConstructionFlag() error
	GetUnderConstructionFlag() (string, error)
	CreateLedgerID(ledgerID string, gb *common.Block) error
	LedgerIDExists(ledgerID string) (bool, error)
	GetAllLedgerIds() ([]string, error)
	Close()
}

func openIDStore() (idStore, error) {
	blockStorageConfig := ledgerconfig.GetBlockStoreProvider()

	switch blockStorageConfig {
	case ledgerconfig.FilesystemLedgerStorage:
		path := ledgerconfig.GetLedgerProviderPath()
		return leveldbid.OpenStore(path), nil
	case ledgerconfig.CouchDBLedgerStorage:
		return cdbid.OpenStore()
	}

	return nil, errors.New("ledger inventory storage provider creation failed due to unknown configuration")
}
