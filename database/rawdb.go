package database

import (
	"chukonu/config"
	"chukonu/core/rawdb"
	"chukonu/core/types"
	"chukonu/ethdb"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"math/big"
)

func OpenDatabaseWithFreezer(ethConfig *config.EthConfig, rawConfig *RawConfig) (ethdb.Database, error) {
	if ethConfig.NoPruning && ethConfig.TrieDirtyCache > 0 {
		if ethConfig.SnapshotCache > 0 {
			ethConfig.TrieCleanCache += ethConfig.TrieDirtyCache * 3 / 5
			ethConfig.SnapshotCache += ethConfig.TrieDirtyCache * 2 / 5
		} else {
			ethConfig.TrieCleanCache += ethConfig.TrieDirtyCache
		}
		ethConfig.TrieDirtyCache = 0
	}

	db, err := rawdb.Open(rawdb.OpenOptions{
		Type:              "",
		Directory:         rawConfig.Path,
		AncientsDirectory: rawConfig.Ancient,
		Namespace:         rawConfig.Namespace,
		Cache:             ethConfig.DatabaseCache,
		Handles:           rawConfig.Handles,
		ReadOnly:          rawConfig.ReadOnly,
	})
	return db, err
}

func GetBlockByNumber(db ethdb.Database, number *big.Int) (*types.Block, error) {
	var (
		block *types.Block
		err   error
	)
	hash := rawdb.ReadCanonicalHash(db, number.Uint64()) // 获取区块hash
	if (hash != common.Hash{}) {
		block = rawdb.ReadBlock(db, hash, number.Uint64())
		if block == nil {
			err = fmt.Errorf("read block(" + number.String() + ") error! block is nil")
		}
	} else {
		err = fmt.Errorf("read block(" + number.String() + ") error! hash is nil")
	}
	return block, err
}

func GetHeaderByNumber(db ethdb.Database, number uint64) (*types.Header, error) {
	var (
		header *types.Header = nil
		err    error         = nil
	)
	hash := rawdb.ReadCanonicalHash(db, number) // 创建StateDB
	if (hash != common.Hash{}) {
		if h := rawdb.ReadHeader(db, hash, number); header != nil {
			header = h
		} else {
			err = fmt.Errorf("create stateDB error! header is nil")
		}
	}
	return header, err
}
