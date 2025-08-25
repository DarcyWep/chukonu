package database

import (
	"chukonu/core/rawdb"
	"chukonu/core/state"
	"chukonu/core/state/snapshot"
	"chukonu/core/types"
	"chukonu/ethdb"
	"chukonu/trie"
	"fmt"
	"github.com/ethereum/go-ethereum/log"
)

func NewStateCache(db ethdb.Database, config *StateDBConfig) state.Database {
	return state.NewDatabaseWithConfig(db, &trie.Config{
		Cache:     config.Cache,
		Journal:   config.Journal,
		Preimages: config.Preimages,
	})
}

func NewSnap(db ethdb.Database, stateCache state.Database, header *types.Header) *snapshot.Tree {
	var recover bool

	if layer := rawdb.ReadSnapshotRecoveryNumber(db); layer != nil && *layer > header.Number.Uint64() {
		log.Warn("Enabling snapshot recovery", "chainhead", header.Number.Uint64(), "diskbase", *layer)
		recover = true
	}
	snapconfig := snapshot.Config{
		CacheSize:  256,
		Recovery:   recover,
		NoBuild:    true,
		AsyncBuild: false,
	}

	snaps, _ := snapshot.New(snapconfig, db, stateCache.TrieDB(), header.Root)
	return snaps
}

func NewStateDB(header *types.Header, stateCache state.Database, snaps *snapshot.Tree) *state.StateDB {
	stateDb, err := state.New(header.Root, stateCache, snaps)
	if err != nil {
		fmt.Println(stateDb, "New StateDB Error", err)
		return nil
	}
	return stateDb
}

func NewStmStateDB(header *types.Header, stateCache state.Database, snaps *snapshot.Tree) *state.StmStateDB {
	stateDb, err := state.NewStmStateDB(header.Root, stateCache, snaps)
	if err != nil {
		fmt.Println(stateDb, "New StateDB Error", err)
		return nil
	}
	return stateDb
}

//func NewStateDatabase(db ethdb.Database, number uint64, parent *types.Header) (*state.StateDB, error) {
//	var stateDB *state.StateDB = nil
//	var err error
//	hash := rawdb.ReadCanonicalHash(db, number) // 创建StateDB
//	if (hash != common.Hash{}) {
//		if header := rawdb.ReadHeader(db, hash, number); header != nil {
//			parent = header
//			stateDB = newStateCache(db, header)
//		} else {
//			err = fmt.Errorf("create stateDB error! header is nil")
//		}
//	} else {
//		err = fmt.Errorf("create stateDB error! header is nil")
//	}
//	return stateDB, err
//}
