package main

import (
	"chukonu/config"
	"chukonu/core"
	"chukonu/core/state"
	"chukonu/core/state/snapshot"
	"chukonu/core/types"
	"chukonu/core/vm"
	"chukonu/database"
	"chukonu/ethdb"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"math/big"
	"time"
)

func replayTransactions() {
	db, err := database.OpenDatabaseWithFreezer(&config.DefaultsEthConfig)
	if err != nil {
		fmt.Println("open leveldb", err)
		return
	}
	defer db.Close()
	var number uint64 = 12000000
	//var number uint64 = 11090500
	blockPre, err := database.GetBlockByNumber(db, new(big.Int).SetUint64(number))
	if err != nil {
		fmt.Println(err)
		return
	}

	var (
		parent     *types.Header  = blockPre.Header()
		parentRoot common.Hash    = parent.Root
		stateCache state.Database = database.NewStateCache(db)
		snaps      *snapshot.Tree = database.NewSnap(db, stateCache, blockPre.Header())
	)

	stateDb := database.NewStmStateDB(blockPre.Header(), stateCache, snaps)
	if stateDb == nil {
		return
	}
	processor := core.NewStateProcessor(config.MainnetChainConfig, db)
	stmProcessor := core.NewStmStateProcessor(config.MainnetChainConfig, db)

	var hash1106 common.Hash
	//var hash1106_1 *common.Hash
	min, max, addSpan := big.NewInt(12000001), big.NewInt(12100000), big.NewInt(1)
	for i := min; i.Cmp(max) == -1; i = i.Add(i, addSpan) {
		stateDb = nil
		stateDb, _ = state.NewStmStateDB(parentRoot, stateCache, snaps) // 每个区块重新构建statedb以释放内存

		block, err2 := database.GetBlockByNumber(db, i) // 正式执行的区块
		if err2 != nil {
			fmt.Println(err2)
			return
		}
		statePre, _ := state.New(parent.Root, stateCache, snaps)
		_, _, _, _, _ = processor.Process(block, statePre, vm.Config{EnablePreimageRecording: false})

		hash1106 = block.Root()
		_, _, _, _, err = stmProcessor.Process(block, stateDb, vm.Config{EnablePreimageRecording: false})
		if err != nil {
			fmt.Println("process error", err)
			return
		}

		// Commit all cached state changes into underlying memory database.
		root, err := stateDb.Commit(config.MainnetChainConfig.IsEIP158(block.Number()))
		if err != nil {
			fmt.Println(err)
			return
		}
		stateCache.TrieDB().Reference(root, common.Hash{}) // metadata reference to keep trie alive

		// If we exceeded our memory allowance, flush matured singleton nodes to disk
		var (
			nodes, imgs = stateCache.TrieDB().Size()
			limit       = common.StorageSize(256) * 1024 * 1024
		)
		if nodes > limit || imgs > 4*1024*1024 {
			stateCache.TrieDB().Cap(limit - ethdb.IdealBatchSize)
		}

		stateCache.TrieDB().Dereference(parent.Root)
		parent = block.Header()
		//fmt.Println(hash1106.Hex())
		//fmt.Println(hash1106_1.Hex())
		fmt.Println("["+time.Now().Format("2006-01-02 15:04:05")+"]", "replay block number "+i.String(), hash1106 == root)
		//fmt.Println()
	}

}

func computing20WBlocksTxSum() {
	db, err := database.OpenDatabaseWithFreezer(&config.DefaultsEthConfig)
	if err != nil {
		fmt.Println("open leveldb", err)
		return
	}
	defer db.Close()

	var txSum uint64 = 0
	min, max, addSpan := big.NewInt(12000001), big.NewInt(12200001), big.NewInt(1)
	for i := min; i.Cmp(max) == -1; i = i.Add(i, addSpan) {
		block, err2 := database.GetBlockByNumber(db, i) // 正式执行的区块
		if err2 != nil {
			fmt.Println(err2)
			return
		}
		txSum += uint64(block.Transactions().Len())
		fmt.Println(block.Number().String(), txSum)
	}

}

func main() {
	//prefetchHashChan = make(chan common.Hash, 2048)
	//compareHashChan = make(chan common.Hash, 2048)
	//runtime.GOMAXPROCS(runtime.NumCPU())
	//replayTransactions()
	//close(prefetchHashChan)
	//close(compareHashChan)

	computing20WBlocksTxSum()
}

// https://www.cyberciti.biz/faq/create-a-bootable-windows-10-usb-in-linux/
