package main

import (
	"chukonu/config"
	"chukonu/core"
	"chukonu/core/state"
	"chukonu/core/types"
	"chukonu/core/vm"
	"chukonu/database"
	"chukonu/experiment/conflict_detection"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"math/big"
	"time"
)

func testChuKoNu() {
	db, err := database.OpenDatabaseWithFreezer(&config.DefaultsEthConfig, database.DefaultRawConfig())
	if err != nil {
		fmt.Println("open leveldb", err)
		return
	}
	defer db.Close()
	var number uint64 = 14000000
	blockPre, err := database.GetBlockByNumber(db, new(big.Int).SetUint64(number))
	if err != nil {
		fmt.Println(err)
		return
	}

	var (
		parent     *types.Header  = blockPre.Header()
		stmRoot    *common.Hash   = &parent.Root
		stateCache state.Database = database.NewStateCache(db, database.DefaultStateDBConfig())
	)

	stateDb := database.NewStmStateDB(blockPre.Header(), stateCache, nil)
	if stateDb == nil {
		return
	}
	stateDbCopy := database.NewStmStateDB(blockPre.Header(), stateCache, nil)

	stateDbCoarse := database.NewStmStateDB(blockPre.Header(), stateCache, nil)
	stateDbCoarseCopy := database.NewStmStateDB(blockPre.Header(), stateCache, nil)

	stateDbCoarseRW := database.NewStmStateDB(blockPre.Header(), stateCache, nil)
	stateDbCoarseRWCopy := database.NewStmStateDB(blockPre.Header(), stateCache, nil)

	stateDbFine := database.NewStmStateDB(blockPre.Header(), stateCache, nil)
	stateDbFineCopy := database.NewStmStateDB(blockPre.Header(), stateCache, nil)

	statePre, _ := state.New(parent.Root, stateCache, nil)
	statePreCopy := statePre.Copy()
	processor := core.NewStateProcessor(config.MainnetChainConfig, db)

	var hash1106 common.Hash
	min, max, addSpan := big.NewInt(14000001), big.NewInt(14020001), big.NewInt(1)
	for i := min; i.Cmp(max) == -1; i = i.Add(i, addSpan) {
		block, err2 := database.GetBlockByNumber(db, i) // 正式执行的区块
		if err2 != nil {
			fmt.Println(err2)
			return
		}

		_, _, _, _, _, _ = processor.Process(block, statePreCopy, vm.Config{EnablePreimageRecording: false})
		primitiveRoot, _, _, _, _, _ := processor.Process(block, statePre, vm.Config{EnablePreimageRecording: false})

		hash1106 = block.Root()
		core.ChuKoNuConcurrencyAccess(block, stateDbCopy, vm.Config{EnablePreimageRecording: false}, config.MainnetChainConfig, db)
		core.ChuKoNuConcurrencyAccess(block, stateDb, vm.Config{EnablePreimageRecording: false}, config.MainnetChainConfig, db)

		core.ChuKoNuConcurrencyCoarse(block, stateDbCoarseCopy, vm.Config{EnablePreimageRecording: false}, config.MainnetChainConfig, db)
		core.ChuKoNuConcurrencyCoarse(block, stateDbCoarse, vm.Config{EnablePreimageRecording: false}, config.MainnetChainConfig, db)

		core.ChuKoNuConcurrencyCoarseRW(block, stateDbCoarseRWCopy, vm.Config{EnablePreimageRecording: false}, config.MainnetChainConfig, db)
		core.ChuKoNuConcurrencyCoarseRW(block, stateDbCoarseRW, vm.Config{EnablePreimageRecording: false}, config.MainnetChainConfig, db)

		core.ChuKoNuConcurrencyFineRW(block, stateDbFineCopy, vm.Config{EnablePreimageRecording: false}, config.MainnetChainConfig, db)
		core.ChuKoNuConcurrencyFineRW(block, stateDbFine, vm.Config{EnablePreimageRecording: false}, config.MainnetChainConfig, db)
		fmt.Println("["+time.Now().Format("2006-01-02 15:04:05")+"]", "replay block number "+i.String(), hash1106 == *primitiveRoot, hash1106 == *stmRoot)
	}

}

func testChuKoNuFast() {
	db, err := database.OpenDatabaseWithFreezer(&config.DefaultsEthConfig, database.DefaultRawConfig())
	if err != nil {
		fmt.Println("open leveldb", err)
		return
	}
	defer db.Close()
	var number uint64 = 14000000
	blockPre, err := database.GetBlockByNumber(db, new(big.Int).SetUint64(number))
	if err != nil {
		fmt.Println(err)
		return
	}

	var (
		parent     *types.Header  = blockPre.Header()
		stateCache state.Database = database.NewStateCache(db, database.DefaultStateDBConfig())
	)

	chuKoNuStateDB, err := state.NewChuKoNuStateDB(parent.Root, stateCache, nil, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	chuKoNuProcessor := core.NewChuKoNuProcessor(config.MainnetChainConfig, db)

	min, max, addSpan := big.NewInt(14000001), big.NewInt(14020001), big.NewInt(1)
	for i := min; i.Cmp(max) == -1; i = i.Add(i, addSpan) {
		block, err2 := database.GetBlockByNumber(db, i) // 正式执行的区块
		if err2 != nil {
			fmt.Println(err2)
			return
		}

		chuKoNuProcessor.SerialSimulation(block, chuKoNuStateDB.Copy(), vm.Config{EnablePreimageRecording: false}, false)
		chuKoNuProcessor.SerialSimulation(block, chuKoNuStateDB, vm.Config{EnablePreimageRecording: false}, false)
		root, _ := chuKoNuStateDB.Commit(true)
		if block.Transactions().Len() != 0 {
			chuKoNuFastProcessor := core.NewChuKoNuFastProcessor(config.MainnetChainConfig, db, block, chuKoNuStateDB)
			chuKoNuFastProcessor.ChuKoNuFast(block, chuKoNuStateDB, vm.Config{EnablePreimageRecording: false})
		}

		chuKoNuStateDB, _ = state.NewChuKoNuStateDB(root, stateCache, nil, nil)
		fmt.Println("["+time.Now().Format("2006-01-02 15:04:05")+"]", "replay block number "+i.String())
	}

}

func main() {
	//testChuKoNu()
	//replayTransactions()
	//for i := 0; i < 100; i++ {
	//	compare()
	//}
	//compare()
	//compareRW()
	//replay()
	//testChuKoNuFast()
	//observation_server.Observation()
	conflict_detection.DetectionOverhead()
	//chukonu.TestChuKoNuLargeTPS()
	//chukonu.TestChuKoNuTPS()
	//dmvcc.TestDMVCCTPS()
}
