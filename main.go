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
	"chukonu/setting"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/params"
	"math/big"
	"sync"
	"time"
)

func replayTransactions() {
	db, err := database.OpenDatabaseWithFreezer(&config.DefaultsEthConfig)
	if err != nil {
		fmt.Println("open leveldb", err)
		return
	}
	defer db.Close()
	var number uint64 = 9776809
	//var number uint64 = 11090500
	blockPre, err := database.GetBlockByNumber(db, new(big.Int).SetUint64(number))
	if err != nil {
		fmt.Println(err)
		return
	}

	var (
		parent     *types.Header  = blockPre.Header()
		parentRoot *common.Hash   = &parent.Root
		stateCache state.Database = database.NewStateCache(db)
		snaps      *snapshot.Tree = database.NewSnap(db, stateCache, blockPre.Header())
	)

	stateDb := database.NewStmStateDB(blockPre.Header(), stateCache, snaps)
	if stateDb == nil {
		return
	}
	processor := core.NewStateProcessor(config.MainnetChainConfig, db)
	//stmProcessor := core.NewStmStateProcessor(config.MainnetChainConfig, db)

	var hash1106 common.Hash
	//var hash1106_1 *common.Hash
	min, max, addSpan := big.NewInt(9776810), big.NewInt(9776820), big.NewInt(1)
	for i := min; i.Cmp(max) == -1; i = i.Add(i, addSpan) {
		stateDb = nil
		stateDb, _ = state.NewStmStateDB(parent.Root, stateCache, snaps) // 每个区块重新构建statedb以释放内存

		block, err := database.GetBlockByNumber(db, i) // 正式执行的区块
		if err != nil {
			fmt.Println(err)
			return
		}
		statePre, _ := state.New(parent.Root, stateCache, snaps)

		// serial execution
		_, _, _, _, _, _ = processor.Process(block, statePre.Copy(), vm.Config{EnablePreimageRecording: false})
		start1 := time.Now()
		root0, _, _, _, _, _ := processor.Process(block, statePre, vm.Config{EnablePreimageRecording: false})
		end1 := time.Since(start1)
		//fmt.Println(root0)
		fmt.Printf("serial execution time: %s\n", end1)

		hash1106 = block.Root()
		fmt.Println(hash1106.Hex())

		_, _ = testBlockSTM(block, config.MainnetChainConfig, db, stateDb.Copy(), 20)
		start2 := time.Now()
		parentRoot, err = testBlockSTM(block, config.MainnetChainConfig, db, stateDb.Copy(), 20)
		end2 := time.Since(start2)
		fmt.Printf("Block-STM execution time: %s\n", end2)

		start3 := time.Now()
		setting.Estimate = true
		estimateRoot, err := testBlockSTM(block, config.MainnetChainConfig, db, stateDb, 20)
		end3 := time.Since(start3)
		fmt.Printf("Block-STM execution with estimate time: %s\n", end3)

		//parentRoot, _, _, _, err = stmProcessor.Process(block, stateDb, vm.Config{EnablePreimageRecording: false})
		if err != nil {
			fmt.Println("process error", err)
			return
		}

		//fmt.Println(parentRoot.Hex())

		// Commit all cached state changes into underlying memory database.
		_, _ = statePre.Commit(config.MainnetChainConfig.IsEIP158(block.Number()))
		root, _ := stateDb.Commit(config.MainnetChainConfig.IsEIP158(block.Number()))
		parentRoot = &root
		parent = block.Header()
		//fmt.Println(*root2 == root)
		//fmt.Println(hash1106.Hex())
		//fmt.Println(hash1106_1.Hex())
		fmt.Println(hash1106)
		fmt.Println(root0)
		fmt.Println(parentRoot)
		fmt.Println(estimateRoot)
		fmt.Println("["+time.Now().Format("2006-01-02 15:04:05")+"]", "replay block number "+i.String(), hash1106 == *root0, hash1106 == *parentRoot, hash1106 == *estimateRoot)
		fmt.Println()
	}
}

func concurrentReplay() {
	db, err := database.OpenDatabaseWithFreezer(&config.DefaultsEthConfig)
	if err != nil {
		fmt.Println("open leveldb", err)
		return
	}
	defer db.Close()
	var number uint64 = 9776809
	var number2 uint64 = 9776810
	blockPre, err := database.GetBlockByNumber(db, new(big.Int).SetUint64(number))
	if err != nil {
		fmt.Println(err)
		return
	}

	var (
		parent     *types.Header  = blockPre.Header()
		parentRoot *common.Hash   = &parent.Root
		stateCache state.Database = database.NewStateCache(db)
		snaps      *snapshot.Tree = database.NewSnap(db, stateCache, blockPre.Header())
	)

	min, max, addSpan := big.NewInt(9776810), big.NewInt(9776850), big.NewInt(1)
	var concurrentTxs types.Transactions
	for i := min; i.Cmp(max) == -1; i = i.Add(i, addSpan) {
		block, err := database.GetBlockByNumber(db, i)
		if err != nil {
			fmt.Println(err)
			return
		}
		newTxs := block.Transactions()
		concurrentTxs = append(concurrentTxs, newTxs...)
	}

	startBlock, err := database.GetBlockByNumber(db, new(big.Int).SetUint64(number2))
	if err != nil {
		fmt.Println(err)
		return
	}
	startBlock.AddTransactions(concurrentTxs)

	stateDb, _ := state.NewStmStateDB(*parentRoot, stateCache, snaps)
	if stateDb == nil {
		return
	}

	serialRoot, _ := testBlockSTM(startBlock, config.MainnetChainConfig, db, stateDb.Copy(), 1)
	start2 := time.Now()
	parentRoot, err = testBlockSTM(startBlock, config.MainnetChainConfig, db, stateDb.Copy(), 20)
	end2 := time.Since(start2)
	fmt.Printf("Block-STM execution time: %s\n", end2)

	start3 := time.Now()
	setting.Estimate = true
	estimateRoot, err := testBlockSTM(startBlock, config.MainnetChainConfig, db, stateDb, 20)
	end3 := time.Since(start3)
	fmt.Printf("Block-STM execution with estimate time: %s\n", end3)

	if err != nil {
		fmt.Println("process error", err)
		return
	}

	fmt.Println(serialRoot.Hex())
	fmt.Println(parentRoot.Hex())
	fmt.Println(estimateRoot.Hex())
	fmt.Println()
}

func testSerialExecution() {
	db, err := database.OpenDatabaseWithFreezer(&config.DefaultsEthConfig)
	if err != nil {
		fmt.Println("open leveldb", err)
		return
	}
	defer db.Close()
	var number uint64 = 9776809
	blockPre, err := database.GetBlockByNumber(db, new(big.Int).SetUint64(number))
	if err != nil {
		fmt.Println(err)
		return
	}

	var (
		parent     *types.Header  = blockPre.Header()
		stateCache state.Database = database.NewStateCache(db)
		snaps      *snapshot.Tree = database.NewSnap(db, stateCache, blockPre.Header())
	)

	stateDb := database.NewStmStateDB(blockPre.Header(), stateCache, snaps)
	if stateDb == nil {
		return
	}
	processor := core.NewStateProcessor(config.MainnetChainConfig, db)

	var totalTime int64
	min, max, addSpan := big.NewInt(9776810), big.NewInt(9776820), big.NewInt(1)
	for i := min; i.Cmp(max) == -1; i = i.Add(i, addSpan) {
		block, err2 := database.GetBlockByNumber(db, i) // 正式执行的区块
		if err2 != nil {
			fmt.Println(err2)
			return
		}
		statePre, _ := state.New(parent.Root, stateCache, snaps)

		_, _, _, _, _, _ = processor.Process(block, statePre.Copy(), vm.Config{EnablePreimageRecording: false})
		start1 := time.Now()
		root0, _, _, _, _, _ := processor.Process(block, statePre, vm.Config{EnablePreimageRecording: false})
		end1 := time.Since(start1)
		fmt.Println(root0)
		fmt.Printf("serial execution time: %s\n", end1)
		totalTime += end1.Milliseconds()

		// Commit all cached state changes into underlying memory database.
		_, _ = statePre.Commit(config.MainnetChainConfig.IsEIP158(block.Number()))
		parent = block.Header()
	}

	fmt.Printf("total execution time: %dms\n", totalTime)
}

func testBlockSTM(block *types.Block, config *params.ChainConfig, chainDb ethdb.Database, stateDB *state.StmStateDB, threads int) (*common.Hash, error) {
	var wg sync.WaitGroup
	stmProcessor := core.NewScheduler(block, config, chainDb)
	wg.Add(threads)
	for i := 0; i < threads; i++ {
		go func() {
			defer wg.Done()
			newThread := core.NewThread(stmProcessor, stateDB)
			newThread.Run()
		}()
	}
	wg.Wait()
	// Fail if Shanghai not enabled and len(withdrawals) is non-zero.
	withdrawals := block.Withdrawals()
	if len(withdrawals) > 0 && !config.IsShanghai(block.Time()) {
		return nil, fmt.Errorf("withdrawals before shanghai")
	}
	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	stateDB.FinaliseMVMemory()
	header := stmProcessor.GetBlockInfo().Header
	//core.StmAccumulateRewards(config, stateDB, header, block.Uncles())
	root := stateDB.IntermediateRoot(config.IsEIP158(header.Number))

	txNum := len(block.Transactions())
	avgExe := float64(stmProcessor.GetExecutionCounter()) / float64(txNum)
	fmt.Printf("Number of txs: %d\n", txNum)
	fmt.Printf("Re-execution overhead: %d, average overhead: %.2f\n", stmProcessor.GetExecutionCounter(), avgExe)

	return &root, nil
}

const testTxsLen = 10000

func compare() {
	db, err := database.OpenDatabaseWithFreezer(&config.DefaultsEthConfig)
	if err != nil {
		fmt.Println("open leveldb", err)
		return
	}
	defer db.Close()
	var number uint64 = 9776809
	//var number uint64 = 11090500
	blockPre, err := database.GetBlockByNumber(db, new(big.Int).SetUint64(number))
	if err != nil {
		fmt.Println(err)
		return
	}

	var (
		parent *types.Header = blockPre.Header()
		//parentRoot *common.Hash   = &parent.Root
		stateCache state.Database = database.NewStateCache(db)
		snaps      *snapshot.Tree = database.NewSnap(db, stateCache, blockPre.Header())
	)

	stateDb := database.NewStmStateDB(blockPre.Header(), stateCache, snaps)
	if stateDb == nil {
		return
	}
	statePre, _ := state.New(parent.Root, stateCache, snaps)

	processor := core.NewStateProcessor(config.MainnetChainConfig, db)
	stmProcessor := core.NewStmStateProcessor(config.MainnetChainConfig, db)

	//var hash1106 common.Hash
	var (
		txsLen                                     = 0
		txs                                        = make(types.Transactions, 0)
		accessAddrNormal []*types.AccessAddressMap = make([]*types.AccessAddressMap, 0)
		accessAddrChu    []*types.AccessAddressMap = make([]*types.AccessAddressMap, 0)
	)
	//min, max, addSpan := big.NewInt(9776810), big.NewInt(9776811), big.NewInt(1)

	min, max, addSpan := big.NewInt(9776810), big.NewInt(9776980), big.NewInt(1)
	for i := min; i.Cmp(max) == -1; i = i.Add(i, addSpan) {
		//stateDb = nil
		//stateDb, _ = state.NewStmStateDB(parent.Root, stateCache, snaps) // 每个区块重新构建statedb以释放内存
		//statePre, _ := state.New(parent.Root, stateCache, snaps)

		block, err2 := database.GetBlockByNumber(db, i) // 正式执行的区块
		if err2 != nil {
			fmt.Println(err2)
			return
		}

		_, accessAddrNormalTmp, _, _, _, _ := processor.Process(block, statePre, vm.Config{EnablePreimageRecording: false})
		accessAddrChuTmp := stmProcessor.ProcessConcurrently(block, stateDb, vm.Config{EnablePreimageRecording: false})
		accessAddrNormal = append(accessAddrNormal, *accessAddrNormalTmp...)
		accessAddrChu = append(accessAddrChu, *accessAddrChuTmp...)
		txs = append(txs, block.Transactions()...)

		txsLen += block.Transactions().Len()
		if txsLen >= testTxsLen { // 对比1000个交易
			break
		}
		fmt.Println("["+time.Now().Format("2006-01-02 15:04:05")+"]", "replay block number "+i.String())
		//fmt.Println()
	}
	compareAccess(&accessAddrNormal, &accessAddrChu, txs)
}

func compareAccess(accessAddrNormal *[]*types.AccessAddressMap, accessAddrChu *[]*types.AccessAddressMap, txs types.Transactions) {
	addrInconsistency, addrReadInconsistency, addrWriteInconsistency := 0, 0, 0
	slotInconsistency := 0
	addrInconsistencyTxs, slotInconsistencyTxs := make([]int, 0), make([]int, 0)
	for i, accessNormal := range *accessAddrNormal {
		if i == testTxsLen {
			break
		}
		isContract := false
		accessChu := (*accessAddrChu)[i]
		for _, slotNormal := range *accessNormal {
			//fmt.Println(i, addr, slotNormal.Slots)
			if len(*slotNormal.Slots) != 0 {
				isContract = true
			}
		}
		for addr, slotNormal := range *accessNormal {
			//fmt.Println(i, addr, slotNormal.Slots)
			//if len(*slotNormal.Slots) != 0 {
			//	isContract = true
			//}
			slotChu, ok := (*accessChu)[addr]
			if !ok { // 正确中有，而模拟的没有，则不一致会影响之后的并发调度，可能造成丢弃；正确中没有，模拟中有，不会影响并发的丢弃
				addrInconsistency += 1
				addrInconsistencyTxs = append(addrInconsistencyTxs, i)
				if slotNormal.IsRead {
					addrReadInconsistency += 1
				}
				if slotNormal.IsWrite {
					addrWriteInconsistency += 1
				}
				break // 每个交易只统计一次
			}
			// 正常地读了, 模拟的既没读，又没写则出错(模拟的不会出现这种情况，因为既没读又没写就是 !ok)
			//if slotNormal.IsRead && (!slotChu.IsRead && !slotChu.IsWrite) {
			//
			//}
			if slotNormal.IsWrite && !slotChu.IsWrite {
				addrInconsistency += 1
				addrWriteInconsistency += 1
				addrInconsistencyTxs = append(addrInconsistencyTxs, i)
				break // 每个交易只统计一次
			}
		}

		for addr, slotNormal := range *accessNormal {
			slotChu, ok := (*accessChu)[addr]
			//if !ok && len(slotNormal.Slots) != 0 { // 正确中有，而模拟的没有，则不一致会影响之后的并发调度，可能造成丢弃；正确中没有，模拟中有，不会影响并发的丢弃
			//	slotInconsistency += 1
			//	break // 每个交易只统计一次
			//}
			if !ok && isContract { // 正确中有，而模拟的没有，则不一致会影响之后的并发调度，可能造成丢弃；正确中没有，模拟中有，不会影响并发的丢弃
				slotInconsistencyTxs = append(slotInconsistencyTxs, i)
				slotInconsistency += 1
				break // 每个交易只统计一次
			}
			// 正常地读了, 模拟的既没读，又没写则出错(模拟的不会出现这种情况，因为既没读又没写就是 !ok)
			//if slotNormal.IsRead && (!slotChu.IsRead && !slotChu.IsWrite) {
			//
			//}
			inconsistency := false
			for key, value := range *slotNormal.Slots {
				valueChu, ok1 := (*slotChu.Slots)[key]
				if !ok1 {
					inconsistency = true
					break
				}
				if value.IsWrite && !valueChu.IsWrite {
					inconsistency = true
					break // 每个交易只统计一次
				}
			}
			if inconsistency {
				slotInconsistency += 1
				slotInconsistencyTxs = append(slotInconsistencyTxs, i)
				break
			}
		}
	}

	//identicalTxs := []int
	//i, j := 0
	//

	fmt.Println("addrInconsistency:", addrInconsistency, addrInconsistencyTxs)
	fmt.Println("slotInconsistency:", slotInconsistency, slotInconsistencyTxs)
	//for _, i := range addrInconsistencyTxs {
	//	accessNormal, accessChu := (*accessAddrNormal)[i], (*accessAddrChu)[i]
	//	for addr, slotNormal := range *accessNormal {
	//		fmt.Println(i, addr, slotNormal.IsRead, slotNormal.IsWrite, txs[i].Hash())
	//	}
	//	fmt.Println()
	//	for addr, slotChu := range *accessChu {
	//		fmt.Println(i, addr, slotChu.IsRead, slotChu.IsWrite, txs[i].Hash())
	//	}
	//	fmt.Println()
	//	fmt.Println()
	//}
}

func main() {
	//replayTransactions()
	//testSerialExecution()
	concurrentReplay()
	//for i := 0; i < 100; i++ {
	//	compare()
	//}
	//compare()
}
