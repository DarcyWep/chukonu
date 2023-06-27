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
	stmProcessor := core.NewStmStateProcessor(config.MainnetChainConfig, db)

	var hash1106 common.Hash
	//var hash1106_1 *common.Hash
	min, max, addSpan := big.NewInt(9776810), big.NewInt(9776811), big.NewInt(1)
	for i := min; i.Cmp(max) == -1; i = i.Add(i, addSpan) {
		stateDb = nil
		stateDb, _ = state.NewStmStateDB(*parentRoot, stateCache, snaps) // 每个区块重新构建statedb以释放内存

		block, err2 := database.GetBlockByNumber(db, i) // 正式执行的区块
		if err2 != nil {
			fmt.Println(err2)
			return
		}
		statePre, _ := state.New(parent.Root, stateCache, snaps)
		_, _, _, _, _, _ = processor.Process(block, statePre, vm.Config{EnablePreimageRecording: false})

		hash1106 = block.Root()
		parentRoot, _, _, _, err = stmProcessor.Process(block, stateDb, vm.Config{EnablePreimageRecording: false})
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
		fmt.Println("["+time.Now().Format("2006-01-02 15:04:05")+"]", "replay block number "+i.String(), hash1106 == root, *parentRoot == root)
		//fmt.Println()
	}

}

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
	min, max, addSpan := big.NewInt(9776810), big.NewInt(9776811), big.NewInt(1)
	//min, max, addSpan := big.NewInt(9776810), big.NewInt(9776980), big.NewInt(1)
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
		if txsLen >= 1000 { // 对比1000个交易
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
		if i == 1000 {
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
	fmt.Println("addrInconsistency:", addrInconsistency, addrInconsistencyTxs)
	fmt.Println("slotInconsistency:", slotInconsistency, slotInconsistencyTxs)
	for _, i := range addrInconsistencyTxs {
		accessNormal, accessChu := (*accessAddrNormal)[i], (*accessAddrChu)[i]
		for addr, slotNormal := range *accessNormal {
			fmt.Println(i, addr, slotNormal.IsRead, slotNormal.IsWrite, txs[i].Hash())
		}
		fmt.Println()
		for addr, slotChu := range *accessChu {
			fmt.Println(i, addr, slotChu.IsRead, slotChu.IsWrite, txs[i].Hash())
		}
		fmt.Println()
		fmt.Println()
	}
}

func main() {
	//replayTransactions()
	compare()
}
