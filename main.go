package main

import (
	"chukonu/config"
	"chukonu/core"
	"chukonu/core/state"
	"chukonu/core/state/snapshot"
	"chukonu/core/types"
	"chukonu/core/vm"
	"chukonu/database"
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
	//var number uint64 = 12000000
	var number uint64 = 9776809
	//var number uint64 = 11090500
	blockPre, err := database.GetBlockByNumber(db, new(big.Int).SetUint64(number))
	if err != nil {
		fmt.Println(err)
		return
	}

	var (
		parent     *types.Header  = blockPre.Header()
		stmRoot    *common.Hash   = &parent.Root
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
	min, max, addSpan := big.NewInt(9776810), big.NewInt(9776812), big.NewInt(1)
	//min, max, addSpan := big.NewInt(12000001), big.NewInt(12090000), big.NewInt(1)
	for i := min; i.Cmp(max) == -1; i = i.Add(i, addSpan) {
		stateDb = nil
		stateDb, _ = state.NewStmStateDB(*stmRoot, stateCache, snaps) // 每个区块重新构建statedb以释放内存

		block, err2 := database.GetBlockByNumber(db, i) // 正式执行的区块
		if err2 != nil {
			fmt.Println(err2)
			return
		}
		statePre, _ := state.New(parent.Root, stateCache, snaps)
		//primitiveRoot, _, _, _, _, _ := processor.Process(block, statePre.Copy(), vm.Config{EnablePreimageRecording: false})
		primitiveRoot, _, _, _, _, _ := processor.Process(block, statePre, vm.Config{EnablePreimageRecording: false})

		hash1106 = block.Root()
		stmRoot, _, _, _, err = stmProcessor.ProcessSerial(block, stateDb, vm.Config{EnablePreimageRecording: false})
		//if err != nil {
		//	fmt.Println("process error", err)
		//	return
		//}
		//
		//// Commit all cached state changes into underlying memory database.
		_, _ = statePre.Commit(config.MainnetChainConfig.IsEIP158(block.Number()))
		r, _ := stateDb.Commit(config.MainnetChainConfig.IsEIP158(block.Number()))
		stmRoot = &r
		//if err != nil {
		//	fmt.Println(err)
		//	return
		//}
		//stateCache.TrieDB().Reference(root, common.Hash{}) // metadata reference to keep trie alive
		//
		//// If we exceeded our memory allowance, flush matured singleton nodes to disk
		//var (
		//	nodes, imgs = stateCache.TrieDB().Size()
		//	limit       = common.StorageSize(256) * 1024 * 1024
		//)
		//if nodes > limit || imgs > 4*1024*1024 {
		//	stateCache.TrieDB().Cap(limit - ethdb.IdealBatchSize)
		//}
		//
		//stateCache.TrieDB().Dereference(parent.Root)
		parent = block.Header()
		//fmt.Println(hash1106.Hex())
		//fmt.Println(hash1106_1.Hex())
		fmt.Println("["+time.Now().Format("2006-01-02 15:04:05")+"]", "replay block number "+i.String(), hash1106 == *primitiveRoot, hash1106 == *stmRoot)
		//fmt.Println("["+time.Now().Format("2006-01-02 15:04:05")+"]", "replay block number "+i.String(), hash1106 == root, *stmRoot == hash1106)
		//fmt.Println()
		//break
	}

}

const testTxsLen = 10000

func addAccessList2AddrChu(tx *types.Transaction, accessChu *types.AccessAddressMap) {
	for _, accessList := range tx.AccessList() {
		addr := accessList.Address
		if _, ok := (*accessChu)[addr]; !ok {
			fmt.Println("new Address")
			(*accessChu)[addr] = types.NewAccessAddress()
			(*accessChu)[addr].IsRead = true
			(*accessChu)[addr].IsWrite = true
		}
		slots := (*accessChu)[addr].Slots
		for _, slot := range accessList.StorageKeys {
			fmt.Println("new Slot")
			if _, ok := (*slots)[slot]; !ok {
				(*slots)[slot] = types.NewAccessSlot()
				(*slots)[slot].IsRead = true
				(*slots)[slot].IsWrite = true
			}
		}

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
		//fmt.Println("finish processor", block.Number())
		accessAddrChuTmp := stmProcessor.ProcessConcurrently(block, stateDb, vm.Config{EnablePreimageRecording: false})
		accessAddrNormal = append(accessAddrNormal, *accessAddrNormalTmp...)
		accessAddrChu = append(accessAddrChu, *accessAddrChuTmp...)
		txs = append(txs, block.Transactions()...)
		//if i.Cmp(big.NewInt(9776813)) == 0 {
		//	accessAddrNormal = append(accessAddrNormal, *accessAddrNormalTmp...)
		//	accessAddrChu = append(accessAddrChu, *accessAddrChuTmp...)
		//	txs = append(txs, block.Transactions()...)
		//	break
		//}

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
		addAccessList2AddrChu(txs[i], (*accessAddrChu)[i])
		accessChu := (*accessAddrChu)[i]
		for _, slotNormal := range *accessNormal {
			//fmt.Println(i, addr, slotNormal.Slots)
			if len(*slotNormal.Slots) != 0 {
				isContract = true
			}
		}
		//if txs[i].Hash() == common.HexToHash("0x8b4d2a4e2d297791dd7da347b351cb6915f909c966afe66558ae452d021c5072") {
		//	fmt.Println(isContract)
		//	for addr, slotNormal := range *accessNormal {
		//		fmt.Println(i, addr, slotNormal.IsRead, slotNormal.IsWrite, slotNormal.Slots)
		//	}
		//	fmt.Println()
		//	for addr, slotChu := range *accessChu {
		//		fmt.Println(i, addr, slotChu.IsRead, slotChu.IsWrite, slotChu.Slots)
		//	}
		//	fmt.Println()
		//}

		for addr, slotNormal := range *accessNormal {
			//fmt.Println(i, addr, slotNormal.Slots)
			//if len(*slotNormal.Slots) != 0 {
			//	isContract = true
			//}
			slotChu, ok := (*accessChu)[addr]
			//_, ok := (*accessChu)[addr]
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
			//for addr, _ := range *accessNormal {
			//	_, ok := (*accessChu)[addr]

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
				//for key, _ := range *slotNormal.Slots {
				//	_, ok1 := (*slotChu.Slots)[key]
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

	identicalTxs := make([]int, 0)
	onlyAddrTxs := make([]int, 0)
	i, j, iLen, jLen := 0, 0, len(addrInconsistencyTxs), len(slotInconsistencyTxs)
	if iLen > 0 && jLen > 0 {
		for {
			if addrInconsistencyTxs[i] == slotInconsistencyTxs[j] {
				identicalTxs = append(identicalTxs, addrInconsistencyTxs[i])
				i += 1
				j += 1
			} else if addrInconsistencyTxs[i] < slotInconsistencyTxs[j] {
				onlyAddrTxs = append(onlyAddrTxs, addrInconsistencyTxs[i])
				i += 1
			} else {
				j += 1
			}
			if i == iLen || j == jLen {
				break
			}
		}
	}

	//fmt.Println("addrInconsistency:", addrInconsistency, addrInconsistencyTxs)
	//fmt.Println("slotInconsistency:", slotInconsistency, slotInconsistencyTxs)
	//fmt.Println("identical:", len(identicalTxs))
	fmt.Println("addrInconsistency:", addrInconsistency)
	fmt.Println("slotInconsistency:", slotInconsistency)
	fmt.Println("identical:", len(identicalTxs))
	//for _, txIndex := range onlyAddrTxs {
	//	fmt.Println(txs[txIndex].Hash())
	//}

	//funcMap := make(map[string]int)
	////for _, txIndex := range slotInconsistencyTxs {
	//for _, txIndex := range addrInconsistencyTxs {
	//	input := txs[txIndex].Data()
	//	if len(input) < 4 {
	//		fmt.Println("len(input) < 4", txs[txIndex].Hash())
	//		continue
	//	}
	//	funcStr := common.Bytes2Hex(input[:4])
	//	if funcStr == setting.TransferKey {
	//		fmt.Println("funcStr == setting.TransferKey", txs[txIndex].Hash())
	//	}
	//	if _, ok := funcMap[funcStr]; !ok {
	//		funcMap[funcStr] = 1
	//	} else {
	//		funcMap[funcStr] += 1
	//	}
	//}
	//
	//type txsByAddr struct {
	//	key string
	//	num int
	//}
	//var listTxsByAddr []txsByAddr
	//for key, vch := range funcMap {
	//	listTxsByAddr = append(listTxsByAddr, txsByAddr{key, vch})
	//}
	//sort.Slice(listTxsByAddr, func(i, j int) bool {
	//	return listTxsByAddr[i].num > listTxsByAddr[j].num // 降序
	//})
	//for _, val := range listTxsByAddr {
	//	fmt.Println(val.key, val.num)
	//}

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

func testChuKoNu() {
	db, err := database.OpenDatabaseWithFreezer(&config.DefaultsEthConfig)
	if err != nil {
		fmt.Println("open leveldb", err)
		return
	}
	defer db.Close()
	//var number uint64 = 12000000
	var number uint64 = 9776809
	//var number uint64 = 11090500
	blockPre, err := database.GetBlockByNumber(db, new(big.Int).SetUint64(number))
	if err != nil {
		fmt.Println(err)
		return
	}

	var (
		parent     *types.Header  = blockPre.Header()
		stmRoot    *common.Hash   = &parent.Root
		stateCache state.Database = database.NewStateCache(db)
		snaps      *snapshot.Tree = database.NewSnap(db, stateCache, blockPre.Header())
	)

	stateDb := database.NewStmStateDB(blockPre.Header(), stateCache, snaps)
	if stateDb == nil {
		return
	}
	stateDbCopy := database.NewStmStateDB(blockPre.Header(), stateCache, snaps)

	stateDbCoarse := database.NewStmStateDB(blockPre.Header(), stateCache, snaps)
	stateDbCoarseCopy := database.NewStmStateDB(blockPre.Header(), stateCache, snaps)

	statePre, _ := state.New(parent.Root, stateCache, snaps)
	statePreCopy := statePre.Copy()
	processor := core.NewStateProcessor(config.MainnetChainConfig, db)

	var hash1106 common.Hash
	min, max, addSpan := big.NewInt(9776810), big.NewInt(9776812), big.NewInt(1)
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
		fmt.Println("["+time.Now().Format("2006-01-02 15:04:05")+"]", "replay block number "+i.String(), hash1106 == *primitiveRoot, hash1106 == *stmRoot)
	}

}

func main() {
	testChuKoNu()
	//replayTransactions()
	//for i := 0; i < 100; i++ {
	//	compare()
	//}
	//compare()
	//fmt.Println(common.Hex2Bytes("0000000000000000000000000000000000000000000000000000000000000001"))
	//fmt.Println(len("0000000000000000000000008f20a07e0541ca2db9152d7e521aee5d639b211d0000000000000000000000000000000000000000000000c893d09c8f51500000"))
	//fmt.Println(len("000000000000000000000000a220f79928906b33ecc80d4d53fa1d750ffb161e000000000000000000000000000000000000000000000000000000012a05f200"))
}
