package main

import (
	"chukonu/config"
	"chukonu/core"
	"chukonu/core/state"
	"chukonu/core/types"
	"chukonu/core/vm"
	"chukonu/database"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"math/big"
	"time"
)

//var testTxsLen = 500

// 500 1000 5000 10000
//// const compareLen = 500
//var testTxsLen = 4 * 200
//
//func addAccessList2AddrChu(tx *types.Transaction, accessChu *types.AccessAddressMap) {
//	for _, accessList := range tx.AccessList() {
//		addr := accessList.Address
//		if _, ok := (*accessChu)[addr]; !ok {
//			fmt.Println("new Address")
//			(*accessChu)[addr] = types.NewAccessAddress()
//			(*accessChu)[addr].IsRead = true
//			(*accessChu)[addr].IsWrite = true
//		}
//		slots := (*accessChu)[addr].Slots
//		for _, slot := range accessList.StorageKeys {
//			fmt.Println("new Slot")
//			if _, ok := (*slots)[slot]; !ok {
//				(*slots)[slot] = types.NewAccessSlot()
//				(*slots)[slot].IsRead = true
//				(*slots)[slot].IsWrite = true
//			}
//		}
//
//	}
//}
//
//func compareRW() {
//	rawConfig := database.DefaultRawConfig()
//	rawConfig.Path = "/home/fuzh/chukonu/data/ethereumdata/copchaincopy"
//	rawConfig.Ancient = "/home/fuzh/chukonu/data/ethereumdata/copchaincopy/ancient"
//	var stateConfig = database.DefaultStateDBConfig()
//	stateConfig.Journal = "/home/fuzh/chukonu/data/ethereumdata/triecache"
//
//	db, err := database.OpenDatabaseWithFreezer(&config.DefaultsEthConfig, rawConfig)
//	if err != nil {
//		fmt.Println("open leveldb", err)
//		return
//	}
//	defer db.Close()
//	var number uint64 = 14000000
//	blockStable, err := database.GetBlockByNumber(db, new(big.Int).SetUint64(number))
//	if err != nil {
//		fmt.Println(err)
//		return
//	}
//
//	var (
//		parent     *types.Header  = blockStable.Header()
//		stateCache state.Database = database.NewStateCache(db, database.DefaultStateDBConfig())
//	)
//
//	chuKoNuStateDB, err := state.NewChuKoNuStateDB(parent.Root, stateCache, nil, nil)
//	if err != nil {
//		fmt.Println(err)
//	}
//	//chuKoNuStateDBCopy, err := state.NewChuKoNuStateDB(parent.Root, stateCache, nil, nil)
//
//	chuKuNoProcessor := core.NewChuKoNuProcessor(config.MainnetChainConfig, db)
//	var (
//		count                                      = 0
//		txsLen                                     = 0
//		txs                                        = make(types.Transactions, 0)
//		accessAddrNormal []*types.AccessAddressMap = make([]*types.AccessAddressMap, 0)
//		accessAddrChu    []*types.AccessAddressMap = make([]*types.AccessAddressMap, 0)
//	)
//
//	min, max, addSpan := big.NewInt(14000001), big.NewInt(14020002), big.NewInt(1)
//	for i := min; i.Cmp(max) == -1; i = i.Add(i, addSpan) {
//		block, err2 := database.GetBlockByNumber(db, i) // 正式执行的区块
//		if err2 != nil {
//			fmt.Println(err2)
//			return
//		}
//
//		normal := chuKuNoProcessor.SerialSimulation(block, chuKoNuStateDB, vm.Config{EnablePreimageRecording: false})
//		accessAddrNormal = append(accessAddrNormal, *normal...)
//		ckn := chuKuNoProcessor.ConcurrentSimulation(block, chuKoNuStateDB, vm.Config{EnablePreimageRecording: false})
//		accessAddrChu = append(accessAddrChu, *ckn...)
//
//		txs = append(txs, block.Transactions()...)
//
//		txsLen += block.Transactions().Len()
//		if txsLen >= testTxsLen { // 对比 testTxsLen 个交易
//			count += 1
//			compareAccess(&accessAddrNormal, &accessAddrChu, txs)
//			root, err := chuKoNuStateDB.Commit(true)
//			if err != nil {
//				fmt.Println("state db commit error", err)
//				return
//			}
//			chuKoNuStateDB, _ = state.NewChuKoNuStateDB(root, stateCache, nil, nil)
//			txsLen = 0
//			txs = txs[:0]
//			accessAddrNormal = accessAddrNormal[:0]
//			accessAddrChu = accessAddrChu[:0]
//			if count == 10 {
//				break
//			}
//		}
//		fmt.Println("["+time.Now().Format("2006-01-02 15:04:05")+"]", "replay block number "+i.String())
//	}
//}
//
//func compare() {
//	db, err := database.OpenDatabaseWithFreezer(&config.DefaultsEthConfig, database.DefaultRawConfig())
//	if err != nil {
//		fmt.Println("open leveldb", err)
//		return
//	}
//	defer db.Close()
//	var number uint64 = 14000000
//	blockPre, err := database.GetBlockByNumber(db, new(big.Int).SetUint64(number))
//	if err != nil {
//		fmt.Println(err)
//		return
//	}
//
//	var (
//		parent *types.Header = blockPre.Header()
//		//parentRoot *common.Hash   = &parent.Root
//		stateCache state.Database = database.NewStateCache(db, database.DefaultStateDBConfig())
//		snaps      *snapshot.Tree = database.NewSnap(db, stateCache, blockPre.Header())
//	)
//
//	stateDb := database.NewStmStateDB(blockPre.Header(), stateCache, snaps)
//	if stateDb == nil {
//		return
//	}
//	statePre, _ := state.New(parent.Root, stateCache, snaps)
//
//	processor := core.NewStateProcessor(config.MainnetChainConfig, db)
//	stmProcessor := core.NewStmStateProcessor(config.MainnetChainConfig, db)
//
//	//var hash1106 common.Hash
//	var (
//		txsLen                                     = 0
//		txs                                        = make(types.Transactions, 0)
//		accessAddrNormal []*types.AccessAddressMap = make([]*types.AccessAddressMap, 0)
//		accessAddrChu    []*types.AccessAddressMap = make([]*types.AccessAddressMap, 0)
//	)
//	//min, max, addSpan := big.NewInt(9776810), big.NewInt(9776811), big.NewInt(1)
//
//	min, max, addSpan := big.NewInt(14000001), big.NewInt(14020001), big.NewInt(1)
//	for i := min; i.Cmp(max) == -1; i = i.Add(i, addSpan) {
//		//stateDb = nil
//		//stateDb, _ = state.NewStmStateDB(parent.Root, stateCache, snaps) // 每个区块重新构建statedb以释放内存
//		//statePre, _ := state.New(parent.Root, stateCache, snaps)
//
//		block, err2 := database.GetBlockByNumber(db, i) // 正式执行的区块
//		if err2 != nil {
//			fmt.Println(err2)
//			return
//		}
//
//		root1, accessAddrNormalTmp, _, _, _, _ := processor.Process(block, statePre, vm.Config{EnablePreimageRecording: false})
//		root2, _, _, _, _ := stmProcessor.ProcessSerial(block, stateDb, vm.Config{EnablePreimageRecording: false})
//		//fmt.Println("finish processor", block.Number())
//		accessAddrChuTmp := stmProcessor.ProcessConcurrently(block, stateDb, vm.Config{EnablePreimageRecording: false})
//		accessAddrNormal = append(accessAddrNormal, *accessAddrNormalTmp...)
//		accessAddrChu = append(accessAddrChu, *accessAddrChuTmp...)
//		txs = append(txs, block.Transactions()...)
//		//if i.Cmp(big.NewInt(9776813)) == 0 {
//		//      accessAddrNormal = append(accessAddrNormal, *accessAddrNormalTmp...)
//		//      accessAddrChu = append(accessAddrChu, *accessAddrChuTmp...)
//		//      txs = append(txs, block.Transactions()...)
//		//      break
//		//}
//
//		txsLen += block.Transactions().Len()
//		if txsLen >= testTxsLen { // 对比1000个交易
//			break
//		}
//		fmt.Println(root1)
//		fmt.Println(root2)
//		fmt.Println("["+time.Now().Format("2006-01-02 15:04:05")+"]", "replay block number "+i.String())
//		//fmt.Println()
//	}
//
//	compareAccess(&accessAddrNormal, &accessAddrChu, txs)
//}
//
//func compareAccess(accessAddrNormal *[]*types.AccessAddressMap, accessAddrChu *[]*types.AccessAddressMap, txs types.Transactions) {
//	addrInconsistency, addrReadInconsistency, addrWriteInconsistency := 0, 0, 0
//	slotInconsistency := 0
//	addrInconsistencyTxs, slotInconsistencyTxs := make([]int, 0), make([]int, 0)
//	for i, accessNormal := range *accessAddrNormal {
//		if i == compareLen-1 {
//			break
//		}
//		isContract := false
//		//addAccessList2AddrChu(txs[i], (*accessAddrChu)[i])
//		accessChu := (*accessAddrChu)[i]
//		for _, slotNormal := range *accessNormal {
//			//fmt.Println(i, addr, slotNormal.Slots)
//			if len(*slotNormal.Slots) != 0 {
//				isContract = true
//			}
//		}
//
//		for addr, slotNormal := range *accessNormal {
//			//fmt.Println(i, addr, slotNormal.Slots)
//			//if len(*slotNormal.Slots) != 0 {
//			//      isContract = true
//			//}
//			slotChu, ok := (*accessChu)[addr]
//			//fmt.Println(slotNormal.IsRead, slotNormal.IsWrite, slotChu.IsRead, slotChu.IsWrite)
//			//_, ok := (*accessChu)[addr]
//
//			if !ok { // 正确中有，而模拟的没有，则不一致会影响之后的并发调度，可能造成丢弃；正确中没有，模拟中有，不会影响并发的丢弃
//				addrInconsistency += 1
//				addrInconsistencyTxs = append(addrInconsistencyTxs, i)
//				if slotNormal.IsRead {
//					addrReadInconsistency += 1
//				}
//				if slotNormal.IsWrite {
//					addrWriteInconsistency += 1
//				}
//				break // 每个交易只统计一次
//			}
//			// 正常地读了, 模拟的既没读，又没写则出错(模拟的不会出现这种情况，因为既没读又没写就是 !ok)
//			//if slotNormal.IsRead && (!slotChu.IsRead && !slotChu.IsWrite) {
//			//
//			//}
//			if slotNormal.IsWrite && !slotChu.IsWrite {
//				addrInconsistency += 1
//				addrWriteInconsistency += 1
//				addrInconsistencyTxs = append(addrInconsistencyTxs, i)
//				break // 每个交易只统计一次
//			}
//		}
//
//		for addr, slotNormal := range *accessNormal {
//			slotChu, ok := (*accessChu)[addr]
//			//for addr, _ := range *accessNormal {
//			//      _, ok := (*accessChu)[addr]
//
//			//if !ok && len(slotNormal.Slots) != 0 { // 正确中有，而模拟的没有，则不一致会影响之后的并发调度，可能造成丢弃；正确中没有，模拟中有，不会影响并发的丢弃
//			//      slotInconsistency += 1
//			//      break // 每个交易只统计一次
//			//}
//			if !ok && isContract { // 正确中有，而模拟的没有，则不一致会影响之后的并发调度，可能造成丢弃；正确中没有，模拟中有，不会影响并发的丢弃
//				slotInconsistencyTxs = append(slotInconsistencyTxs, i)
//				slotInconsistency += 1
//				break // 每个交易只统计一次
//			}
//			// 正常地读了, 模拟的既没读，又没写则出错(模拟的不会出现这种情况，因为既没读又没写就是 !ok)
//			//if slotNormal.IsRead && (!slotChu.IsRead && !slotChu.IsWrite) {
//			//
//			//}
//
//			inconsistency := false
//			for key, value := range *slotNormal.Slots {
//				valueChu, ok1 := (*slotChu.Slots)[key]
//				//for key, _ := range *slotNormal.Slots {
//				//      _, ok1 := (*slotChu.Slots)[key]
//				if !ok1 {
//					inconsistency = true
//					break
//				}
//				if value.IsWrite && !valueChu.IsWrite {
//					inconsistency = true
//					break // 每个交易只统计一次
//				}
//			}
//			if inconsistency {
//				slotInconsistency += 1
//				slotInconsistencyTxs = append(slotInconsistencyTxs, i)
//				break
//			}
//		}
//	}
//
//	identicalTxs := make([]int, 0)
//	onlyAddrTxs := make([]int, 0)
//	i, j, iLen, jLen := 0, 0, len(addrInconsistencyTxs), len(slotInconsistencyTxs)
//	if iLen > 0 && jLen > 0 {
//		for {
//			if addrInconsistencyTxs[i] == slotInconsistencyTxs[j] {
//				identicalTxs = append(identicalTxs, addrInconsistencyTxs[i])
//				i += 1
//				j += 1
//			} else if addrInconsistencyTxs[i] < slotInconsistencyTxs[j] {
//				onlyAddrTxs = append(onlyAddrTxs, addrInconsistencyTxs[i])
//				i += 1
//			} else {
//				j += 1
//			}
//			if i == iLen || j == jLen {
//				break
//			}
//		}
//	}
//
//	//fmt.Println("addrInconsistency:", addrInconsistency, addrInconsistencyTxs)
//	//fmt.Println("slotInconsistency:", slotInconsistency, slotInconsistencyTxs)
//	//fmt.Println("identical:", len(identicalTxs))
//	fmt.Println("addrInconsistency:", addrInconsistency)
//	fmt.Println("slotInconsistency:", slotInconsistency)
//	fmt.Println("identical:", len(identicalTxs))
//	//for _, txIndex := range onlyAddrTxs {
//	//      fmt.Println(txs[txIndex].Hash())
//	//}
//
//	//funcMap := make(map[string]int)
//	////for _, txIndex := range slotInconsistencyTxs {
//	//for _, txIndex := range addrInconsistencyTxs {
//	//      input := txs[txIndex].Data()
//	//      if len(input) < 4 {
//	//              fmt.Println("len(input) < 4", txs[txIndex].Hash())
//	//              continue
//	//      }
//	//      funcStr := common.Bytes2Hex(input[:4])
//	//      if funcStr == setting.TransferKey {
//	//              fmt.Println("funcStr == setting.TransferKey", txs[txIndex].Hash())
//	//      }
//	//      if _, ok := funcMap[funcStr]; !ok {
//	//              funcMap[funcStr] = 1
//	//      } else {
//	//              funcMap[funcStr] += 1
//	//      }
//	//}
//	//
//	//type txsByAddr struct {
//	//      key string
//	//      num int
//	//}
//	//var listTxsByAddr []txsByAddr
//	//for key, vch := range funcMap {
//	//      listTxsByAddr = append(listTxsByAddr, txsByAddr{key, vch})
//	//}
//	//sort.Slice(listTxsByAddr, func(i, j int) bool {
//	//      return listTxsByAddr[i].num > listTxsByAddr[j].num // 降序
//	//})
//	//for _, val := range listTxsByAddr {
//	//      fmt.Println(val.key, val.num)
//	//}
//
//	//for _, i := range addrInconsistencyTxs {
//	//      accessNormal, accessChu := (*accessAddrNormal)[i], (*accessAddrChu)[i]
//	//      for addr, slotNormal := range *accessNormal {
//	//              fmt.Println(i, addr, slotNormal.IsRead, slotNormal.IsWrite, txs[i].Hash())
//	//      }
//	//      fmt.Println()
//	//      for addr, slotChu := range *accessChu {
//	//              fmt.Println(i, addr, slotChu.IsRead, slotChu.IsWrite, txs[i].Hash())
//	//      }
//	//      fmt.Println()
//	//      fmt.Println()
//	//}
//}

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

func computing20WBlocksTxSum() {
	db, err := database.OpenDatabaseWithFreezer(&config.DefaultsEthConfig, database.DefaultRawConfig())
	if err != nil {
		fmt.Println("open leveldb", err)
		return
	}
	defer db.Close()

	var txSum uint64 = 0
	min, max, addSpan := big.NewInt(14000001), big.NewInt(14200001), big.NewInt(1)
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
	//computing20WBlocksTxSum()
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
	//conflict_detection.DetectionOverhead()
	//chukonu.TestChuKoNuLargeTPS()
	//chukonu.TestChuKoNuBlockTPS()
	//dmvcc.TestDMVCCTPSByLarge()
	//dmvcc.TestDMVCCTPSByBlock()
	//analysis.ContractRatio()
	//observation_server.ObservationHotTxParallelism()
}
