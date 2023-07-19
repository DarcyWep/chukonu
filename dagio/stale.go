package dagio

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

const (
	blockTxsLen = 200
	testTxsLen  = 25600
)

func staleCompare() {
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
	statePre, _ := state.New(parent.Root, stateCache, snaps)

	processor := core.NewStateProcessor(config.MainnetChainConfig, db)
	stmProcessor := core.NewStmStateProcessor(config.MainnetChainConfig, db)

	var (
		txsLen                                     = 0
		txs                                        = make(types.Transactions, 0)
		accessAddrNormal []*types.AccessAddressMap = make([]*types.AccessAddressMap, 0)
		accessAddrChu    []*types.AccessAddressMap = make([]*types.AccessAddressMap, 0)
	)
	//min, max, addSpan := big.NewInt(9776810), big.NewInt(9776811), big.NewInt(1)

	min, max, addSpan := big.NewInt(9776810), big.NewInt(9776980), big.NewInt(1)
	for i := min; i.Cmp(max) == -1; i = i.Add(i, addSpan) {

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
			fmt.Println("full 256")
			break
		}
		fmt.Println("["+time.Now().Format("2006-01-02 15:04:05")+"]", "replay block number "+i.String())
	}
	txStaleNum := txStale(&accessAddrNormal, &accessAddrChu, txs)
	epochStaleNum := epochStale(&accessAddrNormal, &accessAddrChu)

	fmt.Println(txStaleNum, epochStaleNum)
}

func txStale(accessAddrNormal *[]*types.AccessAddressMap, accessAddrChu *[]*types.AccessAddressMap, txs types.Transactions) int {
	staleStateNum := 0
	staleTxs := make([]bool, len(txs))
	for i, accessNormal := range *accessAddrNormal {
		if i == testTxsLen {
			break
		}
		accessChu := (*accessAddrChu)[i]

		for addr, _ := range *accessNormal {
			if _, ok := (*accessChu)[addr]; !ok { // 正确中有，而模拟的没有，则不一致会影响之后的并发调度，可能造成丢弃；正确中没有，模拟中有，不会影响并发的丢弃
				staleTxs[i] = true
				staleStateNum += 1
			}
		}
	}
	// 地址所对应的热交易, 热交易执行前准备
	addrMapTxIndex := make(map[common.Address][]int, 0) // address -> txIndex
	for i, _ := range txs {
		if i == testTxsLen {
			break
		}
		accessChu := (*accessAddrChu)[i]
		for addr, _ := range *accessChu {
			if _, ok := addrMapTxIndex[addr]; !ok {
				addrMapTxIndex[addr] = make([]int, 0)
			}
			addrMapTxIndex[addr] = append(addrMapTxIndex[addr], i)
		}
	}

	txAccessNum := make([]int, len(txs))
	for addr, txIndexes := range addrMapTxIndex {
		stale := false
		for _, txIndex := range txIndexes {
			if stale {
				staleStateNum += 1
				break
			}
			txAccessNum[txIndex] += 1
			accessChu := (*accessAddrChu)[txIndex]
			if (*accessChu)[addr].IsWrite {
				stale = true
			}
		}
	}
	for i, accessChu := range *accessAddrChu {
		if i == testTxsLen {
			break
		}
		if len(*accessChu) != txAccessNum[i] {
			staleTxs[i] = true
		}
	}

	staleNum := 0
	for _, stale := range staleTxs {
		if stale {
			staleNum += 1
		}
	}
	//return staleNum
	return staleStateNum
}

func epochStale(accessAddrNormal *[]*types.AccessAddressMap, accessAddrChu *[]*types.AccessAddressMap) int {
	staleStateNum := 0
	allAddressNormal, allAddressChu := make(map[common.Address]struct{}), make(map[common.Address]struct{})
	for i, accessNormal := range *accessAddrNormal {
		if i == testTxsLen {
			break
		}

		for addr, _ := range *accessNormal {
			if _, ok := allAddressNormal[addr]; !ok { // 未加入
				allAddressNormal[addr] = struct{}{}
			}
		}
		accessChu := (*accessAddrChu)[i]
		for addr, _ := range *accessChu {
			if _, ok := allAddressChu[addr]; !ok { // 未加入
				allAddressChu[addr] = struct{}{}
			}
		}
	}

	for addr, _ := range allAddressNormal {
		if _, ok := allAddressChu[addr]; !ok {
			staleStateNum += 1
		}
	}

	return staleStateNum
}
