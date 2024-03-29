package observation_server

import (
	"chukonu/config"
	"chukonu/core"
	"chukonu/core/state"
	"chukonu/core/types"
	"chukonu/core/vm"
	"chukonu/database"
	"chukonu/experiment/analysis"
	"fmt"
	"math/big"
)

func ObservationHotTxParallelism() {
	db, err := database.OpenDatabaseWithFreezer(&config.DefaultsEthConfig, database.DefaultRawConfig())
	if err != nil {
		fmt.Println("open leveldb", err)
		return
	}
	defer db.Close()
	var number uint64 = 14000000
	blockStable, err := database.GetBlockByNumber(db, new(big.Int).SetUint64(number))
	if err != nil {
		fmt.Println(err)
		return
	}

	var (
		parent     *types.Header  = blockStable.Header()
		stateCache state.Database = database.NewStateCache(db, database.DefaultStateDBConfig())
	)

	chuKoNuStateDB, err := state.NewChuKoNuStateDB(parent.Root, stateCache, nil, nil)
	if err != nil {
		fmt.Println(err)
	}

	chuKuNoProcessor := core.NewChuKoNuProcessor(config.MainnetChainConfig, db)
	var (
		txsLen                                          = 0
		txs                                             = make(types.Transactions, 0)
		accessAddrNormal      []*types.AccessAddressMap = make([]*types.AccessAddressMap, 0)
		count                                           = 0
		finish1, finish2, all                           = 0, 0, 0
		compareLen_                                     = 64 * 200
	)

	min, max, addSpan := big.NewInt(14000001), big.NewInt(14020002), big.NewInt(1)
	for i := min; i.Cmp(max) == -1; i = i.Add(i, addSpan) {
		block, err2 := database.GetBlockByNumber(db, i) // 正式执行的区块
		if err2 != nil {
			fmt.Println(err2)
			return
		}

		_, normal, _, _, _ := chuKuNoProcessor.SerialSimulation(block, chuKoNuStateDB, vm.Config{EnablePreimageRecording: false}, true)
		accessAddrNormal = append(accessAddrNormal, *normal...)

		txs = append(txs, block.Transactions()...)

		txsLen += block.Transactions().Len()
		if txsLen >= compareLen_ { // 对比 testTxsLen 个交易
			txs = txs[:compareLen_]
			for j := 0; j < compareLen_; j++ {
				txs[j].AccessSim = (accessAddrNormal)[j]
			}
			hotTxs, _, _, _, _, _ := analysis.AddressTxsAnalysis(txs)
			p1, p2 := hotTxParallelism(hotTxs)
			finish1 += p1
			finish2 += p2
			all += len(hotTxs)
			root, err := chuKoNuStateDB.Commit(true)
			if err != nil {
				fmt.Println("state db commit error", err)
				return
			}
			chuKoNuStateDB, _ = state.NewChuKoNuStateDB(root, stateCache, nil, nil)
			txsLen = 0
			txs = txs[:0]
			accessAddrNormal = accessAddrNormal[:0]
			count += 1
			if count == 10 {
				fmt.Println(float64(finish1)/float64(all)*100, float64(finish2)/float64(all)*100)
				break
			}
		}
		//fmt.Println("["+time.Now().Format("2006-01-02 15:04:05")+"]", "replay block number "+i.String())
	}
}

func hotTxParallelism(txs types.Transactions) (int, int) {
	txsLen := len(txs)
	p1 := coarseParallelism(txs, txsLen)
	p2 := fineParallelism(txs, txsLen)
	//fmt.Println(p1, p2)
	return p1, p2
}
