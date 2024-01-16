package observation_server

import (
	"chukonu/config"
	"chukonu/core"
	"chukonu/core/state"
	"chukonu/core/types"
	"chukonu/core/vm"
	"chukonu/database"
	"chukonu/experiment/observation/classic"
	"chukonu/setting"
	"fmt"
	"github.com/DarcyWep/pureData"
	"github.com/DarcyWep/pureData/transaction"
	"github.com/ethereum/go-ethereum/common"
	"math/big"
	"strings"
	"time"
)

const (
	testTxsLen = 10000
	compareLen = 10000
)

func ObservationParallelism() {
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
		txsLen                                     = 0
		txs                                        = make(types.Transactions, 0)
		accessAddrNormal []*types.AccessAddressMap = make([]*types.AccessAddressMap, 0)
		count                                      = 0
		all1, all2                                 = 0, 0
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
		if txsLen >= testTxsLen { // 对比 testTxsLen 个交易
			p1, p2 := parallelism(&accessAddrNormal, txs)
			all1 += p1
			all2 += p2
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
				fmt.Println(all1/10, all2/10)
				break
			}
		}
		//fmt.Println("["+time.Now().Format("2006-01-02 15:04:05")+"]", "replay block number "+i.String())
	}
}

func ObservationConflictsDetection() {
	db, err := setting.OpenLeveldb("/Users/darcywep/Projects/GoProjects/chukonu/data/new_nativedb") // get native transaction or merge transaction
	defer db.Close()
	if err != nil {
		fmt.Println("open leveldb error,", err)
		return
	}
	val, _ := db.Get([]byte("block.Coinbase"), nil)
	addresses := strings.Split(string(val), " ")
	coinbaseMap := make(map[string]string, 0)
	min, max, addSpan := big.NewInt(14000001), big.NewInt(14200001), big.NewInt(1)
	var k = 0
	for i := new(big.Int).Set(min); i.Cmp(max) == -1; i = i.Add(i, addSpan) {
		coinbaseMap[i.String()] = addresses[k]
		k++
	}
	//for key, val := range coinbaseMap {
	//	fmt.Println(key, val)
	//}

	allTxs := make([]*transaction.Transaction, 0)
	k = 0
	times := 10000
	var allT1, allT2, allT3 time.Duration = 0, 0, 0
	testSpan := 500
	for i := min; i.Cmp(max) == -1; i = i.Add(i, addSpan) {
		txs, err := pureData.GetTransactionsByNumber(db, i)
		if err != nil {
			fmt.Println(err)
			return
		}
		txs = txs[:len(txs)-1]
		for _, tx := range txs {
			if tx.From.Hex() != coinbaseMap[i.String()] && tx.To != nil && tx.To.Hex() != coinbaseMap[i.String()] {
				delete(*tx.AccessAddress, common.HexToAddress(coinbaseMap[i.String()]))
			}
			allTxs = append(allTxs, tx)
		}
		if len(allTxs) >= testSpan {
			//parallelism(allTxs[:testSpan], testSpan)
			t1, t2, t3 := detectionOverhead(allTxs[:testSpan], testSpan, k)
			allT1, allT2, allT3 = allT1+t1, allT2+t2, allT3+t3
			k += 1
			if k == times {
				fmt.Println(time.Duration(allT1.Nanoseconds()/int64(times)).Nanoseconds(),
					time.Duration(allT2.Nanoseconds()/int64(times)).Nanoseconds(),
					time.Duration(allT3.Nanoseconds()/int64(times)).Nanoseconds())
				break
			}
			allTxs = allTxs[testSpan:]
		}
	}
}

func parallelism(accessAddresses *[]*types.AccessAddressMap, txs types.Transactions) (int, int) {
	for i := 0; i < compareLen; i++ {
		txs[i].AccessSim = (*accessAddresses)[i]
	}
	txs = txs[:compareLen]
	p1 := coarseParallelism(txs, compareLen)
	p2 := fineParallelism(txs, compareLen)
	fmt.Println(p1, p2)
	return p1, p2
}

func coarseParallelism(txs types.Transactions, testSpan int) int {
	needAddress := make([]int, testSpan)
	getAddress := make([]int, testSpan)
	accessSequence := make(map[common.Address][]*transaction.Transaction)
	accessToken := make(map[common.Address]bool)
	for i, tx := range txs {
		needAddress[i] = len(*tx.AccessSim)
		for addr, accessAddress := range *tx.AccessSim {
			if _, ok := accessSequence[addr]; !ok {
				accessSequence[addr] = make([]*transaction.Transaction, 0)
				accessToken[addr] = false
			}
			if !accessToken[addr] {
				getAddress[i] += 1
				if accessAddress.CoarseWrite {
					accessToken[addr] = true
				}
			}
		}
	}
	p := 0
	for i, num := range needAddress {
		if num == getAddress[i] {
			p += 1
		}
	}
	return p
}

func fineParallelism(txs types.Transactions, testSpan int) int {
	needAddress := make([]int, testSpan)
	getAddress := make([]int, testSpan)
	accessSequence := make(map[string][]*transaction.Transaction)
	accessToken := make(map[string]bool)
	for i, tx := range txs {
		for addr, accessAddress := range *tx.AccessSim {
			if accessAddress.IsRead || accessAddress.IsWrite { // 读/写了账户状态
				//if *tx.Hash == common.HexToHash("0xc2f8da59f37c506e4d398ca32e2c70baa0f522e76d2b6f83fa3fd20491888e67") {
				//	fmt.Println(tx.Hash, addr, tx.Contract, accessAddress.IsRead, accessAddress.IsWrite)
				//}

				needAddress[i] += 1
				if _, ok := accessSequence[addr.Hex()]; !ok {
					accessSequence[addr.Hex()] = make([]*transaction.Transaction, 0)
					accessToken[addr.Hex()] = false
				}
				if !accessToken[addr.Hex()] {
					getAddress[i] += 1
					if accessAddress.IsWrite {
						accessToken[addr.Hex()] = true
					}
				}
			} else {

			}

			for slot, accessSlot := range *accessAddress.Slots {
				needAddress[i] += 1
				slotKey := addr.Hex() + "_" + slot.Hex()
				if _, ok := accessSequence[slotKey]; !ok {
					accessSequence[slotKey] = make([]*transaction.Transaction, 0)
					accessToken[slotKey] = false
				}
				if !accessToken[slotKey] {
					getAddress[i] += 1
					if accessSlot.IsWrite {
						accessToken[slotKey] = true
					}
				}
			}
		}
	}
	p := 0
	for i, num := range needAddress {
		if num == getAddress[i] {
			p += 1
		}
	}
	return p
}

func detectionOverhead(txs []*transaction.Transaction, testSpan int, num int) (time.Duration, time.Duration, time.Duration) {
	t1 := coarseDetectionOverhead(txs, testSpan)
	t2 := fineDetectionOverhead(txs, testSpan)
	t3 := classic.ClassicDetectionOverhead(txs)
	fmt.Println(num, t1.Microseconds(), t2.Microseconds(), t3.Microseconds())
	return t1, t2, t3
}

func coarseDetectionOverhead(txs []*transaction.Transaction, testSpan int) time.Duration {
	start := time.Now()
	needAddress := make([]int, testSpan)
	getAddress := make([]int, testSpan)
	accessSequence := make(map[common.Address][]*transaction.Transaction)
	accessToken := make(map[common.Address]bool)
	for i, tx := range txs {
		needAddress[i] = len(*tx.AccessAddress)
		for addr, accessAddress := range *tx.AccessAddress {
			if _, ok := accessSequence[addr]; !ok {
				accessSequence[addr] = make([]*transaction.Transaction, 0)
				accessToken[addr] = false
			}
			if !accessToken[addr] {
				getAddress[i] += 1
				if accessAddress.CoarseWrite {
					accessToken[addr] = true
				}
			}
		}
	}
	p := 0
	for i, num := range needAddress {
		if num == getAddress[i] {
			p += 1
		}
	}
	return time.Since(start)
}

func fineDetectionOverhead(txs []*transaction.Transaction, testSpan int) time.Duration {
	start := time.Now()
	needAddress := make([]int, testSpan)
	getAddress := make([]int, testSpan)
	accessSequence := make(map[string][]*transaction.Transaction)
	accessToken := make(map[string]bool)
	for i, tx := range txs {
		for addr, accessAddress := range *tx.AccessAddress {
			if accessAddress.IsRead || accessAddress.IsWrite { // 读/写了账户状态
				//if *tx.Hash == common.HexToHash("0xc2f8da59f37c506e4d398ca32e2c70baa0f522e76d2b6f83fa3fd20491888e67") {
				//	fmt.Println(tx.Hash, addr, tx.Contract, accessAddress.IsRead, accessAddress.IsWrite)
				//}

				needAddress[i] += 1
				if _, ok := accessSequence[addr.Hex()]; !ok {
					accessSequence[addr.Hex()] = make([]*transaction.Transaction, 0)
					accessToken[addr.Hex()] = false
				}
				if !accessToken[addr.Hex()] {
					getAddress[i] += 1
					if accessAddress.IsWrite {
						accessToken[addr.Hex()] = true
					}
				}
			} else {

			}

			for slot, accessSlot := range *accessAddress.Slots {
				needAddress[i] += 1
				slotKey := addr.Hex() + "_" + slot.Hex()
				if _, ok := accessSequence[slotKey]; !ok {
					accessSequence[slotKey] = make([]*transaction.Transaction, 0)
					accessToken[slotKey] = false
				}
				if !accessToken[slotKey] {
					getAddress[i] += 1
					if accessSlot.IsWrite {
						accessToken[slotKey] = true
					}
				}
			}
		}
	}
	p := 0
	for i, num := range needAddress {
		if num == getAddress[i] {
			p += 1
		}
	}
	return time.Since(start)
}
