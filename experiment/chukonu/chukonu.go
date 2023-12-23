package chukonu

import (
	"bufio"
	"chukonu/config"
	"chukonu/core"
	"chukonu/core/state"
	"chukonu/core/types"
	"chukonu/core/vm"
	"chukonu/database"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"math/big"
	"os"
	"runtime"
	"time"
)

const (
	testTxsLen = 10000
	compareLen = 10000
	tpsTxs     = "../data/tps.txt" // serial, chukonu
)

var cpuNum = 32

func TestChuKoNuLargeTPS() {
	runtime.GOMAXPROCS(cpuNum)
	time.Sleep(100 * time.Millisecond)
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
	chuKoNuProcessor := core.NewChuKoNuProcessor(config.MainnetChainConfig, db)
	var (
		txsLen                      = 0
		cknTxs                      = make(core.ChuKoNuLargeTxs, 0)
		serialTime    time.Duration = 0
		serialTPS     float64       = 0
		allSerialTPS  float64       = 0
		allChuKoNuTPS float64       = 0
		cknTxIndex                  = 0
		count                       = 0
		//all1, all2, all3, all4, all5 time.Duration = 0, 0, 0, 0, 0
	)

	min, max, addSpan := big.NewInt(14000001), big.NewInt(14020002), big.NewInt(1)
	for i := min; i.Cmp(max) == -1; i = i.Add(i, addSpan) {
		block, err2 := database.GetBlockByNumber(db, i) // 正式执行的区块
		if err2 != nil {
			fmt.Println(err2)
			return
		}
		serialStart := time.Now()
		chuKoNuProcessor.SerialSimulation(block, chuKoNuStateDB.Copy(), vm.Config{EnablePreimageRecording: false})
		serialTime += time.Since(serialStart)
		_, _, rewardAccess, rewards, _ := chuKoNuProcessor.SerialSimulation(block, chuKoNuStateDB, vm.Config{EnablePreimageRecording: false})

		for _, tx := range block.Transactions() {
			tx.Index = cknTxIndex
			cknTxs = append(cknTxs, core.NewChuKoNuLargeTx(tx, state.NewChuKoNuTxStateDB(chuKoNuStateDB), block, nil))
			cknTxIndex += 1
		}
		cknTxs = append(cknTxs, core.NewChuKoNuLargeTx(types.NewTxForChuKoNuFastLarge(common.HexToHash("0x"+block.Number().String()), rewardAccess, cknTxIndex),
			state.NewChuKoNuTxStateDB(chuKoNuStateDB), block, rewards))
		cknTxIndex += 1

		txsLen += block.Transactions().Len() + 1 // 多一个矿工奖励交易
		if txsLen >= compareLen && serialTPS == 0 {
			serialTPS = float64(txsLen) / serialTime.Seconds()
			allSerialTPS += serialTPS
		}
		if txsLen >= testTxsLen { // 对比 testTxsLen 个交易
			cknTxs = cknTxs[:compareLen]
			root, _ := chuKoNuStateDB.Commit(true) // 用以保证后续执行的正确性

			chuKoNuFastProcessor := core.NewChuKoNuFastLargeProcessor(config.MainnetChainConfig, db, cknTxs, chuKoNuStateDB)
			runTime := chuKoNuFastProcessor.ChuKoNuFast(chuKoNuStateDB, vm.Config{EnablePreimageRecording: false})

			chuKoNuStateDB, _ = state.NewChuKoNuStateDB(root, stateCache, nil, nil)

			allChuKoNuTPS += float64(compareLen) / runTime.Seconds()
			txsLen = 0
			cknTxs = cknTxs[:0]
			serialTime = 0
			serialTPS = 0
			cknTxIndex = 0
			count += 1
			if count == 10 {
				fmt.Println("Serial TPS:", allSerialTPS/10)
				fmt.Println("ChuKoNu TPS:", allChuKoNuTPS/10)
				break
			}
		}
		fmt.Println("["+time.Now().Format("2006-01-02 15:04:05")+"]", "replay block number "+i.String())
	}
}

func TestChuKoNuTPS() {
	runtime.GOMAXPROCS(cpuNum)
	time.Sleep(100 * time.Millisecond)
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
	var data [][]float64 = make([][]float64, 0)

	min, max, addSpan := big.NewInt(14000001), big.NewInt(14020001), big.NewInt(1)
	for i := min; i.Cmp(max) == -1; i = i.Add(i, addSpan) {
		block, err2 := database.GetBlockByNumber(db, i) // 正式执行的区块
		if err2 != nil {
			fmt.Println(err2)
			return
		}

		chuKoNuProcessor.SerialSimulation(block, chuKoNuStateDB.Copy(), vm.Config{EnablePreimageRecording: false})
		serialTPS, _ := chuKoNuProcessor.SerialProcessTPS(block, chuKoNuStateDB, vm.Config{EnablePreimageRecording: false})
		root, _ := chuKoNuStateDB.Commit(true)
		var chuKoNuTPS, compare float64 = 0, 0
		if block.Transactions().Len() != 0 {
			chuKoNuFastProcessor := core.NewChuKoNuFastProcessor(config.MainnetChainConfig, db, block, chuKoNuStateDB)
			chuKoNuTPS = chuKoNuFastProcessor.ChuKoNuFast(block, chuKoNuStateDB, vm.Config{EnablePreimageRecording: false})
			compare = chuKoNuTPS / serialTPS
			data = append(data, []float64{serialTPS, chuKoNuTPS, compare})
		}

		chuKoNuStateDB, _ = state.NewChuKoNuStateDB(root, stateCache, nil, nil)
		fmt.Println("["+time.Now().Format("2006-01-02 15:04:05")+"]", "replay block number "+i.String(), serialTPS, chuKoNuTPS, compare)
	}

	// 打开或创建一个文本文件，如果文件已存在则会被覆盖
	file, err := os.Create(tpsTxs)
	if err != nil {
		fmt.Println("无法创建文件:", err)
		return
	}
	defer file.Close()

	// 创建一个写入器，用于将数据写入文件
	writer := bufio.NewWriter(file)

	// 将数据写入文件
	for _, row := range data {
		_, err := fmt.Fprintf(writer, "%.2f %.2f %.2f\n", row[0], row[1], row[2])
		if err != nil {
			fmt.Println("写入文件失败:", err)
			return
		}
	}

	// 刷新缓冲区以确保数据被写入文件
	err = writer.Flush()
	if err != nil {
		fmt.Println("刷新缓冲区失败:", err)
		return
	}
	fmt.Println("文件写入成功！")
}
