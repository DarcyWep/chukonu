package dmvcc

import (
	"bufio"
	"chukonu/config"
	"chukonu/core"
	"chukonu/core/state"
	"chukonu/core/types"
	"chukonu/core/vm"
	"chukonu/database"
	"fmt"
	"math/big"
	"os"
	"runtime"
	"time"
)

const (
	testTxsLen = 10000
	compareLen = 10000
	cpuNum     = 32
	tpsTxs     = "../data/dmvcc_tps.txt" // serial, chukonu
)

func TestDMVCCTPS() {
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

		chuKoNuProcessor.SerialSimulation(block, chuKoNuStateDB.Copy(), vm.Config{EnablePreimageRecording: false}, false)
		serialTPS, _ := chuKoNuProcessor.SerialProcessTPS(block, chuKoNuStateDB, vm.Config{EnablePreimageRecording: false})
		root, _ := chuKoNuStateDB.Commit(true)
		var chuKoNuTPS, compare float64 = 0, 0
		if block.Transactions().Len() != 0 {
			DMVCCProcessor := core.NewDMVCCProcessor(config.MainnetChainConfig, db, block, chuKoNuStateDB, cpuNum)
			chuKoNuTPS = DMVCCProcessor.DMVCC(block, chuKoNuStateDB, vm.Config{EnablePreimageRecording: false})
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
