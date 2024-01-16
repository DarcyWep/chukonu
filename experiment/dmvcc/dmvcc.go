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
	"github.com/ethereum/go-ethereum/common"
	"math/big"
	"os"
	"runtime"
	"time"
)

const (
	testTxsLen = 10000
	compareLen = 10000
	threadNum  = 32
	tpsTxs     = "../data/dmvcc_tps.txt" // serial, chukonu
)

func TestDMVCCTPSByBlock() {
	runtime.GOMAXPROCS(threadNum + 4)
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
		preRoot    common.Hash    = parent.Root
		stateCache state.Database = database.NewStateCache(db, database.DefaultStateDBConfig())
		cknTxs                    = make(core.DMVCCTxs, 0)
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
			for j, tx := range block.Transactions() {
				tx.Index = j
				cknTxs = append(cknTxs, core.NewDMVCCTx(tx, state.NewChuKoNuTxStateDB(chuKoNuStateDB), block, nil))
			}

			DMVCCProcessor := core.NewDMVCCProcessorAll(config.MainnetChainConfig, db, cknTxs, threadNum)
			runTime := DMVCCProcessor.DMVCCProcessAll(chuKoNuStateDB, vm.Config{EnablePreimageRecording: false})
			chuKoNuTPS = float64(len(cknTxs)) / runTime.Seconds()
			compare = chuKoNuTPS / serialTPS
			data = append(data, []float64{serialTPS, chuKoNuTPS, compare})
			cknTxs = cknTxs[:0]
		} else {
			data = append(data, []float64{0, 0, 0})
		}

		chuKoNuStateDB.Database().TrieDB().Reference(root, common.Hash{}) // metadata reference to keep trie alive
		chuKoNuStateDB, _ = state.NewChuKoNuStateDB(root, stateCache, nil, nil)
		chuKoNuStateDB.Database().TrieDB().Dereference(preRoot)
		preRoot = root

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

func TestDMVCCTPSByLarge() {
	runtime.GOMAXPROCS(threadNum + 4)
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
		preRoot    common.Hash    = parent.Root
		stateCache state.Database = database.NewStateCache(db, database.DefaultStateDBConfig())
	)

	chuKoNuStateDB, err := state.NewChuKoNuStateDB(parent.Root, stateCache, nil, nil)
	if err != nil {
		fmt.Println(err)
	}
	chuKoNuProcessor := core.NewChuKoNuProcessor(config.MainnetChainConfig, db)
	var (
		txsLen                      = 0
		cknTxs                      = make(core.DMVCCTxs, 0)
		serialTime    time.Duration = 0
		serialTPS     float64       = 0
		allSerialTPS  float64       = 0
		allChuKoNuTPS float64       = 0
		cknTxIndex                  = 0
		count                       = 0
	)

	min, max, addSpan := big.NewInt(14000001), big.NewInt(14020002), big.NewInt(1)
	for i := min; i.Cmp(max) == -1; i = i.Add(i, addSpan) {
		block, err2 := database.GetBlockByNumber(db, i) // 正式执行的区块
		if err2 != nil {
			fmt.Println(err2)
			return
		}

		chuKoNuProcessor.SerialSimulation(block, chuKoNuStateDB.Copy(), vm.Config{EnablePreimageRecording: false}, false)
		serialStart := time.Now()
		//_, _, rewardAccess, rewards, _ := chuKoNuProcessor.SerialSimulation(block, chuKoNuStateDB, vm.Config{EnablePreimageRecording: false}, false)
		chuKoNuProcessor.SerialSimulation(block, chuKoNuStateDB, vm.Config{EnablePreimageRecording: false}, false)
		serialTime += time.Since(serialStart)

		for _, tx := range block.Transactions() {
			tx.Index = cknTxIndex
			cknTxs = append(cknTxs, core.NewDMVCCTx(tx, state.NewChuKoNuTxStateDB(chuKoNuStateDB), block, nil))
			cknTxIndex += 1
		}
		//cknTxs = append(cknTxs, core.NewChuKoNuLargeTx(types.NewTxForChuKoNuFastLarge(common.HexToHash("0x"+block.Number().String()), rewardAccess, cknTxIndex),
		//	state.NewChuKoNuTxStateDB(chuKoNuStateDB), block, rewards))
		//cknTxIndex += 1
		//txsLen += block.Transactions().Len() + 1 // 多一个矿工奖励交易

		txsLen += block.Transactions().Len() // 多一个矿工奖励交易
		if txsLen >= compareLen && serialTPS == 0 {
			serialTPS = float64(txsLen) / serialTime.Seconds()
			allSerialTPS += serialTPS
		}
		if txsLen >= testTxsLen { // 对比 testTxsLen 个交易
			cknTxs = cknTxs[:compareLen]
			DMVCCProcessor := core.NewDMVCCProcessorAll(config.MainnetChainConfig, db, cknTxs, threadNum)
			runTime := DMVCCProcessor.DMVCCProcessAll(chuKoNuStateDB, vm.Config{EnablePreimageRecording: false})

			root, _ := chuKoNuStateDB.Commit(true)                            // 用以保证后续执行的正确性
			chuKoNuStateDB.Database().TrieDB().Reference(root, common.Hash{}) // metadata reference to keep trie alive
			chuKoNuStateDB, _ = state.NewChuKoNuStateDB(root, stateCache, nil, nil)
			chuKoNuStateDB.Database().TrieDB().Dereference(preRoot)
			preRoot = root

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
