package blockstm

import (
	"chukonu/setting"
	"github.com/DarcyWep/pureData/transaction"
	"github.com/ethereum/go-ethereum/common"
	"runtime"
	"sync"
	"time"
)

func BlockSTM(txs []*transaction.Transaction) float64 {
	btxs := make(blockStmTxs, 0)
	for _, tx := range txs {
		btx := newBlockStmTx(tx)
		btx.generateReadAndWrite(tx)
		btxs = append(btxs, btx)
	}
	return float64(len(btxs)) / process(btxs, newStateDB()).Seconds()
}

func initPresetVersion(btxs blockStmTxs) {
	var address2OptimisticTxs = make(map[common.Address]blockStmTxs, 0)
	// 将交易与地址对应
	for _, btx := range btxs {
		for addr, _ := range btx.allAddress {
			if _, ok := address2OptimisticTxs[addr]; !ok { // 第一笔相关交易
				address2OptimisticTxs[addr] = make(blockStmTxs, 0)
			}
			address2OptimisticTxs[addr] = append(address2OptimisticTxs[addr], btx)
		}
	}

	//first := make([]int, 0)
	for addr, newOtxs := range address2OptimisticTxs {
		//if addr == common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48") {
		//	for _, tx := range newOtxs {
		//		fmt.Printf("%d ", tx.index)
		//	}
		//	fmt.Println()
		//}
		lastWrite := -1 // 每个地址的第一笔交易，版本默认为-1
		onlyRead := make([]int, 0)
		for _, btx := range newOtxs {
			btx.presetVersion[addr] = lastWrite

			// 如果上个交易写了，则更改当前交易的版本(但是上一个交易可能没写，而上上个交易写了)
			// 如果写了该地址，则为下一个读取的交易更新版本
			// TODO: 写更改，要检查前面的读是否完成
			if _, ok := btx.writeAddress[addr]; ok { //
				lastWrite = btx.index   // 这里应该是前一个交易的index，不是i-1
				if len(onlyRead) != 0 { // 如果之前有纯读, 那么需要等待读的交易完成才能进行该交易的写入
					for _, txIndex := range onlyRead {
						addIntToSlice(&btx.preTxs, txIndex)              // 将该交易的前序读交易写入
						addIntToSlice(&btxs[txIndex].nextTxs, btx.index) // 将该交易写入其前序交易的激活列表
					}
					onlyRead = make([]int, 0) // *
				}
			} else {
				onlyRead = append(onlyRead, btx.index)
			}
			//if btx.index == 150 || btx.index == 116 {
			//	fmt.Println(btx.index)
			//	fmt.Println(btx.allAddress)
			//	fmt.Println(btx.writeAddress)
			//	fmt.Println(btx.presetVersion)
			//	fmt.Println()
			//}

		}
	}

	for _, btx := range btxs {
		btx.preSum = len(btx.preTxs)
	}

	//for _, tx := range btxs {
	//	flag := false
	//	for _, version := range tx.presetVersion {
	//		if version != -1 {
	//			flag = true
	//			break
	//		}
	//	}
	//	if !flag {
	//		first = append(first, tx.index)
	//	}
	//}
	//fmt.Println(first)
}

func process(btxs blockStmTxs, statedb *stateDB) time.Duration {
	initPresetVersion(btxs) // 预定的顺序

	var (
		proNum    = runtime.NumCPU()
		proWg     sync.WaitGroup
		proChan   chan *blockStmTx = make(chan *blockStmTx, setting.OptimisticChanSize)
		closeChan chan struct{}    = make(chan struct{}, setting.OptimisticChanSize)

		mutex sync.RWMutex
	)
	startTime := time.Now()
	runtime.GOMAXPROCS(proNum)
	proWg.Add(proNum)
	for i := 0; i < proNum; i++ {
		go executeTx(proChan, closeChan, statedb, &btxs, &proWg, &mutex)
	}

	for _, btx := range btxs {
		proChan <- btx
	}
	go closeStm(proChan, closeChan, len(btxs))
	proWg.Wait()

	return time.Since(startTime)
}

func executeTx(proCh chan *blockStmTx, closeChan chan struct{}, statedb *stateDB, btxs *blockStmTxs, wg *sync.WaitGroup, mutex *sync.RWMutex) {
	defer wg.Done()
	for btx := range proCh {
		abort := false

		btx.mutex.RLock()
		if btx.preNum != btx.preSum {
			abort = true
		}
		btx.mutex.RUnlock()

		if !abort {
			mutex.RLock() // 读取所需的状态
			for addr, _ := range btx.allAddress {
				read := statedb.getState(addr)
				//if btx.index == 127 {
				//	fmt.Println(addr, read)
				//}
				//fmt.Println(read)
				//newRead := -2
				////abortTxs := make([]int, 0)
				//if read > btx.index {
				//	_, newRead = statedb.abortCommitTx(addr, btx.index)
				//	//fmt.Println(btx.index, abortTxs)
				//}
				//if newRead != -2 {
				//	read = newRead
				//}
				if read != btx.presetVersion[addr] { // 版本号对不上，直接丢弃
					abort = true
					break
				}
				btx.allAddress[addr] = read
			}
			mutex.RUnlock() // 读取所需的状态
		}

		if !abort { // 交易未被丢弃，则执行交易
			time.Sleep(btx.tx.ExecutionTime) // 模拟执行

			mutex.Lock() // 验证交易
			for addr, value := range btx.allAddress {
				abort = statedb.validate(addr, value)
				if abort {
					break
				}
			}
			if !abort {
				for addr, _ := range btx.writeAddress {
					statedb.setState(addr, btx.index) // 如果还没执行的地址，有相同地读，但是未改，这里改了，导致前一个交易无法执行了
				}
			}
			mutex.Unlock() // 验证交易
		}

		if abort {
			proCh <- btx
		} else {
			//fmt.Println(btx.index)
			for _, txIndex := range btx.nextTxs { // 告诉下一个交易，我完成了
				tx := (*btxs)[txIndex]
				tx.mutex.Lock()
				tx.preNum += 1
				tx.mutex.Unlock()
			}
			closeChan <- struct{}{}
		}
	}
}

func closeStm(proCh chan *blockStmTx, closeChan chan struct{}, txSum int) {
	var num = 0
	for _ = range closeChan {
		num += 1
		//fmt.Println("num:", num)
		if num == txSum {
			close(closeChan)
			close(proCh)
		}
	}
}

func addIntToSlice(slice *[]int, i int) {
	isIn := false
	for _, in := range *slice {
		if in == i {
			isIn = true
			break
		}
	}
	if !isIn {
		*slice = append(*slice, i)
	}
}
