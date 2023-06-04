package optimistic

import (
	"chukonu/concurrency_control/conflict/nezha/core/state"
	"chukonu/setting"
	"github.com/DarcyWep/pureData/transaction"
	"github.com/ethereum/go-ethereum/common"
	"runtime"
	"sync"
	"time"
)

func PreSetOrderOptimistic(txs []*transaction.Transaction, db *state.StateDB) []bool {
	otxs := make(optimisticTxs, 0)
	for _, tx := range txs {
		otx := newOptimisticTx(*tx.Hash, tx.ExecutionTime, tx.Index)
		otx.generateReadAndWrite(tx)
		otxs = append(otxs, otx)
	}
	initPresetVersion(otxs) // 预定的顺序
	return process(otxs, newStateDB())
}

func initPresetVersion(otxs optimisticTxs) {
	var address2OptimisticTxs = make(map[common.Address]optimisticTxs, 0)

	// 讲交易与地址对应
	for _, otx := range otxs {
		for addr, _ := range otx.allAddress {
			if _, ok := address2OptimisticTxs[addr]; !ok { // 第一笔相关交易
				address2OptimisticTxs[addr] = make(optimisticTxs, 0)
			}
			address2OptimisticTxs[addr] = append(address2OptimisticTxs[addr], otx)
		}
	}

	for addr, newOtxs := range address2OptimisticTxs {
		for i, otx := range newOtxs {
			if i == 0 { // 等于0 时，所有的版本默认为-1
				otx.presetVersion[addr] = i - 1
				continue
			}
			if _, ok := newOtxs[i-1].writeAddress[addr]; ok { // 如果上个交易写了，则更改当前交易的版本
				otx.presetVersion[addr] = i - 1
			}
		}
	}
}

func process(otxs optimisticTxs, statedb *stateDB) []bool {
	var (
		proNum    = runtime.NumCPU()
		proWg     sync.WaitGroup
		proChan   chan *optimisticTx = make(chan *optimisticTx, setting.OptimisticChanSize)
		abortChan chan *optimisticTx = make(chan *optimisticTx, setting.OptimisticChanSize)

		mutex sync.RWMutex
	)
	runtime.GOMAXPROCS(proNum)
	proWg.Add(proNum)
	for i := 0; i < proNum; i++ {
		go executeTx(proChan, abortChan, statedb, &proWg, &mutex)
	}

	for _, otx := range otxs {
		proChan <- otx
	}
	close(proChan)
	proWg.Wait()

	close(abortChan)
	isAbort := make([]bool, len(otxs))
	for otx := range abortChan {
		isAbort[otx.index] = true
	}
	return isAbort
}

func executeTx(proCh, abortChan chan *optimisticTx, statedb *stateDB, wg *sync.WaitGroup, mutex *sync.RWMutex) {
	defer wg.Done()
	for otx := range proCh {
		abort := false

		mutex.RLock() // 读取所需的状态
		for addr, _ := range otx.allAddress {
			read := statedb.getState(addr)
			if read != otx.presetVersion[addr] { // 版本号对不上，直接丢弃
				abort = true
				break
			}
			otx.allAddress[addr] = read
		}
		mutex.RUnlock() // 读取所需的状态

		if !abort { // 交易未被丢弃，则执行交易
			time.Sleep(otx.executionTime) // 模拟执行

			mutex.Lock() // 验证交易
			for addr, value := range otx.allAddress {
				abort = statedb.setState(addr, value, otx.index) // 无预设顺序
				if abort {
					break
				}
			}
			mutex.Unlock() // 验证交易
		}

		if abort {
			abortChan <- otx
		}
	}
}
