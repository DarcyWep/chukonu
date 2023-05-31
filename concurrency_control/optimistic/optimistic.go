package optimistic

import (
	"github.com/DarcyWep/pureData/transaction"
	"runtime"
	"sync"
	"time"
)

func Optimistic(txs []*transaction.Transaction) []bool {
	otxs := make(optimisticTxs, 0)
	for _, tx := range txs {
		otx := newOptimisticTx(*tx.Hash, tx.ExecutionTime, tx.Index)
		otx.generateReadAndWrite(tx)
		otxs = append(otxs, otx)
	}
	return processOptimistic(otxs, newStateDB())
}

func processOptimistic(otxs optimisticTxs, statedb *stateDB) []bool {
	var (
		proNum    = runtime.NumCPU()
		proWg     sync.WaitGroup
		proChan   chan *optimisticTx = make(chan *optimisticTx, 512)
		abortChan chan *optimisticTx = make(chan *optimisticTx, 512)

		mutex sync.RWMutex
	)
	runtime.GOMAXPROCS(proNum)
	proWg.Add(proNum)
	for i := 0; i < proNum; i++ {
		go executeOptimisticTx(proChan, abortChan, statedb, &proWg, &mutex)
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

func executeOptimisticTx(proCh, abortChan chan *optimisticTx, statedb *stateDB, wg *sync.WaitGroup, mutex *sync.RWMutex) {
	defer wg.Done()
	for otx := range proCh {
		mutex.RLock() // 读取所需的状态
		//for addr, _ := range otx.readAddresses {
		//	otx.readAddresses[addr] = statedb.getState(addr)
		//}
		for addr, _ := range otx.writeAddress {
			otx.writeAddress[addr] = statedb.getState(addr)
		}
		mutex.RUnlock() // 读取所需的状态

		time.Sleep(otx.executionTime) // 模拟执行

		abort := false
		// 验证交易
		mutex.Lock()
		//for addr, value := range otx.allAddress {
		//for addr, value := range otx.writeAddress {
		//	abort = statedb.setState(addr, value, otx.index) // 无预设顺序
		//	if abort {
		//		break
		//	}
		//}
		//for addr, value := range otx.readAddresses {
		//	newValue := statedb.getState(addr)
		//	if newValue != value {
		//		abort = true
		//		break
		//	}
		//}
		if !abort {
			for addr, value := range otx.writeAddress {
				abort = statedb.setState(addr, value, otx.index) // 无预设顺序
				if abort {
					break
				}
			}
		}
		mutex.Unlock() // 验证交易
		if abort {
			abortChan <- otx
		}
	}
}
