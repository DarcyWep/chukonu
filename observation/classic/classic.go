package classic

import (
	"chukonu/setting"
	"github.com/DarcyWep/pureData/transaction"
	"github.com/ethereum/go-ethereum/common"
	"runtime"
	"sync"
	"time"
)

func ClassicDetectionOverhead(txs []*transaction.Transaction) time.Duration {
	startTime := time.Now()
	cgtxs := make(classicGraphTxs, 0)
	for _, tx := range txs {
		cgtx := newClassicGraphTx(tx)
		cgtx.generateReadAndWrite(tx)
		cgtxs = append(cgtxs, cgtx)
	}
	graph := buildConflictGraph(cgtxs)
	return time.Since(startTime)
	ls := newLoopSolving(&graph)
	ls.removeLoops()
	////process(graph, &cgtxs, ls.deleteNodes)
	return time.Since(startTime)
	//return float64(len(txs)) / time.Since(startTime).Seconds()
}

func buildConflictGraph(cgtxs classicGraphTxs) [][]int {
	var gSlice = make([][]int, len(cgtxs))

	for i, cgtx := range cgtxs {

		for j := i + 1; j < len(cgtxs); j++ {
			for _, addr := range cgtxs[j].writeAddress {
				if isExistAddrInSlice(cgtx.readAddresses, addr) {
					// build r-w dependency
					if !isExistIntInSlice(gSlice[i], j) {
						gSlice[i] = append(gSlice[i], j)
					}
				} else if isExistAddrInSlice(cgtx.writeAddress, addr) {
					// build w-w dependency
					if !isExistIntInSlice(gSlice[i], j) {
						gSlice[i] = append(gSlice[i], j)
					}
				}
			}
		}

		for k := i - 1; k >= 0; k-- {
			for _, addr := range cgtxs[k].writeAddress {
				if isExistAddrInSlice(cgtx.writeAddress, addr) {
					// build r-w dependency
					if !isExistIntInSlice(gSlice[i], k) {
						gSlice[i] = append(gSlice[i], k)
					}
				}
			}
		}
	}

	return gSlice
}

func process(graph [][]int, cgtxs *classicGraphTxs, deleteNodes []bool) {
	var (
		proNum    = runtime.NumCPU()
		proWg     sync.WaitGroup
		proChan   chan *classicGraphTx = make(chan *classicGraphTx, setting.OptimisticChanSize)
		closeChan chan struct{}        = make(chan struct{}, setting.OptimisticChanSize)
		abortSum                       = 0
	)
	for node, children := range graph {
		if deleteNodes[node] { // node has been deleted, continue
			(*cgtxs)[node].inDegree = -1 // 结点被删除
			abortSum += 1
			continue
		}
		for _, child := range children {
			if deleteNodes[child] { // node has been deleted, continue
				continue
			}
			(*cgtxs)[child].inDegree += 1 // 孩子的入度+1
		}
	}

	runtime.GOMAXPROCS(proNum)
	proWg.Add(proNum)
	for i := 0; i < proNum; i++ {
		go executeTx(proChan, closeChan, &graph, &deleteNodes, cgtxs, &proWg)
	}

	for _, cgtx := range *cgtxs {
		if cgtx.inDegree == 0 {
			proChan <- cgtx
		}
	}
	go closeClassic(proChan, closeChan, len(*cgtxs)-abortSum)
	proWg.Wait()

	for i, abort := range deleteNodes {
		if abort {
			time.Sleep((*cgtxs)[i].tx.ExecutionTime)
		}
	}
}

func executeTx(proCh chan *classicGraphTx, closeChan chan struct{}, graph *[][]int, deleteNodes *[]bool, cgtxs *classicGraphTxs, wg *sync.WaitGroup) {
	defer wg.Done()
	for cgtx := range proCh {
		//time.Sleep(cgtx.tx.ExecutionTime + 10*time.Microsecond) // 模拟执行
		time.Sleep(cgtx.tx.ExecutionTime) // 模拟执行

		// 已执行完毕， 调整入度
		cgtx.inDegree = -1
		// 清除其与孩子结点的依赖
		for _, child := range (*graph)[cgtx.index] {
			if (*deleteNodes)[child] { // node has been deleted, continue
				continue
			}
			tx := (*cgtxs)[child]
			tx.mutex.Lock()
			tx.inDegree -= 1
			if tx.inDegree == 0 {
				proCh <- tx
			}
			tx.mutex.Unlock()
		}

		closeChan <- struct{}{} // 完成事务
	}
}

func closeClassic(proCh chan *classicGraphTx, closeChan chan struct{}, txSum int) {
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

func isExistAddrInSlice(addresses []common.Address, address common.Address) bool {
	for _, addr := range addresses {
		if address == addr {
			return true
		}
	}
	return false
}

func isExistIntInSlice(numbers []int, number int) bool {
	for _, num := range numbers {
		if number == num {
			return true
		}
	}
	return false
}
