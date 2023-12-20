package classic

import (
	"chukonu/core/types"
	"github.com/ethereum/go-ethereum/common"
	"time"
)

func ClassicDetectionOverhead(txs types.Transactions) time.Duration {
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
