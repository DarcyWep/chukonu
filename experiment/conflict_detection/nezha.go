package conflict_detection

import (
	"chukonu/concurrency_control/conflict/nezha"
	"chukonu/concurrency_control/conflict/nezha/core/state"
	"chukonu/concurrency_control/conflict/nezha/core/tp"
	"chukonu/concurrency_control/conflict/nezha/core/types"
	coretypes "chukonu/core/types"
	"github.com/ethereum/go-ethereum/common"
	"runtime"
	"strings"
	"sync"
	"time"
)

//const dbFile = "../data/Morph_Test3"

func nezhaDetection(txs coretypes.Transactions, statedb *state.StateDB) time.Duration {

	runtime.GOMAXPROCS(runtime.NumCPU())

	var ttxs = make(map[string][]*types.Transaction)
	ttxs["0"] = createBatchTxs(txs)

	statedb.BatchCreateObjects(ttxs)
	return runTestNezha(ttxs, statedb)
}

func runTestNezha(txs map[string][]*types.Transaction, state *state.StateDB) time.Duration {
	var wg sync.WaitGroup
	for _, blk := range txs { // 模拟执行，获取读写集？
		for _, tx := range blk {
			wg.Add(1)
			go func(t *types.Transaction) {
				defer wg.Done()
				msg := t.AsMessage()
				tp.MimicConcurrentExecution(state, msg)
			}(tx)
		}
	}
	wg.Wait()

	startConflict := time.Now()
	input := CreateNezhaRWNodes(txs)

	var mapping = make(map[string]*types.Transaction)
	for blk := range txs {
		txSets := txs[blk]
		for _, tx := range txSets {
			id := string(tx.ID)
			mapping[id] = tx
		}
	}

	queueGraph := nezha.CreateGraph(input)
	sequence := queueGraph.QueuesSort()
	queueGraph.DeSS(sequence)
	return time.Since(startConflict)
}

func CreateNezhaRWNodes(txs map[string][]*types.Transaction) [][]*nezha.RWNode {
	var input [][]*nezha.RWNode
	for blk := range txs {
		txSets := txs[blk] // 取一个区块的事务
		for _, tx := range txSets {
			var rAddr, wAddr, rValue, wValue [][]byte
			id := string(tx.ID)
			ts := tx.Header.Timestamp
			payload := tx.Data()
			for addr := range payload {
				rwSet := payload[addr]
				//fmt.Println(addr)
				for _, rw := range rwSet {
					if strings.Compare(rw.Label, "r") == 0 && !isExist(rAddr, []byte(addr)) {
						rAddr = append(rAddr, []byte(addr))
						rValue = append(rValue, []byte("10"))
					} else if strings.Compare(rw.Label, "w") == 0 && !isExist(wAddr, []byte(addr)) {
						wAddr = append(wAddr, []byte(addr))
						wValue = append(wValue, []byte("10"))
					} else if strings.Compare(rw.Label, "iw") == 0 && !isExist(wAddr, []byte(addr)) {
						rAddr = append(rAddr, []byte(addr))
						rValue = append(rValue, []byte("10"))
						wAddr = append(wAddr, []byte(addr))
						wValue = append(wValue, []byte("10"))
					}
				}
			}
			rwNodes := nezha.CreateRWNode(tx.DropHash, id, uint32(ts), rAddr, rValue, wAddr, wValue) // 一个交易，一个读写结点集
			input = append(input, rwNodes)
		}
	}
	return input
}

func isExist(data [][]byte, addr []byte) bool {
	for _, d := range data {
		if strings.Compare(string(addr), string(d)) == 0 {
			return true
		}
	}
	return false
}

func isHashExistInSLice(sliceHash []common.Hash, hash *common.Hash) bool {
	for _, h := range sliceHash {
		if h == *hash {
			return true
		}
	}
	return false
}
