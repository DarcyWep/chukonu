package conflict

import (
	"chukonu/comparing_concurrency_control/conflict/nezha"
	"chukonu/comparing_concurrency_control/conflict/nezha/core/state"
	"chukonu/comparing_concurrency_control/conflict/nezha/core/tp"
	"chukonu/comparing_concurrency_control/conflict/nezha/core/types"
	"github.com/DarcyWep/pureData/transaction"
	"github.com/ethereum/go-ethereum/common"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

const dbFile = "../data/Morph_Test3"

func Nezha(pureTxs []*transaction.Transaction) []bool {

	runtime.GOMAXPROCS(runtime.NumCPU())

	var ttxs = make(map[string][]*types.Transaction)
	ttxs["0"] = createBatchTxs(pureTxs)

	os.RemoveAll(dbFile)
	statedb3, _ := state.NewState(dbFile, nil)
	statedb3.BatchCreateObjects(ttxs)
	_, abortHashes := runTestNezha(ttxs, statedb3)

	//for i := 0; i < 5; i++ {
	//	d := runTestNezha(ttxs, 1, statedb3)
	//	sum3 += d
	//}
	//fmt.Printf("Average time of Nezha: %.2f\n", float64(sum3)/5)

	var isAbort = make([]bool, len(pureTxs))

	//fmt.Println(len(abortHashes))
	for i, tx := range pureTxs {
		if isHashExistInSLice(abortHashes, tx.Hash) { // abort
			isAbort[i] = true
		}
	}
	return isAbort
}

func runTestNezha(txs map[string][]*types.Transaction, state *state.StateDB) (int64, []common.Hash) {
	start := time.Now()
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
	commitOrder := queueGraph.DeSS(sequence)
	//tps := blkNum*500 - queueGraph.GetAbortedNums()
	//fmt.Printf("Overall txs: %d\n", len(txs["0"]))
	//fmt.Printf("Aborted txs: %d\n", queueGraph.GetAbortedNums())
	_, abortHashes := queueGraph.GetAbortedNums()
	//fmt.Printf("Effective tps: %d\n", tps)

	var keys []int
	for seq := range commitOrder {
		keys = append(keys, int(seq))
	}
	sort.Ints(keys)

	for _, n := range keys {
		for _, group := range commitOrder[int32(n)] {
			if len(group) > 0 {
				wg.Add(1)
				node := group[0]
				tx := mapping[node.TransInfo.ID]
				go func() {
					defer wg.Done()
					msg := tx.AsMessage()
					err := tp.ApplyMessageForNezha(state, msg)
					if err != nil {
						panic(err)
					}
				}()
			}
		}
		wg.Wait()
	}

	state.Commit()
	duration := time.Since(start)
	//fmt.Printf("Time of processing transactions is: %s\n", duration)
	state.Reset()
	return int64(duration), abortHashes
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
