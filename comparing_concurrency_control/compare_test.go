package comparing_concurrency_control

import (
	"chukonu/comparing_concurrency_control/conflict"
	"fmt"
	"github.com/DarcyWep/pureData"
	"math/big"
	"testing"
)

func TestOptimistic(t *testing.T) {
	//number := new(big.Int).SetInt64(12000001)
	//Optimistic(number)
}

func TestNezha(t *testing.T) {
	//conflict.Nezha(1000)
}

func TestClassic(t *testing.T) {
	//graph := [][]int{
	//	  0  1  2  3  4  5  6
	//	0{0, 1, 0, 0, 0, 0, 0},
	//	1{0, 0, 1, 0, 1, 0, 1},
	//	2{0, 0, 0, 1, 0, 0, 0},
	//	3{0, 0, 1, 0, 1, 1, 0},
	//	4{0, 0, 0, 0, 0, 1, 0},
	//	5{0, 0, 0, 0, 1, 0, 0},
	//	6{1, 0, 1, 0, 0, 0, 0},
	//}

	//graph := [][]int{
	//	{0, 1, 0, 0, 0, 0, 0},
	//	{0, 0, 1, 0, 1, 0, 1},
	//	{0, 0, 0, 1, 0, 0, 0},
	//	{0, 0, 1, 0, 1, 1, 0},
	//	{0, 0, 0, 0, 0, 1, 0},
	//	{0, 0, 0, 0, 1, 0, 0},
	//	{1, 0, 1, 0, 0, 0, 0},
	//}
	//graph := [][]int{
	//	{1},
	//	{2, 4, 6},
	//	{3, 7},
	//	{2, 4, 5},
	//	{5},
	//	{4},
	//	{6},
	//	{0, 2},
	//}
	//tarjan := nezha.NewTarjanSCC(&graph)
	//tarjan.SCC()
	//for _, scc := range tarjan.GetSCCs() {
	//	fmt.Println("scc.Member:", scc.Member)
	//	for _, node := range scc.Vertices {
	//		fmt.Printf("%d\t", node)
	//	}
	//	fmt.Println()
	//}
	//Classic(new(big.Int).SetInt64(12000001))
}

func Test(t *testing.T) {
	db, err := openLeveldb(nativeDbPath) // get native transaction or merge transaction
	defer db.Close()
	if err != nil {
		fmt.Println("open leveldb error,", err)
		return
	}
	var abortNum, sum = 0, 0
	for number := startNumber; number < startNumber+1000; number++ {
		txs, _ := pureData.GetTransactionsByNumber(db, new(big.Int).SetInt64(int64(number)))
		txs = txs[:len(txs)-1]
		if len(txs) == 0 {
			continue
		}
		abortNum, sum = 0, len(txs)
		isAbort := conflict.Nezha(txs)
		for _, abort := range isAbort {
			if abort {
				abortNum += 1
			}
		}
		fmt.Printf("%d, Aborted ratio: %.2f%%\n", number, (float64(abortNum)/float64(sum)*100.0)+0.005)
	}

}
