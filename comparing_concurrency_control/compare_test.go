package comparing_concurrency_control

import (
	"chukonu/comparing_concurrency_control/conflict"
	"chukonu/comparing_concurrency_control/conflict/classic"
	"chukonu/comparing_concurrency_control/optimistic"
	"fmt"
	"github.com/DarcyWep/pureData"
	"math/big"
	"testing"
)

func Test(t *testing.T) {
	db, err := openLeveldb(nativeDbPath) // get native transaction or merge transaction
	defer db.Close()
	if err != nil {
		fmt.Println("open leveldb error,", err)
		return
	}

	for number := startNumber; number < startNumber+100; number++ {
		txs, _ := pureData.GetTransactionsByNumber(db, new(big.Int).SetInt64(int64(number)))
		txs = txs[:len(txs)-1]
		if len(txs) == 0 {
			continue
		}
		nezhaAbort := conflict.Nezha(txs)
		nezhaAbortRatio := abortRatio(nezhaAbort)

		classicAbort := classic.Classic(txs)
		classicAbortRatio := abortRatio(classicAbort)

		optimisticAbort := optimistic.Optimistic(txs)
		optimisticAbortRatio := abortRatio(optimisticAbort)

		presetOptimisticAbort := optimistic.PreSetOrderOptimistic(txs)
		presetOptimisticAbortRatio := abortRatio(presetOptimisticAbort)

		fmt.Printf("%d, %.2f%%, %.2f%%, %.2f%%, %.2f%%\n", number,
			nezhaAbortRatio, classicAbortRatio, optimisticAbortRatio, presetOptimisticAbortRatio)
	}

}

func abortRatio(isAbort []bool) float64 {
	var abortNum, sum = 0, len(isAbort)
	for _, abort := range isAbort {
		if abort {
			abortNum += 1
		}
	}
	return (float64(abortNum) / float64(sum) * 100.0) + 0.005
}
