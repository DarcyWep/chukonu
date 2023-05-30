package main

import (
	"chukonu/comparing_concurrency_control/conflict"
	"fmt"
	"github.com/DarcyWep/pureData"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"math/big"
)

const (
	minCache     = 2048
	minHandles   = 2048
	nativeDbPath = "/Users/darcywep/Projects/GoProjects/Morph/pureData/nativedb"

	startNumber = 12000001
)

func openLeveldb(path string) (*leveldb.DB, error) {
	return leveldb.OpenFile(path, &opt.Options{
		OpenFilesCacheCapacity: minHandles,
		BlockCacheCapacity:     minCache / 2 * opt.MiB,
		WriteBuffer:            minCache / 4 * opt.MiB, // Two of these are used internally
		ReadOnly:               true,
	})
}

func main() {
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
