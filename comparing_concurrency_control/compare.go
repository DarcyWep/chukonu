package comparing_concurrency_control

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
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

//func Optimistic(number *big.Int) {
//	txs := getAllTxsByNumber(number)
//	optimistic.Simulated(txs[:len(txs)-1])
//}
//
//func Classic(number *big.Int) {
//	txs := getAllTxsByNumber(number)
//	classic.Classic(txs[:len(txs)-1])
//}
