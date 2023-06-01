package setting

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// read data from Native DB
const (
	MinCache     = 2048
	MinHandles   = 2048
	NativeDbPath = "/Users/darcywep/Projects/GoProjects/Morph/pureData/nativedb"

	StartNumber = 12000001
	SpanNumber  = 100
)

func OpenLeveldb(path string) (*leveldb.DB, error) {
	return leveldb.OpenFile(path, &opt.Options{
		OpenFilesCacheCapacity: MinHandles,
		BlockCacheCapacity:     MinCache / 2 * opt.MiB,
		WriteBuffer:            MinCache / 4 * opt.MiB, // Two of these are used internally
		ReadOnly:               true,
	})
}

// csv path, data of experiment will be saving in that
const (
	NezhaCsv            = "../data/nezha.csv"
	ClassicCsv          = "../data/classic.csv"
	OptimisticCsv       = "../data/optimistic.csv"
	PresetOptimisticCsv = "../data/preset_optimistic.csv"

	NezhaDB = "../data/Morph_Test3"
)
