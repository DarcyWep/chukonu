package setting

import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

// read data from Native DB
const (
	MinCache        = 2048
	MinHandles      = 2048
	NativeDbPath    = "/Users/darcywep/Projects/GoProjects/Morph/pureData/nativedb"
	NewNativeDbPath = "../data/newnativedb"

	StartNumber = 12000001
	//StartNumber = 12048500
	SpanNumber = 50000
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

	TpsCsv        = "../data/tps.csv"
	ChuKoNuTpsCsv = "../data/chukonu_tps.csv"

	TxCategoriesCsv = "../data/tx_categories.csv"

	NezhaDB = "../data/Morph_Test3"
)

// OptimisticChanSize optimistic channel size
const OptimisticChanSize = 1024

// etherscan setting
const (
	ApiKey                      = "TUIJUUD9JQ4VEDS97DHTQ3V13V2R8KFEEF" // wep
	ContractLeveldb             = "../data/contract"
	AllContractAddressKey       = "all_contract_addresses"
	FinishContractAddressKey    = "finish_contract_addresses"
	NotFinishContractAddressKey = "not_finish_contract_addresses"
	SortFunctionsKey            = "sort_functions"
	SortContractsKey            = "sort_contracts"
)

// Contract

const (
	TransferKey                 = "a9059cbb"
	ApproveKey                  = "095ea7b3"
	SwapExactETHForTokensKey    = "7ff36ab5"
	SwapExactTokensForTokensKey = "38ed1739"
	SwapExactTokensForETHKey    = "18cbafe5"
)
