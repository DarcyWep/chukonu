package baseline

import (
	"chukonu/baseline/blockstm"
	"chukonu/baseline/classic"
	"chukonu/baseline/serial"
	"chukonu/setting"
	"fmt"
	"github.com/DarcyWep/pureData"
	"math/big"
	"testing"
	"time"
)

// OptimisticChanSize optimistic channel size
const OptimisticChanSize = 1024

func Test(t *testing.T) {
	db, err := setting.OpenLeveldb(setting.NativeDbPath) // get native transaction or merge transaction
	defer db.Close()
	if err != nil {
		fmt.Println("open leveldb error,", err)
		return
	}

	for number := setting.StartNumber; number < setting.StartNumber+setting.SpanNumber; number++ {
		txs, _ := pureData.GetTransactionsByNumber(db, new(big.Int).SetInt64(int64(number)))
		txs = txs[:len(txs)-1]
		if len(txs) == 0 {
			continue
		}

		blockStmTps := blockstm.BlockSTM(txs)
		serialTps := serial.Serial(txs)
		classicTps := classic.Classic(txs)
		fmt.Printf("["+time.Now().Format("2006-01-02 15:04:05")+"]"+" finish block number %d, %.0f %.0f %.0f %.2fx %.2fx\n", number, blockStmTps, classicTps, serialTps, blockStmTps/serialTps, classicTps/serialTps)
		//break
	}
}
