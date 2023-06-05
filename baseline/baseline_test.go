package baseline

import (
	"chukonu/baseline/blockstm"
	"chukonu/baseline/classic"
	"chukonu/baseline/serial"
	"chukonu/file"
	"chukonu/setting"
	"fmt"
	"github.com/DarcyWep/pureData"
	"math/big"
	"os"
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

	os.Remove(setting.TpsCsv)
	var tpsCSV = file.NewWriteCSV(setting.TpsCsv)
	defer tpsCSV.Close()
	tpsCSV.Write(&[]string{"block number", "block stm tps", "classic graph tps", "serial tps", "block stm speed up", "classic graph speed up"})

	for number := setting.StartNumber; number < setting.StartNumber+setting.SpanNumber; number++ {
		txs, _ := pureData.GetTransactionsByNumber(db, new(big.Int).SetInt64(int64(number)))
		txs = txs[:len(txs)-1]
		if len(txs) == 0 {
			continue
		}

		blockStmTps := blockstm.BlockSTM(txs)
		classicTps := classic.Classic(txs)
		serialTps := serial.Serial(txs)
		wStr := []string{(txs)[0].BlockNumber.String(), fmt.Sprintf("%.0f", blockStmTps+0.5),
			fmt.Sprintf("%.0f", classicTps+0.5), fmt.Sprintf("%.0f", serialTps+0.5),
			fmt.Sprintf("%.2f", blockStmTps/serialTps+0.005), fmt.Sprintf("%.2f", classicTps/serialTps+0.005)}
		tpsCSV.Write(&wStr)

		fmt.Printf("["+time.Now().Format("2006-01-02 15:04:05")+"]"+" finish block number %d\n", number)
		//break
	}
}