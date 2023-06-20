package baseline

import (
	"chukonu/baseline/blockstm"
	"chukonu/baseline/chukonu"
	"chukonu/baseline/classic"
	"chukonu/baseline/peep"
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
	tpsCSV.Write(&[]string{"block number", "block stm tps", "classic graph tps", "chukonu tps", "peep tps", "serial tps", "block stm speed up", "peep speed up", "classic graph speed up", "chukonu speed up"})

	for number := setting.StartNumber; number < setting.StartNumber+setting.SpanNumber; number++ {
		txs, _ := pureData.GetTransactionsByNumber(db, new(big.Int).SetInt64(int64(number)))
		txs = txs[:len(txs)-1]
		if len(txs) == 0 {
			continue
		}

		blockStmTps := blockstm.BlockSTM(txs)
		classicTps := classic.Classic(txs)
		chuKoNuTps := chukonu.ChuKoNuConcurrency(txs)
		peepTps := peep.Peep(txs)
		serialTps := serial.Serial(txs)
		wStr := []string{(txs)[0].BlockNumber.String(), fmt.Sprintf("%.0f", blockStmTps+0.5),
			fmt.Sprintf("%.0f", classicTps+0.5), fmt.Sprintf("%.0f", chuKoNuTps+0.5),
			fmt.Sprintf("%.0f", peepTps+0.5), fmt.Sprintf("%.0f", serialTps+0.5),
			fmt.Sprintf("%.2f", blockStmTps/serialTps+0.005),
			fmt.Sprintf("%.2f", peepTps/serialTps+0.005),
			fmt.Sprintf("%.2f", classicTps/serialTps+0.005),
			fmt.Sprintf("%.2f", chuKoNuTps/serialTps+0.005)}
		tpsCSV.Write(&wStr)

		//fmt.Printf("["+time.Now().Format("2006-01-02 15:04:05")+"]"+" finish block number %d, %s\n", number, wStr)
		fmt.Printf("["+time.Now().Format("2006-01-02 15:04:05")+"]"+" finish block number %d\n", number)
		//break
	}
}

func TestChuKoNuTransferOpt(t *testing.T) {
	db, err := setting.OpenLeveldb(setting.NewNativeDbPath) // get native transaction or merge transaction
	defer db.Close()
	if err != nil {
		fmt.Println("open leveldb error,", err)
		return
	}

	os.Remove(setting.ChuKoNuTpsCsv)
	var tpsCSV = file.NewWriteCSV(setting.ChuKoNuTpsCsv)
	defer tpsCSV.Close()
	tpsCSV.Write(&[]string{"block number", "transfer opt tps", "chukonu concurrency tps", "chukonu tps", "serial tps", "transfer opt speed up", "chukonu concurrency speed up", "chukonu speed up"})

	for number := setting.StartNumber; number < setting.StartNumber+setting.SpanNumber; number++ {
		txs, _ := pureData.GetTransactionsByNumber(db, new(big.Int).SetInt64(int64(number)))
		txs = txs[:len(txs)-1]
		if len(txs) == 0 {
			continue
		}

		chuKoNuTransferOpt := chukonu.ChuKoNuTransferOpt(txs)
		chuKoNuConcurrencyTps := chukonu.ChuKoNuConcurrency(txs)
		chuKoNuTps := chukonu.ChuKoNu(txs)
		serialTps := serial.Serial(txs)

		wStr := []string{(txs)[0].BlockNumber.String(), fmt.Sprintf("%.0f", chuKoNuTransferOpt+0.5),
			fmt.Sprintf("%.0f", chuKoNuConcurrencyTps+0.5), fmt.Sprintf("%.0f", chuKoNuTps+0.5),
			fmt.Sprintf("%.0f", serialTps+0.5),
			fmt.Sprintf("%.2f", chuKoNuTransferOpt/serialTps+0.005),
			fmt.Sprintf("%.2f", chuKoNuConcurrencyTps/serialTps+0.005),
			fmt.Sprintf("%.2f", chuKoNuTps/serialTps+0.005)}
		tpsCSV.Write(&wStr)

		//fmt.Printf("["+time.Now().Format("2006-01-02 15:04:05")+"]"+" finish block number %d, %s\n", number, wStr)
		fmt.Printf("["+time.Now().Format("2006-01-02 15:04:05")+"]"+" finish block number %d\n", number)
	}
}
