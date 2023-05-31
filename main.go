package main

import (
	"chukonu/concurrency_control"
	"chukonu/concurrency_control/conflict"
	"chukonu/concurrency_control/conflict/classic"
	"chukonu/concurrency_control/optimistic"
	"chukonu/file"
	"chukonu/setting"
	"fmt"
	"github.com/DarcyWep/pureData"
	"math/big"
	"os"
	"time"
)

func main() {
	db, err := setting.OpenLeveldb(setting.NativeDbPath) // get native transaction or merge transaction
	defer db.Close()
	if err != nil {
		fmt.Println("open leveldb error,", err)
		return
	}
	os.Remove(setting.NezhaCsv)
	os.Remove(setting.ClassicCsv)
	os.Remove(setting.OptimisticCsv)
	os.Remove(setting.PresetOptimisticCsv)
	var (
		nezhaCSV            = file.NewWriteCSV(setting.NezhaCsv)
		classicCSV          = file.NewWriteCSV(setting.ClassicCsv)
		optimisticCSV       = file.NewWriteCSV(setting.OptimisticCsv)
		presetOptimisticCSV = file.NewWriteCSV(setting.PresetOptimisticCsv)
	)
	nezhaCSV.Write(&[]string{"block number", "abort ratio", "average contract num factor", "average call num factor", "average executed tim&e factor"})
	classicCSV.Write(&[]string{"block number", "abort ratio", "average contract num factor", "average call num factor", "average executed time factor"})
	optimisticCSV.Write(&[]string{"block number", "abort ratio", "average contract num factor", "average call num factor", "average executed time factor"})
	presetOptimisticCSV.Write(&[]string{"block number", "abort ratio", "average contract num factor", "average call num factor", "average executed time factor"})
	for number := setting.StartNumber; number < setting.StartNumber+setting.SpanNumber; number++ {
		txs, _ := pureData.GetTransactionsByNumber(db, new(big.Int).SetInt64(int64(number)))
		txs = txs[:len(txs)-1]
		if len(txs) == 0 {
			continue
		}

		nezhaAverage := concurrency_control.NewAverage(&txs, conflict.Nezha)
		nezhaAverage.ComputingRelatedData()

		classicAverage := concurrency_control.NewAverage(&txs, classic.Classic)
		classicAverage.ComputingRelatedData()

		optimisticAverage := concurrency_control.NewAverage(&txs, optimistic.Optimistic)
		optimisticAverage.ComputingRelatedData()

		presetOptimisticAverage := concurrency_control.NewAverage(&txs, optimistic.PreSetOrderOptimistic)
		presetOptimisticAverage.ComputingRelatedData()

		nezhaCSV.Write(nezhaAverage.WroteStrings())
		classicCSV.Write(classicAverage.WroteStrings())
		optimisticCSV.Write(optimisticAverage.WroteStrings())
		presetOptimisticCSV.Write(presetOptimisticAverage.WroteStrings())
		fmt.Println("["+time.Now().Format("2006-01-02 15:04:05")+"]", "finish block number", number)
	}
}
