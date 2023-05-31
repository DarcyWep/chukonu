package concurrency_control

import (
	"chukonu/concurrency_control/conflict"
	"chukonu/concurrency_control/conflict/classic"
	"chukonu/concurrency_control/optimistic"
	"chukonu/file"
	"chukonu/setting"
	"fmt"
	"github.com/DarcyWep/pureData"
	"math/big"
	"os"
	"testing"
	"time"
)

func Test(t *testing.T) {
	startTime := time.Now()
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
	nezhaCSV.Write(&[]string{"abort ratio", "average contract num factor", "average call num factor", "average executed tim&e factor"})
	classicCSV.Write(&[]string{"abort ratio", "average contract num factor", "average call num factor", "average executed time factor"})
	optimisticCSV.Write(&[]string{"abort ratio", "average contract num factor", "average call num factor", "average executed time factor"})
	presetOptimisticCSV.Write(&[]string{"abort ratio", "average contract num factor", "average call num factor", "average executed time factor"})
	for number := setting.StartNumber; number < setting.StartNumber+setting.SpanNumber; number++ {
		txs, _ := pureData.GetTransactionsByNumber(db, new(big.Int).SetInt64(int64(number)))
		txs = txs[:len(txs)-1]
		if len(txs) == 0 {
			continue
		}

		nezhaAverage := newAverage(&txs, conflict.Nezha)
		nezhaAverage.computingRelatedData()

		classicAverage := newAverage(&txs, classic.Classic)
		classicAverage.computingRelatedData()

		optimisticAverage := newAverage(&txs, optimistic.Optimistic)
		optimisticAverage.computingRelatedData()

		presetOptimisticAverage := newAverage(&txs, optimistic.PreSetOrderOptimistic)
		presetOptimisticAverage.computingRelatedData()

		nezhaCSV.Write(nezhaAverage.wroteStrings())
		classicCSV.Write(classicAverage.wroteStrings())
		optimisticCSV.Write(optimisticAverage.wroteStrings())
		presetOptimisticCSV.Write(presetOptimisticAverage.wroteStrings())
	}
	fmt.Println(time.Since(startTime))
}
