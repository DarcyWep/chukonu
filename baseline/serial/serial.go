package serial

import (
	"github.com/DarcyWep/pureData/transaction"
	"time"
)

func Serial(txs []*transaction.Transaction) float64 {
	startTime := time.Now()
	for _, tx := range txs {
		time.Sleep(tx.ExecutionTime + 10*time.Microsecond)
	}
	return float64(len(txs)) / time.Since(startTime).Seconds()
}
