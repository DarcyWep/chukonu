package chukonu

import (
	"github.com/DarcyWep/pureData/transaction"
	"time"
)

func ChuKoNuTransferOpt(txs []*transaction.Transaction) float64 {
	ctxs := make(chukonuTxs, 0)
	for _, tx := range txs {
		ctx := newChuKoNuTx(tx)
		ctx.generateReadAndWrite(tx)
		ctxs = append(ctxs, ctx)
	}
	startTime := time.Now()
	for _, ctx := range ctxs {
		if ctx.isOpt {
			time.Sleep(ctx.optExecutedTime)
		} else {
			time.Sleep(ctx.tx.ExecutionTime + 10*time.Microsecond)
		}
	}
	return float64(len(txs)) / time.Since(startTime).Seconds()
}
