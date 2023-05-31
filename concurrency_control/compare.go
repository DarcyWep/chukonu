package concurrency_control

import (
	"fmt"
	"github.com/DarcyWep/pureData/transaction"
	"time"
)

// average of Contract, CallSum, ExecutionTime in aborted tx or executed tx
type average struct {
	txs     *[]*transaction.Transaction
	isAbort []bool

	abortedContract      int
	abortedCallNum       int
	abortedExecutionTime time.Duration

	executedContract      int
	executedCallNum       int
	executedExecutionTime time.Duration

	// aborted / executed, 丢弃的交易 是 已执行交易的多少倍
	contractFactor      float64
	callNumFactor       float64
	executionTimeFactor float64

	// not average
	abortRatio float64
}

func newAverage(txs *[]*transaction.Transaction, simulatedFunc func([]*transaction.Transaction) []bool) *average {
	return &average{
		txs:                  txs,
		isAbort:              simulatedFunc(*txs),
		abortedContract:      0,
		abortedCallNum:       0,
		abortedExecutionTime: 0,

		executedContract:      0,
		executedCallNum:       0,
		executedExecutionTime: 0,

		// aborted / executed, 丢弃的交易 是 已执行交易的多少倍
		contractFactor:      0,
		callNumFactor:       0,
		executionTimeFactor: 0,

		// not average
		abortRatio: 0,
	}
}

func (a *average) computingRelatedData() {
	var abortedNum, executedNum, txSum = 0, 0, len(a.isAbort)
	for i, tx := range *a.txs {
		if a.isAbort[i] {
			abortedNum += 1
			a.abortedContract += bool2int(tx.Contract)
			a.abortedCallNum += tx.CallSum
			a.abortedExecutionTime += tx.ExecutionTime
		} else {
			executedNum += 1
			a.executedContract += bool2int(tx.Contract)
			a.executedCallNum += tx.CallSum
			a.executedExecutionTime += tx.ExecutionTime
		}
	}
	a.abortRatio = (float64(abortedNum) / float64(txSum) * 100.0) + 0.005
	a.contractFactor = (float64(a.abortedContract) / float64(a.executedContract)) + 0.005
	a.callNumFactor = (float64(a.abortedCallNum) / float64(a.executedCallNum)) + 0.005
	a.executionTimeFactor = (float64(a.abortedExecutionTime) / float64(a.executedExecutionTime)) + 0.005
}

func (a *average) wroteStrings() *[]string {
	return &[]string{fmt.Sprintf("%.2f", a.abortRatio), fmt.Sprintf("%.2f", a.contractFactor),
		fmt.Sprintf("%.2f", a.callNumFactor), fmt.Sprintf("%.2f", a.executionTimeFactor)}
}

func bool2int(b bool) int {
	if b {
		return 1
	}
	return 0
}
