package concurrency_control

import (
	"chukonu/concurrency_control/conflict/nezha/core/state"
	"fmt"
	"github.com/DarcyWep/pureData/transaction"
	"time"
)

// Average of Contract, CallSum, ExecutionTime in aborted tx or executed tx
type Average struct {
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

	// not Average
	abortRatio float64
}

func NewAverage(txs *[]*transaction.Transaction, statedb *state.StateDB, simulatedFunc func([]*transaction.Transaction, *state.StateDB) []bool) *Average {
	return &Average{
		txs:                  txs,
		isAbort:              simulatedFunc(*txs, statedb),
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

		// not Average
		abortRatio: 0,
	}
}

func (a *Average) ComputingRelatedData() {
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
	abortedAvgContract := float64(a.abortedContract) / float64(abortedNum)
	abortedAvgCallNum := float64(a.abortedCallNum) / float64(abortedNum)
	abortedAvgExecutionTime := float64(a.abortedExecutionTime) / float64(abortedNum)

	executedAvgContract := float64(a.executedContract) / float64(executedNum)
	executedAvgCallNum := float64(a.executedCallNum) / float64(executedNum)
	executedAvgExecutionTime := float64(a.executedExecutionTime) / float64(executedNum)

	a.abortRatio = (float64(abortedNum) / float64(txSum) * 100.0) + 0.005
	a.contractFactor = (abortedAvgContract / executedAvgContract) + 0.005
	a.callNumFactor = (abortedAvgCallNum / executedAvgCallNum) + 0.005
	a.executionTimeFactor = (abortedAvgExecutionTime / executedAvgExecutionTime) + 0.005
}

func (a *Average) WroteStrings() *[]string {
	return &[]string{(*a.txs)[0].BlockNumber.String(), fmt.Sprintf("%.2f", a.abortRatio), fmt.Sprintf("%.2f", a.contractFactor),
		fmt.Sprintf("%.2f", a.callNumFactor), fmt.Sprintf("%.2f", a.executionTimeFactor)}
}

func bool2int(b bool) int {
	if b {
		return 1
	}
	return 0
}
