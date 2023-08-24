package core

import (
	"chukonu/core/state"
	"chukonu/core/types"
	"chukonu/core/vm"
	"chukonu/ethdb"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/params"
	"math/big"
	"sort"
	"sync"
	"time"
)

const chanSize = 1024

type distributeMap map[common.Address]types.Transactions
type distributeChan chan *depQueue
type checkChan chan *depQueue
type executionChan chan *[]*depQueue
type closeChan chan bool
type feeChan chan *big.Int

type depQueue struct {
	depAddr    common.Address      //所依赖的地址
	pendingTxs *types.Transactions // 该地址等待执行的队列
	AccessSlot types.AccessSlotMap // 该地址下正在访问的Slot
	canExec    bool                // 队首依赖是否可以执行
	index      int                 // 当前执行的交易在pending中的下标
	len        int                 // pending的总长度
}

func ChuKoNuConcurrencyAccess(block *types.Block, stmStateDB *state.StmStateDB, cfg vm.Config, config *params.ChainConfig, chainDb ethdb.Database) {
	var (
		disNum  = 2
		execNum = 6
		//num      = runtime.NumCPU()
		checkNum = 2
		disWg    sync.WaitGroup
		disCh    = make(distributeChan, chanSize)

		checkWg sync.WaitGroup
		//checkCh  = make(checkChan, chanSize)
		checkCh = make([]checkChan, checkNum)

		execWg sync.WaitGroup
		execCh = make(executionChan, chanSize)

		closeWg sync.WaitGroup
		closeCh = make(closeChan, chanSize/2)

		feeCh = make(feeChan, chanSize)
	)
	startTime := time.Now()
	disWg.Add(disNum)
	for i := 0; i < disNum; i++ {
		go distributeTxs(disCh, &checkCh, &disWg, checkNum)
	}

	checkWg.Add(checkNum) //并发: Cpu核数=并发线程数
	for i := 0; i < checkNum; i++ {
		checkCh[i] = make(checkChan, chanSize/checkNum+1)
		go checkTxs(checkCh[i], execCh, &checkWg)
	}

	execWg.Add(execNum)
	for i := 0; i < execNum; i++ {
		go executionTxs(execCh, disCh, closeCh, feeCh, block, stmStateDB, cfg, config, chainDb, &execWg)
	}

	queueLen := constructionOrder(block, disCh) // 依赖队列的长度

	closeWg.Add(1)
	go closeChuKoNu(closeCh, disCh, &checkCh, execCh, queueLen, &closeWg, checkNum)

	closeWg.Wait()
	disWg.Wait()
	checkWg.Wait()
	execWg.Wait()
	close(feeCh)
	allFee := new(big.Int).SetInt64(0)
	for fee := range feeCh {
		allFee.Add(allFee, fee)
	}
	stmStateDB.AddBalance(block.Coinbase(), allFee)

	// Fail if Shanghai not enabled and len(withdrawals) is non-zero.
	withdrawals := block.Withdrawals()
	if len(withdrawals) > 0 && !config.IsShanghai(block.Time()) {
		fmt.Println("withdrawals before shanghai")
	}
	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	stmAccumulateRewards(config, stmStateDB, block.Header(), block.Uncles())

	root := stmStateDB.IntermediateRoot(config.IsEIP158(block.Number()), -1)

	fmt.Println(root, "ChuKoNu Access", float64(block.Transactions().Len())/time.Since(startTime).Seconds())
}

func constructionOrder(block *types.Block, disCh distributeChan) int {
	seQueue := make(distributeMap)

	// 	地址对应的队列
	for i, tx := range block.Transactions() {
		tx.Index = i
		tx.AccessSum = len(*tx.AccessPre)
		for addr, _ := range *tx.AccessPre {
			if _, ok := seQueue[addr]; !ok {
				seQueue[addr] = make(types.Transactions, 0)
			}
			seQueue[addr] = append(seQueue[addr], tx)
		}
	}

	type txsByAddr struct {
		addr common.Address
		num  int
	}

	var listTxsByAddr []txsByAddr
	for key, vch := range seQueue {
		listTxsByAddr = append(listTxsByAddr, txsByAddr{key, len(vch)})
	}

	sort.Slice(listTxsByAddr, func(i, j int) bool {
		return listTxsByAddr[i].num > listTxsByAddr[j].num // 降序
	})

	for _, val := range listTxsByAddr {
		list := seQueue[val.addr]
		disCh <- &depQueue{depAddr: val.addr, pendingTxs: &list, canExec: false, index: 0, len: len(list)}
	}
	return len(listTxsByAddr)

	//for addr, _ := range seQueue {
	//	list := seQueue[addr]
	//	disCh <- &depQueue{depAddr: addr, pendingTxs: &list, canExec: false, index: 0, len: len(list)}
	//}
	//return len(seQueue)
}

func distributeTxs(disCh distributeChan, checkCh *[]checkChan, wg *sync.WaitGroup, num int) {
	defer wg.Done()
	for queue := range disCh {
		tx := (*queue.pendingTxs)[queue.index] // queue.index 当前可执行的交易下标，递增

		queue.canExec = true              // 合约地址依赖都为可执行
		(*checkCh)[tx.Index%num] <- queue // 发送给对应的检查线程，根据tx.index进行选择
	}
}

func checkTxs(checkCh checkChan, execCh executionChan, wg *sync.WaitGroup) {
	wg.Done()
	queueByAddr := make(map[common.Hash][]*depQueue)
	for queue := range checkCh {
		tx := (*queue.pendingTxs)[queue.index]
		var depQ []*depQueue
		if _, ok := queueByAddr[tx.Hash()]; !ok { // 第一次接收到该交易的依赖
			depQ = make([]*depQueue, 0)
		} else {
			depQ = queueByAddr[tx.Hash()]
		}
		depQ = append(depQ, queue)
		if len(depQ) == tx.AccessSum { // 可以执行
			execCh <- &depQ
			delete(queueByAddr, tx.Hash())
		} else { // 还不能执行
			queueByAddr[tx.Hash()] = depQ
		}
	}
}

func executionTxs(execCh executionChan, disCh distributeChan, closeCh closeChan, feeCh feeChan, block *types.Block, stmStateDB *state.StmStateDB, cfg vm.Config, config *params.ChainConfig, chainDb ethdb.Database, wg *sync.WaitGroup) {
	wg.Done()
	for depQ := range execCh {
		q0 := (*depQ)[0]
		tx := (*q0.pendingTxs)[q0.index]

		var (
			usedGas     = new(uint64)
			header      = block.Header()
			blockHash   = block.Hash()
			blockNumber = block.Number()
			gp          = new(GasPool).AddGas(block.GasLimit())
		)
		//fmt.Println(tx.Index, tx.Hash())
		blockContext := NewEVMBlockContext(header, chainDb, nil)

		stmTxDB := state.NewStmTransaction(tx, tx.Index, stmStateDB)
		stmTxDB.GetAllState(tx)
		vmenv := vm.NewEVM(blockContext, vm.TxContext{}, stmTxDB, config, cfg)
		msg, err := TransactionToMessage(tx, types.MakeSigner(config, header.Number), header.BaseFee)
		if err != nil {
			fmt.Printf("could not apply tx %d [%v]: %w\n", tx.Index, tx.Hash().Hex(), err)
		}
		err, fee := applyChuKoNuTransaction(msg, config, gp, stmStateDB, stmTxDB, blockNumber, blockHash, tx, usedGas, vmenv)
		feeCh <- new(big.Int).Set(fee)
		if err != nil {
			fmt.Printf("could not apply tx %d [%v]: %w", tx.Index, tx.Hash().Hex(), err)
		}
		stmTxDB.Validation(true)

		// 执行完成，处理剩余的待处理队列
		for _, q := range *depQ {
			q.index += 1
			if q.index < q.len { // 还有事务需要执行
				//fmt.Println("q.index < q.len:", q.index, q.len)
				disCh <- q
			} else {
				//fmt.Println("q.index >= q.len:", q.index, q.len)
				closeCh <- true
			}
		}
	}
}

func closeChuKoNu(closeCh closeChan, disCh distributeChan, checkCh *[]checkChan, execCh executionChan, queueLen int, wg *sync.WaitGroup, num int) {
	defer wg.Done()
	var finishNum = 0
	for _ = range closeCh {
		finishNum += 1
		if finishNum == queueLen {
			close(closeCh)
			close(disCh)
			for i := 0; i < num; i++ {
				close((*checkCh)[i])
			}
			close(execCh)
		}
	}
}

func applyChuKoNuTransaction(msg *Message, config *params.ChainConfig, gp *GasPool, stmStateDB *state.StmStateDB, statedb *state.StmTransaction, blockNumber *big.Int, blockHash common.Hash, tx *types.Transaction, usedGas *uint64, evm *vm.EVM) (error, *big.Int) {
	// Create a new context to be used in the EVM environment.
	txContext := NewEVMTxContext(msg)
	evm.Reset(txContext, statedb)

	// Apply the transaction to the current state (included in the env).
	result, err, fee := ApplyMessage(evm, msg, gp)
	if err != nil {
		return err, fee
	}
	*usedGas += result.UsedGas
	return err, fee
}
