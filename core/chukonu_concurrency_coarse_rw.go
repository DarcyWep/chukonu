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

type (
	distributeCoarseRWChan chan *accountTokenRW
	checkCoarseRWChan      chan *accountTokenRW
	executionCoarseRWChan  chan *[]*accountTokenRW

	distributeCoarseRWMap map[common.Address]coarseRWTxs
)

type coarseRWTxs []*coarseRWTx
type coarseRWTx struct {
	tx   *types.Transaction
	txdb *state.StmTransaction
}

func newCoarseRWTx(tx *types.Transaction, txdb *state.StmTransaction) *coarseRWTx {
	return &coarseRWTx{tx: tx, txdb: txdb}
}

type accountTokenRW struct {
	address common.Address // 授予交易的哪个账户的访问权限
	tx      *coarseRWTx    // 授予账户的访问权限给哪个交易 (如果tx == nil, 则为第一笔交易)
	//txIndexInAccessSequence int                // 用于标识结束 (即接收到最后一个交易执行的返回后结束)
	readOnly bool // 授予交易的访问权限标识 (读或写)
}

func newAccountTokenRW(addr common.Address, tx *coarseRWTx, readOnly bool) *accountTokenRW {
	return &accountTokenRW{address: addr, tx: tx, readOnly: readOnly}
}

type accountAccessSequenceRWMap map[common.Address]*accountAccessSequenceRW
type accountAccessSequenceRW struct {
	mutex             sync.Mutex
	address           common.Address
	pendingTxs        *coarseRWTxs // 该地址等待执行的队列
	executingTxsNum   int          // 正在执行的交易个数
	readAccessTxsNum  int          // 获取了读权限交易的个数
	writeAccessTxsNum int          // 获取了写权限交易的个数
	pendingIndex      int          // 最新的等待执行的交易在pendingTxs中的下标
	finishTxsNum      int          // 已完成执行的事务
	len               int          // pending的总长度
}

func newAccountAccessSequenceRW(addr common.Address, txs *coarseRWTxs, len int) *accountAccessSequenceRW {
	return &accountAccessSequenceRW{
		address:           addr,
		pendingTxs:        txs,
		executingTxsNum:   0, // 接收到事务返回的令牌时，表示已经有一个事务执行完成
		readAccessTxsNum:  0,
		writeAccessTxsNum: 0,
		pendingIndex:      0,
		len:               len,
	}
}

func ChuKoNuConcurrencyCoarseRW(block *types.Block, stmStateDB *state.StmStateDB, cfg vm.Config, config *params.ChainConfig, chainDb ethdb.Database) {
	var (
		disNum  = 3
		execNum = 4
		//num      = runtime.NumCPU()
		checkNum = 3
		disWg    sync.WaitGroup
		disMutex sync.RWMutex
		disCh    = make(distributeCoarseRWChan, chanSize)

		checkWg sync.WaitGroup
		checkCh = make([]checkCoarseRWChan, checkNum)

		execWg sync.WaitGroup
		execCh = make(executionCoarseRWChan, chanSize)

		closeWg sync.WaitGroup
		closeCh = make(closeChan, chanSize/2)

		feeCh = make(feeChan, chanSize)

		accountAccessSequences = make(accountAccessSequenceRWMap)
	)
	startTime := time.Now()
	disWg.Add(disNum)
	for i := 0; i < disNum; i++ {
		go distributeTxsCoarseRW(&accountAccessSequences, disCh, &checkCh, closeCh, &disWg, &disMutex, checkNum)
	}

	checkWg.Add(checkNum) //并发: Cpu核数=并发线程数
	for i := 0; i < checkNum; i++ {
		checkCh[i] = make(checkCoarseRWChan, chanSize/checkNum+1)
		go checkTxsCoarseRW(checkCh[i], execCh, &checkWg)
	}

	execWg.Add(execNum)
	for i := 0; i < execNum; i++ {
		go executionTxsCoarseRW(execCh, disCh, feeCh, block, stmStateDB, cfg, config, chainDb, &execWg)
	}

	queueLen := constructionOrderCoarseRW(block, &accountAccessSequences, disCh, stmStateDB) // 依赖队列的长度

	closeWg.Add(1)
	go closeChuKoNuCoarseRW(closeCh, disCh, &checkCh, execCh, queueLen, &closeWg, checkNum)

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

	fmt.Println(root, "ChuKoNu Coarse RW", float64(block.Transactions().Len())/time.Since(startTime).Seconds())
}

func constructionOrderCoarseRW(block *types.Block, accountAccessSequences *accountAccessSequenceRWMap, disCh distributeCoarseRWChan, stmStateDB *state.StmStateDB) int {
	seQueue := make(distributeCoarseRWMap)

	coarseTxs := make(coarseRWTxs, 0)
	for i, tx := range block.Transactions() {
		tx.Index = i
		coarseTxs = append(coarseTxs, newCoarseRWTx(tx, state.NewStmTransaction(tx, tx.Index, stmStateDB)))
	}

	// 	地址对应的队列
	for _, tx := range coarseTxs {
		tx.tx.AccessSum = len(*tx.tx.AccessPre)
		for addr, _ := range *tx.tx.AccessPre {
			if _, ok := seQueue[addr]; !ok {
				seQueue[addr] = make(coarseRWTxs, 0)
			}
			seQueue[addr] = append(seQueue[addr], tx)
			//fmt.Println(seQueue)
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
		(*accountAccessSequences)[val.addr] = newAccountAccessSequenceRW(val.addr, &list, len(list))
	}
	for _, val := range listTxsByAddr {
		disCh <- newAccountTokenRW(val.addr, nil, true)
	}
	return len(listTxsByAddr)
}

func distributeTxsCoarseRW(accountAccessSequences *accountAccessSequenceRWMap, disCh distributeCoarseRWChan, checkCh *[]checkCoarseRWChan, closeCh closeChan, wg *sync.WaitGroup, disMutex *sync.RWMutex, num int) {
	defer wg.Done()
	for token := range disCh {
		// 获取账户相应的访问队列
		disMutex.RLock()
		accessSequence := (*accountAccessSequences)[token.address]
		disMutex.RUnlock()

		// 处理访问队列
		accessSequence.mutex.Lock()

		if token.tx != nil { // 有一个事务完成执行
			accessSequence.finishTxsNum += 1
			accessSequence.executingTxsNum -= 1
			if token.readOnly {
				accessSequence.readAccessTxsNum -= 1
			} else {
				accessSequence.writeAccessTxsNum -= 1
			}
		}

		// 查看是否已经执行完所有的事务
		if accessSequence.finishTxsNum == accessSequence.len { // 所有的事务都完成了执行
			accessSequence.mutex.Unlock()
			closeCh <- true
			continue // 结束当前账户的所有交易
		}

		// 查看是否还有待处理的事务
		if accessSequence.pendingIndex == accessSequence.len {
			accessSequence.mutex.Unlock()
			continue // 该地址的所有事务都已完成
		}

		executingTxsNum := accessSequence.executingTxsNum // 正在执行的事务数量

		tx := (*accessSequence.pendingTxs)[accessSequence.pendingIndex] // accessSequence.pendingIndex 当前可执行的交易下标，递增
		txAccessPre := (*tx.tx.AccessPre)[token.address]

		// 没有正在执行的事务 或 执行的都是读事务 (即不会产生冲突) --> 只有事务执行完成或者前一个事务为读才会进入此程序
		if executingTxsNum == 0 || accessSequence.writeAccessTxsNum == 0 {
			tx.txdb.GetAccountState(token.address, txAccessPre)
			if txAccessPre.CoarseWrite {
				(*checkCh)[tx.tx.Index%num] <- newAccountTokenRW(token.address, tx, false) // 发送给对应的检查线程，根据tx.index进行选择
				accessSequence.writeAccessTxsNum += 1
			} else {
				(*checkCh)[tx.tx.Index%num] <- newAccountTokenRW(token.address, tx, true) // 发送给对应的检查线程，根据tx.index进行选择
				accessSequence.readAccessTxsNum += 1
			}

			accessSequence.pendingIndex += 1    // 需要继续往下处理事务
			accessSequence.executingTxsNum += 1 // 正在执行的事务+1

			// 当前授予令牌的事务为只读事务，该地址可以继续进行令牌的发放
			if !txAccessPre.CoarseWrite {
				//fmt.Println(tx.tx.Hash(), token.address)
				disCh <- newAccountTokenRW(token.address, nil, false)
			}
		}

		//// 没有正在执行的事务 或 待执行事务没有进行写 (即不会产生冲突) --> 只有事务执行完成或者前一个事务为读才会进入此程序
		//if executingTxsNum == 0 || (executingTxsNum != 0 && !txAccessPre.CoarseWrite) {
		//	tx.txdb.GetAccountState(token.address, txAccessPre)
		//	(*checkCh)[tx.tx.Index%num] <- newAccountTokenRW(token.address, tx) // 发送给对应的检查线程，根据tx.index进行选择
		//	accessSequence.pendingIndex += 1                                    // 需要继续往下处理事务
		//	accessSequence.executingTxsNum += 1                                 // 正在执行的事务+1
		//
		//	// 当前授予令牌的事务为只读事务，该地址可以继续进行令牌的发放
		//	if !txAccessPre.CoarseWrite {
		//		//fmt.Println(tx.tx.Hash(), token.address)
		//		disCh <- newAccountTokenRW(token.address, nil)
		//	}
		//}
		accessSequence.mutex.Unlock()
	}
}

func checkTxsCoarseRW(checkCh checkCoarseRWChan, execCh executionCoarseRWChan, wg *sync.WaitGroup) {
	wg.Done()
	queueByAddr := make(map[common.Hash][]*accountTokenRW)
	for token := range checkCh {
		var tokens []*accountTokenRW
		if _, ok := queueByAddr[token.tx.tx.Hash()]; !ok { // 第一次接收到该交易的依赖
			tokens = make([]*accountTokenRW, 0)
		} else {
			tokens = queueByAddr[token.tx.tx.Hash()]
		}
		tokens = append(tokens, token)
		if len(tokens) == token.tx.tx.AccessSum { // 可以执行
			execCh <- &tokens
			delete(queueByAddr, token.tx.tx.Hash())
		} else { // 还不能执行
			queueByAddr[token.tx.tx.Hash()] = tokens
		}
	}
}

func executionTxsCoarseRW(execCh executionCoarseRWChan, disCh distributeCoarseRWChan, feeCh feeChan, block *types.Block, stmStateDB *state.StmStateDB, cfg vm.Config, config *params.ChainConfig, chainDb ethdb.Database, wg *sync.WaitGroup) {
	wg.Done()
	for tokens := range execCh {
		tx := (*tokens)[0].tx
		var (
			usedGas     = new(uint64)
			header      = block.Header()
			blockHash   = block.Hash()
			blockNumber = block.Number()
			gp          = new(GasPool).AddGas(block.GasLimit())
		)
		//fmt.Println(tx.Index, tx.Hash())
		blockContext := NewEVMBlockContext(header, chainDb, nil)

		//stmTxDB := state.NewStmTransaction(tx, tx.Index, stmStateDB)
		//stmTxDB.GetAllState(tx)

		vmenv := vm.NewEVM(blockContext, vm.TxContext{}, tx.txdb, config, cfg)
		msg, err := TransactionToMessage(tx.tx, types.MakeSigner(config, header.Number), header.BaseFee)
		//tx.txdb.ReallyGetState(tx.tx, msg.From)
		//// 避免Nonce错误
		//tx.txdb.SetNonce(msg.From, msg.Nonce)
		//fmt.Println(tx)

		if err != nil {
			fmt.Printf("could not apply tx %d [%v]: %w\n", tx.tx.Index, tx.tx.Hash().Hex(), err)
		}
		err, fee := applyChuKoNuTransaction(msg, config, gp, stmStateDB, tx.txdb, blockNumber, blockHash, tx.tx, usedGas, vmenv)
		feeCh <- new(big.Int).Set(fee)
		if err != nil {
			fmt.Printf("could not apply tx %d [%v]: %w", tx.tx.Index, tx.tx.Hash().Hex(), err)
		}
		tx.txdb.Validation(true)

		// 执行完成，处理剩余的待处理队列
		for _, token := range *tokens {
			disCh <- token
		}
	}
}

func closeChuKoNuCoarseRW(closeCh closeChan, disCh distributeCoarseRWChan, checkCh *[]checkCoarseRWChan, execCh executionCoarseRWChan, queueLen int, wg *sync.WaitGroup, num int) {
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
