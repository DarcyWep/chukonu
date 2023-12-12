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
	"runtime"
	"sort"
	"sync"
	"time"
)

type (
	distributeFineRWChan chan *accountTokenFine
	checkFineRWChan      chan *accountTokenFine
	executionFineRWChan  chan *[]*accountTokenFine

	initAccountAccessSequenceChan chan *accountAccessSequenceFine

	distributeFineRWMap map[common.Address]fineRWTxs
	slotTxSequenceMap   map[common.Hash]fineRWTxs
)

type fineRWTxs []*fineRWTx
type fineRWTx struct {
	tx   *types.Transaction
	txdb *state.StmTransaction
	//txIndexInAccessSequence *map[common.Address]int // 用于标识在账户访问队列的位置，用于辅助
}

func newFineRWTx(tx *types.Transaction, txdb *state.StmTransaction) *fineRWTx {
	return &fineRWTx{tx: tx, txdb: txdb}
}

type accountTokenFine struct {
	address common.Address // 授予交易的哪个账户的访问权限
	tx      *fineRWTx      // 授予账户的访问权限给哪个交易 (如果tx == nil, 则为第一笔交易)
	//txIndexInAccessSequence int            // 用于标识结束 (即接收到最后一个交易执行的返回后结束)
	readOnly bool // 授予交易的访问权限标识 (读或写), 只标识账户状态而不标识slot
}

func newAccountTokenFine(addr common.Address, tx *fineRWTx, readOnly bool) *accountTokenFine {
	return &accountTokenFine{address: addr, tx: tx, readOnly: readOnly}
}

type accountAccessSequenceFineMap map[common.Address]*accountAccessSequenceFine

// type accountAccessSequenceFineMap sync.Map
type accountAccessSequenceFine struct {
	mutex   sync.Mutex
	address common.Address

	pendingTxs            *fineRWTxs // 该地址等待执行的队列
	slotAccessSequenceMap *slotAccessSequenceFineMap
	//slotAccessSequenceMap *sync.Map
	checkIntegrity map[int]int // 交易在该地址下已经获取了多少个状态, txIndexInAccessSequence -> got slot state num
	slotNum        []int       // 在该地址下的该交易需要获取多少个状态才能执行, txIndexInAccessSequence -> slotNum
	readyTxs       []int       // 准备就绪队列 (等待获取状态并执行的队列, 需按照顺序进行排列)

	executingTxsNum   int // 正在执行的交易个数
	readAccessTxsNum  int // 获取了读权限交易的个数
	writeAccessTxsNum int // 获取了写权限交易的个数
	pendingIndex      int // 最新的等待执行的交易在pendingTxs中的下标
	finishTxsNum      int // 已完成执行的事务
	len               int // pending的总长度
}

func newAccountAccessSequenceFine(addr common.Address, txs *fineRWTxs, len int) *accountAccessSequenceFine {
	testMap := make(slotAccessSequenceFineMap)
	return &accountAccessSequenceFine{
		address:               addr,
		pendingTxs:            txs,
		slotAccessSequenceMap: &testMap,
		checkIntegrity:        make(map[int]int), // 交易在该地址下已经获取了多少个状态, txIndexInAccessSequence -> slotNum
		//slotNum:               []int,       // 在该地址下的该交易需要获取多少个状态才能执行, txIndexInAccessSequence -> slotNum
		readyTxs:          make([]int, 0), // 准备就绪队列 (等待获取状态并执行的队列, 需按照顺序进行排列)
		executingTxsNum:   0,              // 接收到事务返回的令牌时，表示已经有一个事务执行完成
		readAccessTxsNum:  0,
		writeAccessTxsNum: 0,
		pendingIndex:      0,
		finishTxsNum:      0,
		len:               len,
	}
}

type slotAccessSequenceFineMap map[common.Hash]*slotAccessSequenceFine
type slotAccessSequenceFine struct {
	//mutex                   sync.Mutex
	slotKey                 common.Hash
	pendingTxs              *fineRWTxs // 该地址等待执行的队列
	txIndexInAccessSequence []int      // 交易序列在accessSequence中的位置

	executingTxsNum   int // 正在执行的交易个数
	readAccessTxsNum  int // 获取了读权限交易的个数
	writeAccessTxsNum int // 获取了写权限交易的个数
	pendingIndex      int // 最新的等待执行的交易在pendingTxs中的下标
	finishTxsNum      int // 已完成执行的事务
	len               int // pending的总长度
}

func newSlotAccessSequenceFine(slotKey common.Hash, txs *fineRWTxs, len_ int, txIndexInAccessSequenceMap *map[common.Hash]int) *slotAccessSequenceFine {
	s := &slotAccessSequenceFine{
		slotKey:                 slotKey,
		pendingTxs:              txs,
		txIndexInAccessSequence: make([]int, len_),
		executingTxsNum:         0, // 接收到事务返回的令牌时，表示已经有一个事务执行完成
		readAccessTxsNum:        0,
		writeAccessTxsNum:       0,
		pendingIndex:            0,
		finishTxsNum:            0,
		len:                     len_,
	}
	for i, tx := range *txs {
		s.txIndexInAccessSequence[i] = (*txIndexInAccessSequenceMap)[tx.tx.Hash()]
	}
	return s
}

func ChuKoNuConcurrencyFineRW(block *types.Block, stmStateDB *state.StmStateDB, cfg vm.Config, config *params.ChainConfig, chainDb ethdb.Database) {
	var (
		disNum  = 3
		execNum = 5
		//num      = runtime.NumCPU()
		checkNum = 2
		disWg    sync.WaitGroup
		disMutex sync.RWMutex
		disCh    = make(distributeFineRWChan, chanSize)

		checkWg sync.WaitGroup
		checkCh = make([]checkFineRWChan, checkNum)

		execWg sync.WaitGroup
		execCh = make(executionFineRWChan, chanSize)

		closeWg sync.WaitGroup
		closeCh = make(closeChan, chanSize/2)

		feeCh = make(feeChan, chanSize)

		accountAccessSequences = make(accountAccessSequenceFineMap)
	)
	//constructionOrderFineRW(block, &accountAccessSequences, stmStateDB) // 依赖队列的长度
	queueLen, addresses := constructionOrderFineRW(block, &accountAccessSequences, stmStateDB) // 依赖队列的长度

	startTime := time.Now()
	//queueLen, addresses := constructionOrderFineRW(block, &accountAccessSequences, stmStateDB) // 依赖队列的长度

	//fmt.Println(queueLen, addresses)
	disWg.Add(disNum)
	for i := 0; i < disNum; i++ {
		go distributeTxsFineRW(&accountAccessSequences, disCh, &checkCh, closeCh, &disWg, &disMutex, checkNum)
	}

	checkWg.Add(checkNum) //并发: Cpu核数=并发线程数
	for i := 0; i < checkNum; i++ {
		checkCh[i] = make(checkFineRWChan, chanSize/checkNum+1)
		go checkTxsFineRW(checkCh[i], execCh, &checkWg)
	}

	execWg.Add(execNum)
	for i := 0; i < execNum; i++ {
		go executionTxsFineRW(execCh, disCh, feeCh, block, stmStateDB, cfg, config, chainDb, &execWg)
	}

	for _, addr := range addresses {
		disCh <- newAccountTokenFine(addr, nil, true)
	}

	closeWg.Add(1)
	go closeChuKoNuFineRW(closeCh, disCh, &checkCh, execCh, queueLen, &closeWg, checkNum)

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

	fmt.Println(root, "ChuKoNu Fine RW", float64(block.Transactions().Len())/time.Since(startTime).Seconds())
}

func constructionOrderFineRW(block *types.Block, accountAccessSequences *accountAccessSequenceFineMap, stmStateDB *state.StmStateDB) (int, []common.Address) {
	seQueue := make(distributeFineRWMap)

	fineTxs := make(fineRWTxs, 0)
	for i, tx := range block.Transactions() {
		tx.Index = i
		fineTxs = append(fineTxs, newFineRWTx(tx, state.NewStmTransaction(tx, tx.Index, stmStateDB, false)))
	}

	// 	地址对应的队列
	for _, tx := range fineTxs {
		tx.tx.AccessSum = len(*tx.tx.AccessPre)
		for addr, _ := range *tx.tx.AccessPre {
			if _, ok := seQueue[addr]; !ok {
				seQueue[addr] = make(fineRWTxs, 0)
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
		//// Make sure the sender is an EOA
		//codeHash := st.state.GetCodeHash(msg.From)
		//if codeHash != (common.Hash{}) && codeHash != types.EmptyCodeHash {
		//	return fmt.Errorf("%w: address %v, codehash: %s", ErrSenderNoEOA,
		//		msg.From.Hex(), codeHash)
		//}
		(*accountAccessSequences)[val.addr] = newAccountAccessSequenceFine(val.addr, &list, len(list))
	}

	var (
		conSlotNum       = runtime.NumCPU()
		conSlotWg        sync.WaitGroup
		accessSequenceCh = make(initAccountAccessSequenceChan, chanSize)
	)
	conSlotWg.Add(conSlotNum)
	for i := 0; i < conSlotNum; i++ {
		go constructionSlot(accessSequenceCh, &conSlotWg)
	}
	for _, accessSequence := range *accountAccessSequences {
		accessSequenceCh <- accessSequence
	}
	close(accessSequenceCh)
	addresses := make([]common.Address, 0)
	for _, val := range listTxsByAddr {
		addresses = append(addresses, val.addr)
	}
	conSlotWg.Wait()

	return len(listTxsByAddr), addresses
}

func constructionSlot(accessSequenceCh initAccountAccessSequenceChan, wg *sync.WaitGroup) {
	defer wg.Done()
	for accessSequence := range accessSequenceCh {
		seQueue := make(slotTxSequenceMap)
		accessSequence.slotNum = make([]int, len(*accessSequence.pendingTxs))

		txIndexInAccessSequenceMap := make(map[common.Hash]int)

		// 	地址对应的队列
		for i, tx := range *accessSequence.pendingTxs {
			txIndexInAccessSequenceMap[tx.tx.Hash()] = i
			accessAddr := (*tx.tx.AccessPre)[accessSequence.address]

			if len(*accessAddr.Slots) == 0 { // 如果没有访问Slot, 则至少访问了account
				accessSequence.slotNum[i] = 1
			} else {
				accessSequence.slotNum[i] = len(*accessAddr.Slots) + 1
				for key, _ := range *accessAddr.Slots {
					if _, ok := seQueue[key]; !ok {
						seQueue[key] = make(fineRWTxs, 0)
					}
					seQueue[key] = append(seQueue[key], tx)
				}
			}
			if accessSequence.writeAccessTxsNum == 0 { // 没有写的事务正在执行则可以授予权限
				accessSequence.checkIntegrity[i] += 1
				accessSequence.executingTxsNum += 1
				accessSequence.pendingIndex += 1

				if !accessAddr.IsWrite { // 非写
					accessSequence.readAccessTxsNum += 1
				} else {
					accessSequence.writeAccessTxsNum += 1
				}
			}
		}

		type txsByAddr struct {
			key common.Hash
			num int
		}

		var listTxsByAddr []txsByAddr
		for key, vch := range seQueue {
			listTxsByAddr = append(listTxsByAddr, txsByAddr{key, len(vch)})
		}

		//sort.Slice(listTxsByAddr, func(i, j int) bool {
		//	return listTxsByAddr[i].num > listTxsByAddr[j].num // 降序
		//})

		for _, val := range listTxsByAddr {
			list := seQueue[val.key]
			(*accessSequence.slotAccessSequenceMap)[val.key] = newSlotAccessSequenceFine(val.key, &list, len(list), &txIndexInAccessSequenceMap)
		}

		//if tx.tx.Hash() == common.HexToHash("0xfbf88792b6d9c10aa22faafd43680460da96b5bd9fc307ec824e24dcc3efe8b4") {
		//	fmt.Println(accessSequence.address, tx.tx.Hash(), len(*accessAddr.Slots))
		//}

		for slotKey, slotAccessSequence := range *accessSequence.slotAccessSequenceMap {
			for i, tx := range *slotAccessSequence.pendingTxs {
				accessAddr := (*tx.tx.AccessPre)[accessSequence.address]
				accessSlot := (*accessAddr.Slots)[slotKey]
				if slotAccessSequence.writeAccessTxsNum == 0 { // 非写，可以授予权限
					accessSequence.checkIntegrity[slotAccessSequence.txIndexInAccessSequence[i]] += 1

					slotAccessSequence.executingTxsNum += 1
					slotAccessSequence.pendingIndex += 1
					if !accessSlot.IsWrite {
						slotAccessSequence.readAccessTxsNum += 1
					} else {
						slotAccessSequence.writeAccessTxsNum += 1
					}
					//if accessSequence.address == common.HexToAddress("0xCaDb96858Fe496Bb6309622F9023BA2DEfb5d540") {
					//	fmt.Println(accessSequence.address, accessSequence.checkIntegrity, accessSequence.readyTxs, accessSequence.slotNum[0])
					//}
				}
			}
		}

		for txIndexInAccessSequence, haveSlotNum := range accessSequence.checkIntegrity {
			if accessSequence.slotNum[txIndexInAccessSequence] == haveSlotNum { // 获取完了所有的状态，可以执行
				accessSequence.readyTxs = append(accessSequence.readyTxs, txIndexInAccessSequence)
			}
		}

		sort.Ints(accessSequence.readyTxs) // 需从小到大授予权限

		for _, txIndexInAccessSequence := range accessSequence.readyTxs {
			//fmt.Println((*accessSequence.pendingTxs)[txIndexInAccessSequence].tx.Hash())
			delete(accessSequence.checkIntegrity, txIndexInAccessSequence)
		}
		//if accessSequence.address == common.HexToAddress("0xCaDb96858Fe496Bb6309622F9023BA2DEfb5d540") {
		//	fmt.Println("accessSequence", accessSequence.address, accessSequence.readyTxs)
		//}
		//fmt.Println(accessSequence.address, accessSequence.readyTxs)
	}
}

func distributeTxsFineRW(accountAccessSequences *accountAccessSequenceFineMap, disCh distributeFineRWChan, checkCh *[]checkFineRWChan, closeCh closeChan, wg *sync.WaitGroup, disMutex *sync.RWMutex, num int) {
	defer wg.Done()
	for token := range disCh {
		// 获取账户相应的访问队列
		disMutex.RLock()
		accessSequence := (*accountAccessSequences)[token.address]
		disMutex.RUnlock()

		// 处理访问队列
		accessSequence.mutex.Lock()

		//if token.address == common.HexToAddress("0xCaDb96858Fe496Bb6309622F9023BA2DEfb5d540") {
		//	fmt.Println(token.address, len(*accessSequence.slotAccessSequenceMap))
		//	//for slotKey, slotAccessSequence := range *accessSequence.slotAccessSequenceMap {
		//	//	for _, tx := range *slotAccessSequence.pendingTxs {
		//	//		accessAddr := (*tx.tx.AccessPre)[accessSequence.address]
		//	//		accessSlot := (*accessAddr.Slots)[slotKey]
		//	//		if accessSequence.writeAccessTxsNum == 0 { // 非写，可以授予权限
		//	//			accessSequence.checkIntegrity[tx.txIndexInAccessSequence] += 1
		//	//
		//	//			slotAccessSequence.executingTxsNum += 1
		//	//			slotAccessSequence.pendingIndex += 1
		//	//			if !accessSlot.IsWrite {
		//	//				slotAccessSequence.readAccessTxsNum += 1
		//	//			} else {
		//	//				slotAccessSequence.writeAccessTxsNum += 1
		//	//			}
		//	//		}
		//	//	}
		//	//}
		//}

		if token.tx != nil { // 有一个事务完成执行
			// 处理账户状态的结束工作
			accessSequence.finishTxsNum += 1
			accessSequence.executingTxsNum -= 1
			if token.readOnly {
				accessSequence.readAccessTxsNum -= 1
			} else {
				accessSequence.writeAccessTxsNum -= 1
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
		}
		//else {
		//	if token.address == common.HexToAddress("0xCaDb96858Fe496Bb6309622F9023BA2DEfb5d540") {
		//		fmt.Println("dis -> check 1", accessSequence.address, accessSequence.readyTxs)
		//	}
		//}

		if token.tx != nil { // 处理返回来事务的的令牌重新授予
			for {
				if accessSequence.pendingIndex == accessSequence.len {
					break // 该地址的所有事务都已授予过权限了
				}

				if accessSequence.writeAccessTxsNum == 0 { // 没有写的事务正在执行则可以授予权限
					tx := (*accessSequence.pendingTxs)[accessSequence.pendingIndex] // accessSequence.pendingIndex 当前可执行的交易下标，递增
					txAccessPre := (*tx.tx.AccessPre)[token.address]

					accessSequence.checkIntegrity[accessSequence.pendingIndex] += 1
					accessSequence.executingTxsNum += 1
					accessSequence.pendingIndex += 1

					if !txAccessPre.IsWrite { // 非写
						accessSequence.readAccessTxsNum += 1
					} else {
						accessSequence.writeAccessTxsNum += 1
						break // 有一个写事务，则不能继续授予权限了
					}
				}
			}

			txAccessPre := (*token.tx.tx.AccessPre)[token.address]
			for slotKey, slotAccess := range *txAccessPre.Slots {
				slotAccessSequence := (*accessSequence.slotAccessSequenceMap)[slotKey]
				// 处理账户状态的结束工作
				slotAccessSequence.finishTxsNum += 1
				slotAccessSequence.executingTxsNum -= 1
				if !slotAccess.IsWrite {
					slotAccessSequence.readAccessTxsNum -= 1
				} else {
					slotAccessSequence.writeAccessTxsNum -= 1
				}

				// 授予新的令牌
				for {
					if slotAccessSequence.pendingIndex == slotAccessSequence.len {
						break // 该地址的所有事务都已授予过权限了
					}

					if slotAccessSequence.writeAccessTxsNum == 0 { // 没有写的事务正在执行则可以授予权限
						tx := (*slotAccessSequence.pendingTxs)[slotAccessSequence.pendingIndex] // accessSequence.pendingIndex 当前可执行的交易下标，递增
						txAccessSlot := (*(*tx.tx.AccessPre)[token.address].Slots)[slotKey]

						accessSequence.checkIntegrity[slotAccessSequence.txIndexInAccessSequence[slotAccessSequence.pendingIndex]] += 1
						slotAccessSequence.executingTxsNum += 1
						slotAccessSequence.pendingIndex += 1

						if !txAccessSlot.IsWrite { // 非写
							slotAccessSequence.readAccessTxsNum += 1
						} else {
							slotAccessSequence.writeAccessTxsNum += 1
							break // 有一个写事务，则不能继续授予权限了
						}
					}
				}

			}

			for txIndexInAccessSequence, haveSlotNum := range accessSequence.checkIntegrity {
				//fmt.Println(len(*accessSequence.pendingTxs), txIndexInAccessSequence)
				if accessSequence.slotNum[txIndexInAccessSequence] == haveSlotNum { // 获取完了所有的状态，可以执行
					accessSequence.readyTxs = append(accessSequence.readyTxs, txIndexInAccessSequence)
				}
			}

			sort.Ints(accessSequence.readyTxs) // 需从小到大授予权限

			for _, txIndexInAccessSequence := range accessSequence.readyTxs {
				//fmt.Println((*accessSequence.pendingTxs)[txIndexInAccessSequence].tx.Hash())
				delete(accessSequence.checkIntegrity, txIndexInAccessSequence)
			}

			//fmt.Println(accessSequence.address, accessSequence.readyTxs)
		}

		//if token.address == common.HexToAddress("0xCaDb96858Fe496Bb6309622F9023BA2DEfb5d540") {
		//	fmt.Println("dis -> check", accessSequence.address, accessSequence.readyTxs)
		//}

		// 派发可以执行的事务
		for _, txIndexInSequence := range accessSequence.readyTxs {
			tx := (*accessSequence.pendingTxs)[txIndexInSequence] // 可执行队列中的事务
			txAccessPre := (*tx.tx.AccessPre)[token.address]

			//fmt.Println("distribute", token.address, tx.tx.Hash())

			tx.txdb.GetAccountState(token.address, txAccessPre)
			if txAccessPre.IsWrite {
				(*checkCh)[tx.tx.Index%num] <- newAccountTokenFine(token.address, tx, false) // 发送给对应的检查线程，根据tx.index进行选择
			} else {
				(*checkCh)[tx.tx.Index%num] <- newAccountTokenFine(token.address, tx, true) // 发送给对应的检查线程，根据tx.index进行选择
			}
		}
		accessSequence.readyTxs = accessSequence.readyTxs[:0]
		accessSequence.mutex.Unlock()
	}
}

func checkTxsFineRW(checkCh checkFineRWChan, execCh executionFineRWChan, wg *sync.WaitGroup) {
	wg.Done()
	queueByAddr := make(map[common.Hash][]*accountTokenFine)
	for token := range checkCh {
		var tokens []*accountTokenFine
		//fmt.Println("checkTxsFineRW", token.tx.tx.Hash(), token.address)
		if _, ok := queueByAddr[token.tx.tx.Hash()]; !ok { // 第一次接收到该交易的依赖
			tokens = make([]*accountTokenFine, 0)
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

func executionTxsFineRW(execCh executionFineRWChan, disCh distributeFineRWChan, feeCh feeChan, block *types.Block, stmStateDB *state.StmStateDB, cfg vm.Config, config *params.ChainConfig, chainDb ethdb.Database, wg *sync.WaitGroup) {
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

func closeChuKoNuFineRW(closeCh closeChan, disCh distributeFineRWChan, checkCh *[]checkFineRWChan, execCh executionFineRWChan, queueLen int, wg *sync.WaitGroup, num int) {
	defer wg.Done()
	var finishNum = 0
	for _ = range closeCh {
		finishNum += 1
		//fmt.Println(finishNum)
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
