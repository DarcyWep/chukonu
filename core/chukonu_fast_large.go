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
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type ChuKoNuLargeTxs []*ChuKoNuLargeTx

// ChuKoNuLargeTx bind tx state db for tx
type ChuKoNuLargeTx struct {
	tx      *types.Transaction
	txdb    *state.ChuKoNuTxStateDB
	block   *types.Block
	rewards *map[common.Address]*big.Int
}

func NewChuKoNuLargeTx(tx *types.Transaction, txdb *state.ChuKoNuTxStateDB, block *types.Block, rewards *map[common.Address]*big.Int) *ChuKoNuLargeTx {
	return &ChuKoNuLargeTx{
		tx:      tx,
		txdb:    txdb,
		block:   block,
		rewards: rewards,
	}
}

type SequenceLarge struct {
	slotKey               common.Hash
	sequence              ChuKoNuLargeTxs // 该地址等待执行的队列
	sequenceLen           int
	pendingExecutionIndex int  // 最新的等待执行的交易在pendingTxs中的下标
	isWrite               bool // 如果有交易获取了写权限, 则该序列停止权限授予
}

func newSequenceLarge(slot common.Hash) *SequenceLarge {
	return &SequenceLarge{
		slotKey:  slot,
		sequence: make(ChuKoNuLargeTxs, 0),
	}
}

func (s *SequenceLarge) append(tx *ChuKoNuLargeTx) {
	s.sequence = append(s.sequence, tx)
}

func (s *SequenceLarge) setLen() {
	s.sequenceLen = len(s.sequence)
}
func (s *SequenceLarge) String() string {
	str := ""
	for _, tx := range s.sequence {
		str += strconv.Itoa(tx.tx.Index) + " "
	}
	return str
}

type accountInfoLarge struct {
	address         common.Address // 传输的是哪个地址的令牌
	accountState    *state.ChuKoNuStateObject
	atomicCounter   atomic.Bool
	accountSequence *SequenceLarge
	slotSequence    map[common.Hash]*SequenceLarge

	finishedTxNum  int   // 结束了多少个事务，该地址下所有的事务都执行完毕，则通知监控线程
	checkIntegrity []int // 交易在该地址下已经获取了多少个状态, tx.Index -> slot tokens were got
	needSlotToken  []int // 交易在该地址下已经获取多少个状态才能执行, tx.Index -> all slot tokens need be got
	readyTxs       []int
}

func newAccountInfoLarge(addr common.Address, txsLen int) *accountInfoLarge {
	ai := &accountInfoLarge{
		address:         addr,
		accountSequence: newSequenceLarge(common.Hash{}),
		slotSequence:    make(map[common.Hash]*SequenceLarge), // EOA is not have slotSequence
		checkIntegrity:  make([]int, txsLen),
		needSlotToken:   make([]int, txsLen),
		readyTxs:        make([]int, 0),
	}
	ai.atomicCounter.Store(false)
	return ai
}

type tokenLarge struct {
	address                  common.Address
	account                  *accountInfoLarge
	tx                       *ChuKoNuLargeTx
	stateToTxDB              *state.StateTokenToTxDB
	stateTokenToAccountState *state.StateTokenToAccountState
	rwSet                    *types.AccessAddress
}

func newTokenLarge(address common.Address, account *accountInfoLarge, tx *ChuKoNuLargeTx, stateToTxDB *state.StateTokenToTxDB, rwSet *types.AccessAddress) *tokenLarge {
	return &tokenLarge{
		address:     address,
		account:     account,
		tx:          tx,
		stateToTxDB: stateToTxDB,
		rwSet:       rwSet,
	}
}

type (
	tokenLargeChan    chan *tokenLarge
	executorLargeChan chan *[]*tokenLarge
)

type ChuKoNuFastLargeProcessor struct {
	config             *params.ChainConfig // Chain configuration options
	chainDb            ethdb.Database      // Canonical block chain
	cknTxs             ChuKoNuLargeTxs
	tokenManagerNumCPU int
	schedulerNumCPU    int
	executorNumCPU     int
	tokenManagerWg     sync.WaitGroup
	tokenManagerCh     tokenLargeChan
	schedulerWg        sync.WaitGroup
	schedulerCh        []tokenLargeChan // 用多个chan实现
	executorWg         sync.WaitGroup
	executorCh         executorLargeChan
	closeWg            sync.WaitGroup
	closeCh            closeChan
	feeCh              feeChan

	slotConflictDetectionNum int
}

func NewChuKoNuFastLargeProcessor(config *params.ChainConfig, chainDb ethdb.Database, txs ChuKoNuLargeTxs, statedb *state.ChuKoNuStateDB) *ChuKoNuFastLargeProcessor {
	schedulerNumCPU := 4
	cp := &ChuKoNuFastLargeProcessor{
		config:             config,
		chainDb:            chainDb,
		cknTxs:             txs,
		tokenManagerNumCPU: 10,
		schedulerNumCPU:    schedulerNumCPU,
		executorNumCPU:     18,
		tokenManagerCh:     make(tokenLargeChan, chanSize),
		schedulerCh:        make([]tokenLargeChan, schedulerNumCPU), // 用多个chan实现
		executorCh:         make(executorLargeChan, chanSize),
		closeCh:            make(closeChan, chanSize/2),
		feeCh:              make(feeChan, 10),
		//slotConflictDetectionNum: runtime.NumCPU(),
		slotConflictDetectionNum: 32,
	}
	return cp
}

func (p *ChuKoNuFastLargeProcessor) ChuKoNuFast(statedb *state.ChuKoNuStateDB, cfg vm.Config) time.Duration {
	addresses, accountLen := p.accountConflictDetection(statedb) // 依赖队列的长度

	startTime := time.Now()
	p.tokenManagerWg.Add(p.tokenManagerNumCPU)
	for i := 0; i < p.tokenManagerNumCPU; i++ {
		go p.tokenManager(statedb)
	}

	p.schedulerWg.Add(p.schedulerNumCPU) //并发: Cpu核数=并发线程数
	for i := 0; i < p.schedulerNumCPU; i++ {
		p.schedulerCh[i] = make(tokenLargeChan, chanSize/p.schedulerNumCPU+1)
		go p.scheduler(p.schedulerCh[i], i)
	}

	p.executorWg.Add(p.executorNumCPU)
	for i := 0; i < p.executorNumCPU; i++ {
		go p.executor(cfg)
	}

	for _, accInfo := range addresses {
		p.tokenManagerCh <- newTokenLarge(accInfo.address, accInfo, nil, nil, nil)
	}

	p.closeWg.Add(1)
	go p.close(accountLen)

	p.closeWg.Wait()
	p.tokenManagerWg.Wait()
	p.schedulerWg.Wait()
	p.executorWg.Wait()
	close(p.feeCh)
	statedb.IntermediateRoot(true)

	return time.Since(startTime)
}

func (p *ChuKoNuFastLargeProcessor) accountConflictDetection(statedb *state.ChuKoNuStateDB) (map[common.Address]*accountInfoLarge, int) {

	accountQueue := make(map[common.Address]*accountInfoLarge)

	txsLen := len(p.cknTxs)

	// 	地址对应的队列
	for _, tx := range p.cknTxs {
		tx.tx.AccessSum = len(*tx.tx.AccessPre)
		for addr, _ := range *tx.tx.AccessPre {
			if _, ok := accountQueue[addr]; !ok {
				accountQueue[addr] = newAccountInfoLarge(addr, txsLen)
			}
			accountQueue[addr].accountSequence.append(tx)
		}
	}
	//type txsByAddr struct {
	//	addr    common.Address
	//	accInfo *accountInfoLarge
	//	num     int
	//}
	//var listTxsByAddr []txsByAddr
	//for key, accQueue := range accountQueue {
	//	accQueue.accountSequence.setLen()
	//	listTxsByAddr = append(listTxsByAddr, txsByAddr{key, accQueue, accQueue.accountSequence.sequenceLen})
	//}
	//sort.Slice(listTxsByAddr, func(i, j int) bool {
	//	return listTxsByAddr[i].num > listTxsByAddr[j].num // 降序
	//})

	var (
		slotConflictDetectionWg sync.WaitGroup
		accountInfoCh           = make(chan *accountInfoLarge, chanSize)
	)
	slotConflictDetectionWg.Add(p.slotConflictDetectionNum)
	for i := 0; i < p.slotConflictDetectionNum; i++ {
		go p.slotConflictDetection(accountInfoCh, &slotConflictDetectionWg, statedb)
	}
	//for _, val := range listTxsByAddr {
	//	accountInfoCh <- val.accInfo
	//}
	for _, val := range accountQueue {
		//val.accountSequence.setLen()
		accountInfoCh <- val
	}
	close(accountInfoCh)
	slotConflictDetectionWg.Wait()

	return accountQueue, len(accountQueue)
	//return len(accountQueue)
}

func (p *ChuKoNuFastLargeProcessor) slotConflictDetection(accountInfoCh chan *accountInfoLarge, wg *sync.WaitGroup, statedb *state.ChuKoNuStateDB) {
	defer wg.Done()
	for accInfo := range accountInfoCh {
		accInfo.accountSequence.setLen()
		slots := make(map[common.Hash]struct{}) // 整个地址下所访问的所有的slots, 为状态提取做准备
		grantedTxIndexes := make(map[int]struct{}, 0)
		// 	地址对应的队列
		for _, cknTx := range accInfo.accountSequence.sequence {
			tx := cknTx.tx
			addressRW := (*tx.AccessPre)[accInfo.address]

			if len(*addressRW.Slots) == 0 { // 如果没有访问Slot, 则至少访问了account
				accInfo.needSlotToken[tx.Index] = 1
			} else {
				accInfo.needSlotToken[tx.Index] = len(*addressRW.Slots) + 1 // 所有的 Slot 状态 加上 Account 状态
				for slot, _ := range *addressRW.Slots {
					slots[slot] = struct{}{}

					if _, ok := accInfo.slotSequence[slot]; !ok {
						accInfo.slotSequence[slot] = newSequenceLarge(slot)
					}

					accInfo.slotSequence[slot].append(cknTx)
				}
			}

			// account sequence 初始权限授予
			if !accInfo.accountSequence.isWrite { // 没有事务拿到写权限，则可以一直授予权限
				accInfo.checkIntegrity[tx.Index] += 1
				accInfo.accountSequence.pendingExecutionIndex += 1
				grantedTxIndexes[tx.Index] = struct{}{} // 用于授予权限

				if addressRW.IsWrite { // 如果是写, 则标记接下来不能继续授予权限
					accInfo.accountSequence.isWrite = true
				}
			}
		}

		// 处理account下的每个slot sequence, 将它们的权限授予出去
		for slot, slotSequence := range accInfo.slotSequence {
			//if accInfo.address == common.HexToAddress("0xA68Dd8cB83097765263AdAD881Af6eeD479c4a33") {
			//	fmt.Println(slot, slotSequence)
			//}
			slotSequence.setLen() // 要设置好slot的长度, 用于判断结束
			for _, cknTx := range slotSequence.sequence {
				tx := cknTx.tx
				addressRW := (*tx.AccessPre)[accInfo.address]
				slotRW := (*addressRW.Slots)[slot]

				accInfo.checkIntegrity[tx.Index] += 1
				grantedTxIndexes[tx.Index] = struct{}{} // 用于授予权限

				slotSequence.pendingExecutionIndex += 1
				if slotRW.IsWrite { // 如果是写, 则不能再继续授予权限了
					slotSequence.isWrite = true
					break // 接下来该序列不可能继续授予权限了，直接跳出
				}
			}
		}
		//0x5Cb7880035bD592a66aad803ce1Cdf6Aa385e2a1 0 2 4
		//0xA68Dd8cB83097765263AdAD881Af6eeD479c4a33 0 2 4

		// 获取完了所有状态的事务，可以执行
		accInfo.readyTxs = accInfo.readyTxs[:0]
		for txIndex, _ := range grantedTxIndexes {
			if accInfo.checkIntegrity[txIndex] == accInfo.needSlotToken[txIndex] {
				accInfo.readyTxs = append(accInfo.readyTxs, txIndex)
			}
		}

		sort.Ints(accInfo.readyTxs) // 需要从 tx 下标小的开始授予权限, 防止写事务先授予，然后读事务没授予完就已经返回更改了状态

		// 获取状态
		accInfo.accountState = statedb.CreateAccountObject(accInfo.address)
		for slot, _ := range slots {
			accInfo.accountState.SetOriginState(slot, statedb.GetOriginState(accInfo.address, slot))
		}
	}
}

func (p *ChuKoNuFastLargeProcessor) recycleToken(token *tokenLarge, statedb *state.ChuKoNuStateDB) {
	grantedTxIndexes := make(map[int]struct{}, 0) // 用于统计授予了哪些事务相关权限, 方便后续的令牌发放
	token.account.finishedTxNum += 1              // 完成的事务 + 1, 用于停止线程

	if token.stateTokenToAccountState != nil {
		token.stateTokenToAccountState.UpdateAccountState(token.account.accountState) // 更新账户的状态
	}

	// 查看是否已经执行完所有的事务
	if token.account.finishedTxNum == token.account.accountSequence.sequenceLen { // 所有的事务都完成了执行, 结束后续的权限授予
		statedb.UpdateByAccountObject(token.account.accountState) // 将临时账户状态更新至世界状态
		ok := token.account.atomicCounter.CompareAndSwap(true, false)
		if !ok {
			fmt.Printf("finishing rights granted failed")
		}
		//fmt.Println(token.tx.tx.Index)
		p.closeCh <- true
		return // 结束当前账户的所有交易
	}

	// 处理事务结束后账户序列的权限
	if token.rwSet.IsWrite { // 账户是写操作，为账户序列继续授予权限
		accInfo := token.account
		accInfo.accountSequence.isWrite = false // 允许继续授予权限
		for ; accInfo.accountSequence.pendingExecutionIndex < accInfo.accountSequence.sequenceLen; accInfo.accountSequence.pendingExecutionIndex++ {
			tx := accInfo.accountSequence.sequence[accInfo.accountSequence.pendingExecutionIndex].tx
			addressRW := (*tx.AccessPre)[accInfo.address]

			accInfo.checkIntegrity[tx.Index] += 1
			grantedTxIndexes[tx.Index] = struct{}{} // 用于授予权限

			if addressRW.IsWrite { // 如果是写, 则不能再继续授予权限了
				accInfo.accountSequence.isWrite = true
				accInfo.accountSequence.pendingExecutionIndex += 1 // 退出前要把下标 + 1, 否则之后就无法发放令牌了
				break                                              // 接下来该队列不可能再授予权限了, 直接退出
			}
		}
	}

	//if token.account.address == common.HexToAddress("0xA68Dd8cB83097765263AdAD881Af6eeD479c4a33") && token.tx.tx.Index == 2 {
	//	fmt.Println(token.rwSet.Slots)
	//}
	// 处理事务结束后每个slot序列的权限
	//fmt.Println("len(*token.rwSet.Slots):", len(*token.rwSet.Slots))
	for slot, tokenSlotRW := range *token.rwSet.Slots {
		if tokenSlotRW.IsWrite {
			slotSequence := token.account.slotSequence[slot]
			slotSequence.isWrite = false // 允许继续授予权限

			for ; slotSequence.pendingExecutionIndex < slotSequence.sequenceLen; slotSequence.pendingExecutionIndex++ {
				tx := slotSequence.sequence[slotSequence.pendingExecutionIndex].tx
				addressRW := (*tx.AccessPre)[token.address]
				slotRW := (*addressRW.Slots)[slot]

				token.account.checkIntegrity[tx.Index] += 1
				grantedTxIndexes[tx.Index] = struct{}{} // 用于授予权限

				if slotRW.IsWrite { // 如果是写, 则不能再继续授予权限了
					slotSequence.isWrite = true
					slotSequence.pendingExecutionIndex += 1 // 退出前要把下标 + 1, 否则之后就无法发放令牌了
					break                                   // 接下来该队列不可能再授予权限了, 直接退出
				}
			}
		}
	}

	//if token.account.address == common.HexToAddress("0xA68Dd8cB83097765263AdAD881Af6eeD479c4a33") {
	//	fmt.Println(grantedTxIndexes, token.account.checkIntegrity[4], token.account.needSlotToken[4])
	//}
	//fmt.Println(len(grantedTxIndexes))
	if len(grantedTxIndexes) > 0 { // 获取完了所有状态的事务，可以分发权限
		token.account.readyTxs = token.account.readyTxs[:0]
		for txIndex, _ := range grantedTxIndexes {
			if token.account.checkIntegrity[txIndex] == token.account.needSlotToken[txIndex] {
				token.account.readyTxs = append(token.account.readyTxs, txIndex)
			}
		}
		sort.Ints(token.account.readyTxs) // 需要从 tx 下标小的开始授予权限, 防止写事务先授予，然后读事务没授予完就已经返回更改了状态
		for _, txIndex := range token.account.readyTxs {
			rwSet := (*p.cknTxs[txIndex].tx.AccessPre)[token.address]
			slots := make([]common.Hash, len(*rwSet.Slots))
			for key, _ := range *rwSet.Slots {
				slots = append(slots, key)
			}
			p.schedulerCh[txIndex%p.schedulerNumCPU] <- newTokenLarge(token.account.address, token.account, p.cknTxs[txIndex],
				state.NewStateTokenToTxDB(token.address, token.account.accountState, slots, token.account.accountState.Deleted()),
				rwSet)
		}
	}

	ok := token.account.atomicCounter.CompareAndSwap(true, false)

	if !ok {
		fmt.Printf("finishing rights granted failed")
	}
}

func (p *ChuKoNuFastLargeProcessor) tokenManager(statedb *state.ChuKoNuStateDB) {
	defer p.tokenManagerWg.Done()
	for token := range p.tokenManagerCh {
		ok := token.account.atomicCounter.CompareAndSwap(false, true)
		if !ok { // 有线程正在处理该地址相关的token
			p.tokenManagerCh <- token
			//fmt.Println(token.address, token.tx.tx.Index)
			continue
		}
		if token.tx != nil {
			p.recycleToken(token, statedb)
		} else {
			for _, txIndex := range token.account.readyTxs {
				rwSet := (*p.cknTxs[txIndex].tx.AccessPre)[token.account.address]
				slots := make([]common.Hash, len(*rwSet.Slots))
				for key, _ := range *rwSet.Slots {
					slots = append(slots, key)
				}
				p.schedulerCh[txIndex%p.schedulerNumCPU] <- newTokenLarge(token.account.address, token.account, p.cknTxs[txIndex],
					state.NewStateTokenToTxDB(token.address, token.account.accountState, slots, token.account.accountState.Deleted()),
					rwSet)
			}
			ok = token.account.atomicCounter.CompareAndSwap(true, false) // 首次权限开始授予
			if !ok {
				fmt.Printf("firstly finishing rights granted failed")
			}
		}

	}
}

func (p *ChuKoNuFastLargeProcessor) scheduler(schedulerCh tokenLargeChan, i int) {
	defer p.schedulerWg.Done()
	txTokens := make(map[common.Hash][]*tokenLarge)
	for t := range schedulerCh {
		var tokens []*tokenLarge
		if _, ok := txTokens[t.tx.tx.Hash()]; !ok { // 第一次接收到该交易的依赖
			tokens = make([]*tokenLarge, 0)
		} else {
			tokens = txTokens[t.tx.tx.Hash()]
		}
		tokens = append(tokens, t)
		if len(tokens) == t.tx.tx.AccessSum { // 可以执行
			for _, to := range tokens {
				to.stateToTxDB.UpdateTxState(t.tx.txdb) // 将每个令牌的状态更新至txdb
			}
			p.executorCh <- &tokens
			delete(txTokens, t.tx.tx.Hash())
		} else { // 还不能执行
			txTokens[t.tx.tx.Hash()] = tokens
		}
		//fmt.Println(t.tx.tx.Index, len(tokens), t.tx.tx.AccessSum, i)
	}
}

func (p *ChuKoNuFastLargeProcessor) executor(cfg vm.Config) {
	defer p.executorWg.Done()
	for tokens := range p.executorCh {
		tx := (*tokens)[0].tx
		if tx.rewards != nil { // 处理矿工奖励
			for addr, val := range *tx.rewards {
				tx.txdb.AddBalance(addr, val)
			}
			tx.txdb.GenerateStateTokenToAccountState()
			// 执行完成, 回传Token给token manager
			for _, to := range *tokens {
				to.stateTokenToAccountState = tx.txdb.Token(to.address) // 带更新的状态
				p.tokenManagerCh <- to
			}
			continue
		}

		var (
			usedGas = new(uint64)
			header  = tx.block.Header()
			gp      = new(GasPool).AddGas(tx.block.GasLimit())
		)
		blockContext := NewEVMBlockContext(header, p.chainDb, nil)

		vmenv := vm.NewEVM(blockContext, vm.TxContext{}, tx.txdb, p.config, cfg)
		msg, err := TransactionToMessage(tx.tx, types.MakeSigner(p.config, header.Number), header.BaseFee)
		if err != nil {
			fmt.Printf("could not apply tx %d [%v]: %w\n", tx.tx.Index, tx.tx.Hash().Hex(), err)
		}

		tx.txdb.SetNonce(msg.From, msg.Nonce)
		// 避免Balance错误
		mgval := new(big.Int).SetUint64(msg.GasLimit)
		mgval = mgval.Mul(mgval, msg.GasPrice)
		balanceCheck := mgval
		if msg.GasFeeCap != nil {
			balanceCheck = new(big.Int).SetUint64(msg.GasLimit)
			balanceCheck = balanceCheck.Mul(balanceCheck, msg.GasFeeCap)
			balanceCheck.Add(balanceCheck, msg.Value)
		}
		tx.txdb.AddBalance(msg.From, balanceCheck)

		// Create a new context to be used in the EVM environment.
		txContext := NewEVMTxContext(msg)
		vmenv.Reset(txContext, tx.txdb)

		// Apply the transaction to the current state (included in the env).

		result, err, _ := ApplyMessage(vmenv, msg, gp)
		if err != nil {
			fmt.Printf("could not apply tx %d [%v]: %w", tx.tx.Index, tx.tx.Hash().Hex(), err)
		}
		*usedGas += result.UsedGas

		//p.feeCh <- new(big.Int).Set(fee)
		if err != nil {
			fmt.Printf("could not apply tx %d [%v]: %w", tx.tx.Index, tx.tx.Hash().Hex(), err)
		}
		tx.txdb.GenerateStateTokenToAccountState()
		// 执行完成, 回传Token给token manager
		for _, to := range *tokens {
			to.stateTokenToAccountState = tx.txdb.Token(to.address) // 带更新的状态
			p.tokenManagerCh <- to
		}
	}
}

func (p *ChuKoNuFastLargeProcessor) close(accountLen int) {
	defer p.closeWg.Done()
	var finishNum = 0
	for range p.closeCh {
		finishNum += 1
		if finishNum == accountLen {
			close(p.closeCh)
			close(p.tokenManagerCh)
			for i := 0; i < p.schedulerNumCPU; i++ {
				close(p.schedulerCh[i])
			}
			close(p.executorCh)
		}
	}
}
