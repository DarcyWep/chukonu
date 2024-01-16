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
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type chuKoNuTxs []*chuKoNuTx

// chuKoNuTx bind tx state db for tx
type chuKoNuTx struct {
	tx   *types.Transaction
	txdb *state.ChuKoNuTxStateDB
}

func newChuKoNuTx(tx *types.Transaction, txdb *state.ChuKoNuTxStateDB) *chuKoNuTx {
	return &chuKoNuTx{
		tx:   tx,
		txdb: txdb,
	}
}

type Sequence struct {
	slotKey               common.Hash
	sequence              chuKoNuTxs // 该地址等待执行的队列
	sequenceLen           int
	pendingExecutionIndex int  // 最新的等待执行的交易在pendingTxs中的下标
	isWrite               bool // 如果有交易获取了写权限, 则该序列停止权限授予
}

func newSequence(slot common.Hash) *Sequence {
	return &Sequence{
		slotKey:  slot,
		sequence: make(chuKoNuTxs, 0),
	}
}

func (s *Sequence) append(tx *chuKoNuTx) {
	s.sequence = append(s.sequence, tx)
}

func (s *Sequence) setLen() {
	s.sequenceLen = len(s.sequence)
}

func (s *Sequence) String() string {
	str := ""
	for _, tx := range s.sequence {
		str += strconv.Itoa(tx.tx.Index) + " "
	}
	return str
}

type accountInfo struct {
	address         common.Address // 传输的是哪个地址的令牌
	accountState    *state.ChuKoNuStateObject
	atomicCounter   atomic.Bool
	mutex           sync.Mutex
	accountSequence *Sequence
	slotSequence    map[common.Hash]*Sequence

	finishedTxNum  int   // 结束了多少个事务，该地址下所有的事务都执行完毕，则通知监控线程
	checkIntegrity []int // 交易在该地址下已经获取了多少个状态, tx.Index -> slot tokens were got
	needSlotToken  []int // 交易在该地址下已经获取多少个状态才能执行, tx.Index -> all slot tokens need be got
	readyTxs       []int
}

func newAccountInfo(addr common.Address, txsLen int) *accountInfo {
	ai := &accountInfo{
		address:         addr,
		accountSequence: newSequence(common.Hash{}),
		slotSequence:    make(map[common.Hash]*Sequence), // EOA is not have slotSequence
		checkIntegrity:  make([]int, txsLen),
		needSlotToken:   make([]int, txsLen),
		readyTxs:        make([]int, 0),
	}
	ai.atomicCounter.Store(false)
	return ai
}

type token struct {
	address                  common.Address
	account                  *accountInfo
	tx                       *chuKoNuTx
	stateToTxDB              *state.StateTokenToTxDB
	stateTokenToAccountState *state.StateTokenToAccountState
	rwSet                    *types.AccessAddress
}

func newToken(address common.Address, account *accountInfo, tx *chuKoNuTx, stateToTxDB *state.StateTokenToTxDB, rwSet *types.AccessAddress) *token {
	return &token{
		address:     address,
		account:     account,
		tx:          tx,
		stateToTxDB: stateToTxDB,
		rwSet:       rwSet,
	}
}

type (
	tokenChan    chan *token
	executorChan chan *[]*token
)

type ChuKoNuFastProcessor struct {
	config             *params.ChainConfig // Chain configuration options
	chainDb            ethdb.Database      // Canonical block chain
	cknTxs             chuKoNuTxs
	tokenManagerNumCPU int
	schedulerNumCPU    int
	executorNumCPU     int
	tokenManagerWg     sync.WaitGroup
	tokenManagerCh     tokenChan
	schedulerWg        sync.WaitGroup
	schedulerCh        []tokenChan // 用多个chan实现
	executorWg         sync.WaitGroup
	executorCh         executorChan
	closeWg            sync.WaitGroup
	closeCh            closeChan
	feeCh              feeChan

	slotConflictDetectionNum int
}

func NewChuKoNuFastProcessor(config *params.ChainConfig, chainDb ethdb.Database, block *types.Block, statedb *state.ChuKoNuStateDB) *ChuKoNuFastProcessor {
	schedulerNumCPU := 1
	cp := &ChuKoNuFastProcessor{
		config:                   config,
		chainDb:                  chainDb,
		cknTxs:                   make(chuKoNuTxs, block.Transactions().Len()),
		tokenManagerNumCPU:       1,
		schedulerNumCPU:          schedulerNumCPU,
		executorNumCPU:           30,
		tokenManagerCh:           make(tokenChan, chanSize),
		schedulerCh:              make([]tokenChan, schedulerNumCPU), // 用多个chan实现
		executorCh:               make(executorChan, chanSize),
		closeCh:                  make(closeChan, chanSize/2),
		feeCh:                    make(feeChan, block.Transactions().Len()+10),
		slotConflictDetectionNum: runtime.NumCPU(),
	}
	for i, tx := range block.Transactions() {
		tx.Index = i
		cp.cknTxs[i] = newChuKoNuTx(tx, state.NewChuKoNuTxStateDB(statedb))
	}
	return cp
}

func (p *ChuKoNuFastProcessor) ChuKoNuFast(block *types.Block, statedb *state.ChuKoNuStateDB, cfg vm.Config) float64 {
	addresses, accountLen := p.accountConflictDetection(statedb) // 依赖队列的长度

	startTime := time.Now()
	p.tokenManagerWg.Add(p.tokenManagerNumCPU)
	for i := 0; i < p.tokenManagerNumCPU; i++ {
		go p.tokenManager(statedb)
	}

	p.schedulerWg.Add(p.schedulerNumCPU) //并发: Cpu核数=并发线程数
	for i := 0; i < p.schedulerNumCPU; i++ {
		p.schedulerCh[i] = make(tokenChan, chanSize/p.schedulerNumCPU+1)
		go p.scheduler(p.schedulerCh[i], i)
	}

	p.executorWg.Add(p.executorNumCPU)
	for i := 0; i < p.executorNumCPU; i++ {
		go p.executor(block, cfg)
	}

	for _, accInfo := range addresses {
		p.tokenManagerCh <- newToken(accInfo.address, accInfo, nil, nil, nil)
	}

	p.closeWg.Add(1)
	go p.close(accountLen)

	p.closeWg.Wait()
	p.tokenManagerWg.Wait()
	p.schedulerWg.Wait()
	p.executorWg.Wait()
	close(p.feeCh)
	allFee := new(big.Int).SetInt64(0)
	for fee := range p.feeCh {
		allFee.Add(allFee, fee)
	}
	statedb.AddBalance(block.Coinbase(), allFee)

	// Fail if Shanghai not enabled and len(withdrawals) is non-zero.
	withdrawals := block.Withdrawals()
	if len(withdrawals) > 0 && !p.config.IsShanghai(block.Time()) {
		fmt.Println("withdrawals before shanghai")
	}
	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	ckuFastAccumulateRewards(p.config, statedb, block.Header(), block.Uncles())

	statedb.IntermediateRoot(p.config.IsEIP158(block.Number()))

	return float64(block.Transactions().Len()) / time.Since(startTime).Seconds()
}

func (p *ChuKoNuFastProcessor) accountConflictDetection(statedb *state.ChuKoNuStateDB) (map[common.Address]*accountInfo, int) {
	accountQueue := make(map[common.Address]*accountInfo)

	txsLen := len(p.cknTxs)

	// 	地址对应的队列
	for _, tx := range p.cknTxs {
		tx.tx.AccessSum = len(*tx.tx.AccessPre)
		for addr, _ := range *tx.tx.AccessPre {
			if _, ok := accountQueue[addr]; !ok {
				accountQueue[addr] = newAccountInfo(addr, txsLen)
			}
			accountQueue[addr].accountSequence.append(tx)
		}
	}
	type txsByAddr struct {
		addr    common.Address
		accInfo *accountInfo
		num     int
	}
	var listTxsByAddr []txsByAddr
	for key, accQueue := range accountQueue {
		accQueue.accountSequence.setLen()
		listTxsByAddr = append(listTxsByAddr, txsByAddr{key, accQueue, accQueue.accountSequence.sequenceLen})
	}
	sort.Slice(listTxsByAddr, func(i, j int) bool {
		return listTxsByAddr[i].num > listTxsByAddr[j].num // 降序
	})

	var (
		slotConflictDetectionWg sync.WaitGroup
		accountInfoCh           = make(chan *accountInfo, chanSize)
	)
	slotConflictDetectionWg.Add(p.slotConflictDetectionNum)
	for i := 0; i < p.slotConflictDetectionNum; i++ {
		go p.slotConflictDetection(accountInfoCh, &slotConflictDetectionWg, statedb)
	}
	for _, val := range listTxsByAddr {
		accountInfoCh <- val.accInfo
	}
	close(accountInfoCh)
	slotConflictDetectionWg.Wait()

	return accountQueue, len(listTxsByAddr)
}

func (p *ChuKoNuFastProcessor) slotConflictDetection(accountInfoCh chan *accountInfo, wg *sync.WaitGroup, statedb *state.ChuKoNuStateDB) {
	defer wg.Done()
	for accInfo := range accountInfoCh {
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
						accInfo.slotSequence[slot] = newSequence(slot)
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

func (p *ChuKoNuFastProcessor) recycleToken(tok *token, statedb *state.ChuKoNuStateDB) {
	grantedTxIndexes := make(map[int]struct{}, 0) // 用于统计授予了哪些事务相关权限, 方便后续的令牌发放
	tok.account.finishedTxNum += 1                // 完成的事务 + 1, 用于停止线程

	if tok.stateTokenToAccountState != nil {
		tok.stateTokenToAccountState.UpdateAccountState(tok.account.accountState) // 更新账户的状态
	}

	// 查看是否已经执行完所有的事务
	if tok.account.finishedTxNum == tok.account.accountSequence.sequenceLen { // 所有的事务都完成了执行, 结束后续的权限授予
		statedb.UpdateByAccountObject(tok.account.accountState) // 将临时账户状态更新至世界状态
		//tok.account.mutex.Lock()
		ok := tok.account.atomicCounter.CompareAndSwap(true, false)
		if !ok {
			fmt.Printf("finishing rights granted failed")
		}
		//tok.account.mutex.Unlock()
		//fmt.Println(tok.tx.tx.Index)
		p.closeCh <- true
		return // 结束当前账户的所有交易
	}

	// 处理事务结束后账户序列的权限
	if tok.rwSet.IsWrite { // 账户是写操作，为账户序列继续授予权限
		accInfo := tok.account
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

	//if tok.account.address == common.HexToAddress("0xA68Dd8cB83097765263AdAD881Af6eeD479c4a33") && tok.tx.tx.Index == 2 {
	//	fmt.Println(tok.rwSet.Slots)
	//}
	// 处理事务结束后每个slot序列的权限
	//fmt.Println("len(*tok.rwSet.Slots):", len(*tok.rwSet.Slots))
	for slot, tokenSlotRW := range *tok.rwSet.Slots {
		if tokenSlotRW.IsWrite {
			slotSequence := tok.account.slotSequence[slot]
			slotSequence.isWrite = false // 允许继续授予权限

			for ; slotSequence.pendingExecutionIndex < slotSequence.sequenceLen; slotSequence.pendingExecutionIndex++ {
				tx := slotSequence.sequence[slotSequence.pendingExecutionIndex].tx
				addressRW := (*tx.AccessPre)[tok.address]
				slotRW := (*addressRW.Slots)[slot]

				tok.account.checkIntegrity[tx.Index] += 1
				grantedTxIndexes[tx.Index] = struct{}{} // 用于授予权限

				if slotRW.IsWrite { // 如果是写, 则不能再继续授予权限了
					slotSequence.isWrite = true
					slotSequence.pendingExecutionIndex += 1 // 退出前要把下标 + 1, 否则之后就无法发放令牌了
					break                                   // 接下来该队列不可能再授予权限了, 直接退出
				}
			}
		}
	}

	//if tok.account.address == common.HexToAddress("0xA68Dd8cB83097765263AdAD881Af6eeD479c4a33") {
	//	fmt.Println(grantedTxIndexes, tok.account.checkIntegrity[4], tok.account.needSlotToken[4])
	//}
	//fmt.Println(len(grantedTxIndexes))
	if len(grantedTxIndexes) > 0 { // 获取完了所有状态的事务，可以分发权限
		tok.account.readyTxs = tok.account.readyTxs[:0]
		for txIndex, _ := range grantedTxIndexes {
			if tok.account.checkIntegrity[txIndex] == tok.account.needSlotToken[txIndex] {
				tok.account.readyTxs = append(tok.account.readyTxs, txIndex)
			}
		}
		sort.Ints(tok.account.readyTxs) // 需要从 tx 下标小的开始授予权限, 防止写事务先授予，然后读事务没授予完就已经返回更改了状态
		for _, txIndex := range tok.account.readyTxs {
			rwSet := (*p.cknTxs[txIndex].tx.AccessPre)[tok.address]
			slots := make([]common.Hash, len(*rwSet.Slots))
			for key, _ := range *rwSet.Slots {
				slots = append(slots, key)
			}
			p.schedulerCh[txIndex%p.schedulerNumCPU] <- newToken(tok.account.address, tok.account, p.cknTxs[txIndex],
				state.NewStateTokenToTxDB(tok.address, tok.account.accountState, slots, tok.account.accountState.Deleted()),
				rwSet)
		}
	}

	ok := tok.account.atomicCounter.CompareAndSwap(true, false)

	if !ok {
		fmt.Printf("finishing rights granted failed")
	}
}

func (p *ChuKoNuFastProcessor) tokenManager(statedb *state.ChuKoNuStateDB) {
	defer p.tokenManagerWg.Done()
	for tok := range p.tokenManagerCh {
		//tok.account.mutex.Lock()
		ok := tok.account.atomicCounter.CompareAndSwap(false, true)
		if !ok { // 有线程正在处理该地址相关的token
			p.tokenManagerCh <- tok
			//fmt.Println(tok.address, tok.tx.tx.Index)
			//tok.account.mutex.Unlock()
			continue
		}

		if tok.tx != nil {
			p.recycleToken(tok, statedb)
		} else {
			for _, txIndex := range tok.account.readyTxs {
				rwSet := (*p.cknTxs[txIndex].tx.AccessPre)[tok.account.address]
				slots := make([]common.Hash, len(*rwSet.Slots))
				for key, _ := range *rwSet.Slots {
					slots = append(slots, key)
				}
				p.schedulerCh[txIndex%p.schedulerNumCPU] <- newToken(tok.account.address, tok.account, p.cknTxs[txIndex],
					state.NewStateTokenToTxDB(tok.account.address, tok.account.accountState, slots, tok.account.accountState.Deleted()),
					rwSet)
			}
			ok = tok.account.atomicCounter.CompareAndSwap(true, false) // 首次权限开始授予
			if !ok {
				fmt.Printf("firstly finishing rights granted failed")
			}
		}
		//tok.account.mutex.Unlock()
	}
}

func (p *ChuKoNuFastProcessor) scheduler(schedulerCh tokenChan, i int) {
	defer p.schedulerWg.Done()
	txTokens := make(map[common.Hash][]*token)
	for t := range schedulerCh {
		var tokens []*token
		if _, ok := txTokens[t.tx.tx.Hash()]; !ok { // 第一次接收到该交易的依赖
			tokens = make([]*token, 0)
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

func (p *ChuKoNuFastProcessor) executor(block *types.Block, cfg vm.Config) {
	defer p.executorWg.Done()
	for tokens := range p.executorCh {
		tx := (*tokens)[0].tx
		var (
			usedGas = new(uint64)
			header  = block.Header()
			gp      = new(GasPool).AddGas(block.GasLimit())
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

		result, err, fee := ApplyMessage(vmenv, msg, gp)
		if err != nil {
			fmt.Printf("could not apply tx %d [%v]: %w", tx.tx.Index, tx.tx.Hash().Hex(), err)
		}
		*usedGas += result.UsedGas

		p.feeCh <- new(big.Int).Set(fee)
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

func (p *ChuKoNuFastProcessor) close(accountLen int) {
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

func ckuFastAccumulateRewards(config *params.ChainConfig, state *state.ChuKoNuStateDB, header *types.Header, uncles []*types.Header) {
	// Ethash proof-of-work protocol constants.
	var (
		FrontierBlockReward       = big.NewInt(5e+18) // Block reward in wei for successfully mining a block
		ByzantiumBlockReward      = big.NewInt(3e+18) // Block reward in wei for successfully mining a block upward from Byzantium
		ConstantinopleBlockReward = big.NewInt(2e+18) // Block reward in wei for successfully mining a block upward from Constantinople

		big8  = big.NewInt(8)
		big32 = big.NewInt(32)
	)

	// Select the correct block reward based on chain progression
	blockReward := FrontierBlockReward
	if config.IsByzantium(header.Number) {
		blockReward = ByzantiumBlockReward
	}
	if config.IsConstantinople(header.Number) {
		blockReward = ConstantinopleBlockReward
	}
	// Accumulate the rewards for the miner and any included uncles
	reward := new(big.Int).Set(blockReward)
	r := new(big.Int)
	for _, uncle := range uncles {
		r.Add(uncle.Number, big8)
		r.Sub(r, header.Number)
		r.Mul(r, blockReward)
		r.Div(r, big8)
		state.AddBalance(uncle.Coinbase, r)

		r.Div(blockReward, big32)
		reward.Add(reward, r)
	}
	state.AddBalance(header.Coinbase, reward)
}
