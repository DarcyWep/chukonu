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
	"strconv"
	"sync"
	"time"
)

type DMVCCTxs []*DMVCCTx

// DMVCCTx bind tx state db for tx
type DMVCCTx struct {
	tx      *types.Transaction
	txdb    *state.ChuKoNuTxStateDB
	block   *types.Block
	rwSets  map[string]*types.AccessSlot
	rewards *map[common.Address]*big.Int
}

func NewDMVCCTx(tx *types.Transaction, txdb *state.ChuKoNuTxStateDB, block *types.Block, rewards *map[common.Address]*big.Int) *DMVCCTx {
	dtx := &DMVCCTx{
		tx:      tx,
		txdb:    txdb,
		block:   block,
		rwSets:  make(map[string]*types.AccessSlot),
		rewards: rewards,
	}
	for addr, addressRW := range *tx.AccessPre {
		dtx.rwSets[addr.Hex()] = &types.AccessSlot{IsRead: addressRW.IsRead, IsWrite: addressRW.IsWrite}
		for slot, slotRW := range *addressRW.Slots {
			key := addr.Hex() + "_" + slot.Hex()
			dtx.rwSets[key] = slotRW
		}
	}
	return dtx
}

type dmvccSequence struct {
	key                   string
	sequence              DMVCCTxs // 该地址等待执行的队列
	sequenceLen           int
	pendingExecutionIndex int  // 最新的等待执行的交易在pendingTxs中的下标
	isWrite               bool // 如果有交易获取了写权限, 则该序列停止权限授予
}

func newDmvccSequence(key string) *dmvccSequence {
	return &dmvccSequence{
		key:      key,
		sequence: make(DMVCCTxs, 0),
	}
}

func (s *dmvccSequence) append(tx *DMVCCTx) {
	s.sequence = append(s.sequence, tx)
}

func (s *dmvccSequence) setLen() {
	s.sequenceLen = len(s.sequence)
}
func (s *dmvccSequence) String() string {
	str := ""
	for _, tx := range s.sequence {
		str += strconv.Itoa(tx.tx.Index) + " "
	}
	return str
}

type stateInfo struct {
	address      common.Address // 传输的是哪个地址的令牌
	slot         []byte         // 为空则为地址
	key          string
	accountState *state.ChuKoNuStateObject
	storageState common.Hash
	mutex        sync.Mutex
	sequence     *dmvccSequence

	finishedTxNum int // 结束了多少个事务，该地址下所有的事务都执行完毕，则通知监控线程
	readyTxs      []int
}

func newStateInfo(addr common.Address, slot []byte, accountState *state.ChuKoNuStateObject, statedb *state.ChuKoNuStateDB) *stateInfo {
	key := addr.Hex()
	if slot != nil {
		key = key + "_" + common.BytesToHash(slot).Hex()
	}
	s := &stateInfo{
		address:       addr,
		slot:          common.CopyBytes(slot),
		key:           key,
		accountState:  accountState,
		sequence:      newDmvccSequence(key),
		finishedTxNum: 0,
		readyTxs:      make([]int, 0),
	}
	if slot != nil {
		s.storageState = statedb.GetOriginState(addr, common.BytesToHash(slot))
	}
	return s
}

type dmvccToken struct {
	address                  common.Address // 传输的是哪个地址的令牌
	slot                     []byte         // 为空则为地址
	key                      string
	state                    *stateInfo
	tx                       *DMVCCTx
	stateToTxDB              *state.StateTokenToTxDB
	stateTokenToAccountState *state.StateTokenToAccountState
	rwSet                    *types.AccessSlot
}

func newDmvccToken(state *stateInfo, tx *DMVCCTx, stateToTxDB *state.StateTokenToTxDB, rwSet *types.AccessSlot) *dmvccToken {
	return &dmvccToken{
		address:     state.address,
		slot:        common.CopyBytes(state.slot),
		state:       state,
		tx:          tx,
		stateToTxDB: stateToTxDB,
		rwSet:       rwSet,
	}
}

type (
	dmvccTokenChan    chan *dmvccToken
	dmvccExecutorChan chan *[]*dmvccToken
)

type DMVCCProcessorAll struct {
	config             *params.ChainConfig // Chain configuration options
	chainDb            ethdb.Database      // Canonical block chain
	cknTxs             DMVCCTxs
	tokenManagerNumCPU int
	schedulerNumCPU    int
	executorNumCPU     int
	tokenManagerWg     sync.WaitGroup
	tokenManagerCh     dmvccTokenChan
	schedulerWg        sync.WaitGroup
	schedulerCh        []dmvccTokenChan // 用多个chan实现
	executorWg         sync.WaitGroup
	executorCh         dmvccExecutorChan
	closeWg            sync.WaitGroup
	closeCh            closeChan
	feeCh              feeChan

	slotConflictDetectionNum int
}

func NewDMVCCProcessorAll(config *params.ChainConfig, chainDb ethdb.Database, txs DMVCCTxs, threadNum int) *DMVCCProcessorAll {
	schedulerNumCPU := 1
	cp := &DMVCCProcessorAll{
		config:                   config,
		chainDb:                  chainDb,
		cknTxs:                   txs,
		tokenManagerNumCPU:       1,
		schedulerNumCPU:          schedulerNumCPU,
		executorNumCPU:           threadNum - 2,
		tokenManagerCh:           make(dmvccTokenChan, chanSize),
		schedulerCh:              make([]dmvccTokenChan, schedulerNumCPU), // 用多个chan实现
		executorCh:               make(dmvccExecutorChan, chanSize),
		closeCh:                  make(closeChan, chanSize/2),
		feeCh:                    make(feeChan, 10),
		slotConflictDetectionNum: 32,
	}
	return cp
}

func (p *DMVCCProcessorAll) DMVCCProcessAll(statedb *state.ChuKoNuStateDB, cfg vm.Config) time.Duration {
	states, statesLen := p.conflictDetection(statedb) // 依赖队列的长度

	startTime := time.Now()
	p.tokenManagerWg.Add(p.tokenManagerNumCPU)
	for i := 0; i < p.tokenManagerNumCPU; i++ {
		go p.tokenManager(statedb)
	}

	p.schedulerWg.Add(p.schedulerNumCPU) //并发: Cpu核数=并发线程数
	for i := 0; i < p.schedulerNumCPU; i++ {
		p.schedulerCh[i] = make(dmvccTokenChan, chanSize/p.schedulerNumCPU+1)
		go p.scheduler(p.schedulerCh[i], i)
	}

	p.executorWg.Add(p.executorNumCPU)
	for i := 0; i < p.executorNumCPU; i++ {
		go p.executor(cfg)
	}

	for _, state := range states {
		state.sequence.setLen()
		p.tokenManagerCh <- newDmvccToken(state, nil, nil, nil)
	}

	p.closeWg.Add(1)
	go p.close(statesLen)

	p.closeWg.Wait()
	p.tokenManagerWg.Wait()
	p.schedulerWg.Wait()
	p.executorWg.Wait()
	close(p.feeCh)
	statedb.IntermediateRoot(true)

	return time.Since(startTime)
}

func (p *DMVCCProcessorAll) conflictDetection(statedb *state.ChuKoNuStateDB) (map[string]*stateInfo, int) {
	accounts := make(map[common.Address]*state.ChuKoNuStateObject)
	states := make(map[string]*stateInfo)

	// 	地址对应的队列
	for _, tx := range p.cknTxs {
		for addr, addressRW := range *tx.tx.AccessPre {
			tx.tx.AccessSum += 1 // 用于确定要获取多少个锁才能执行
			if _, ok := accounts[addr]; !ok {
				accounts[addr] = statedb.CreateAccountObject(addr)
				states[addr.Hex()] = newStateInfo(addr, nil, accounts[addr], statedb)
			}
			states[addr.Hex()].sequence.append(tx)

			for slot, _ := range *addressRW.Slots {
				tx.tx.AccessSum += 1
				key := addr.Hex() + "_" + slot.Hex()
				if _, ok := states[key]; !ok {
					states[key] = newStateInfo(addr, slot.Bytes(), accounts[addr], statedb)
				}
				states[key].sequence.append(tx)
			}
		}
	}
	return states, len(states)
}

func (p *DMVCCProcessorAll) recycleToken(tok *dmvccToken, statedb *state.ChuKoNuStateDB) {
	tok.state.finishedTxNum += 1 // 完成的事务 + 1, 用于停止线程

	if tok.stateTokenToAccountState != nil {
		tok.state.accountState.MutexForDMVCC.Lock()
		tok.stateTokenToAccountState.UpdateAccountState(tok.state.accountState) // 更新账户的状态
		tok.state.accountState.MutexForDMVCC.Unlock()
	}

	// 查看是否已经执行完所有的事务
	if tok.state.finishedTxNum == tok.state.sequence.sequenceLen { // 所有的事务都完成了执行, 结束后续的权限授予
		tok.state.accountState.MutexForDMVCC.Lock()
		statedb.UpdateByAccountObject(tok.state.accountState) // 将临时账户状态更新至世界状态
		tok.state.accountState.MutexForDMVCC.Unlock()
		p.closeCh <- true
		return // 结束当前账户的所有交易
	}

	// 处理事务结束后账户序列的权限
	if tok.rwSet.IsWrite { // 账户是写操作，为账户序列继续授予权限
		tok.state.sequence.isWrite = false // 允许继续授予权限
		for ; tok.state.sequence.pendingExecutionIndex < tok.state.sequence.sequenceLen; tok.state.sequence.pendingExecutionIndex++ {
			tx := tok.state.sequence.sequence[tok.state.sequence.pendingExecutionIndex]
			slots := make([]common.Hash, 0)
			if tok.state.slot != nil {
				slots = append(slots, common.BytesToHash(tok.state.slot))
			}
			p.schedulerCh[tx.tx.Index%p.schedulerNumCPU] <- newDmvccToken(tok.state, tx,
				state.NewStateTokenToTxDB(tok.address, tok.state.accountState, slots, tok.state.accountState.Deleted()),
				tx.rwSets[tok.state.key])

			if tx.rwSets[tok.state.key].IsWrite { // 如果是写, 则不能再继续授予权限了
				tok.state.sequence.isWrite = true
				tok.state.sequence.pendingExecutionIndex += 1 // 退出前要把下标 + 1, 否则之后就无法发放令牌了
				break                                         // 接下来该队列不可能再授予权限了, 直接退出
			}
		}
	}
}

func (p *DMVCCProcessorAll) tokenManager(statedb *state.ChuKoNuStateDB) {
	defer p.tokenManagerWg.Done()
	for tok := range p.tokenManagerCh {
		tok.state.mutex.Lock()
		//time.Sleep(150 * time.Nanosecond) // 模拟锁
		if tok.tx != nil {
			p.recycleToken(tok, statedb)
		} else {
			tok.state.sequence.isWrite = false // 允许继续授予权限
			for _, tx := range tok.state.sequence.sequence {
				slots := make([]common.Hash, 0)
				if tok.state.slot != nil {
					slots = append(slots, common.BytesToHash(tok.state.slot))
				}
				p.schedulerCh[tx.tx.Index%p.schedulerNumCPU] <- newDmvccToken(tok.state, tx,
					state.NewStateTokenToTxDB(tok.address, tok.state.accountState, slots, tok.state.accountState.Deleted()),
					tx.rwSets[tok.state.key])

				tok.state.sequence.pendingExecutionIndex += 1
				if tx.rwSets[tok.state.key].IsWrite { // 如果是写, 则不能再继续授予权限了
					tok.state.sequence.isWrite = true
					break // 接下来该队列不可能再授予权限了, 直接退出
				}
			}
		}
		tok.state.mutex.Unlock()
	}
}

func (p *DMVCCProcessorAll) scheduler(schedulerCh dmvccTokenChan, i int) {
	defer p.schedulerWg.Done()
	txTokens := make(map[common.Hash][]*dmvccToken)
	for t := range schedulerCh {
		var tokens []*dmvccToken
		if _, ok := txTokens[t.tx.tx.Hash()]; !ok { // 第一次接收到该交易的依赖
			tokens = make([]*dmvccToken, 0)
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

func (p *DMVCCProcessorAll) executor(cfg vm.Config) {
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
		mgval := new(big.Int).SetUint64(msg.GasLimit * 2)
		mgval = mgval.Mul(mgval, msg.GasPrice)
		balanceCheck := mgval
		if msg.GasFeeCap != nil {
			balanceCheck = new(big.Int).SetUint64(msg.GasLimit * 2)
			balanceCheck = balanceCheck.Mul(balanceCheck, msg.GasFeeCap)
		}
		balanceCheck.Add(balanceCheck, msg.Value)
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
		for _, tok := range *tokens {
			tok.stateTokenToAccountState = tx.txdb.Token(tok.address) // 带更新的状态
			p.tokenManagerCh <- tok
		}
	}
}

func (p *DMVCCProcessorAll) close(accountLen int) {
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
