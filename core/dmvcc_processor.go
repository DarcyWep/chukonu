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
	"sync"
	"time"
)

type sequenceAndLockManager struct {
	key              string
	sequence         chuKoNuTxs // 该地址等待执行的队列
	sequenceLen      int
	requireLockIndex int  // 询问锁的事务下标
	isWrite          bool // 如果有交易获取了写权限, 则该序列停止授予锁
}

func newSequenceAndLockManager(key string) *sequenceAndLockManager {
	return &sequenceAndLockManager{
		key:              key,
		sequence:         make(chuKoNuTxs, 0),
		requireLockIndex: -1,
	}
}

func (manager *sequenceAndLockManager) append(tx *chuKoNuTx) {
	manager.sequence = append(manager.sequence, tx)
}
func (manager *sequenceAndLockManager) setLen() {
	manager.sequenceLen = len(manager.sequence)
}

func (manager *sequenceAndLockManager) lock(tx *chuKoNuTx, isWrite bool) bool {
	if manager.requireLockIndex == tx.tx.Index { // 如果和上次询问的一致，则可以授予锁
		manager.isWrite = isWrite
		return true
	} else if !manager.isWrite { // 之前授予的不是写锁， 则可以继续授予锁
		manager.isWrite = isWrite
		manager.requireLockIndex = tx.tx.Index
		return true
	} else if !isWrite && manager.requireLockIndex > tx.tx.Index {
		return true
	} else { // 有事务正在写
		return false
	}
}

func (manager *sequenceAndLockManager) release(tx *chuKoNuTx, isWrite bool) {
	if isWrite {
		manager.isWrite = false
	}
}

type dmvccAccount struct {
	address      common.Address // 传输的是哪个地址的令牌
	accountState *state.ChuKoNuStateObject
	slots        map[common.Hash]struct{}
}

func newDMVCCAccount(addr common.Address) *dmvccAccount {
	ai := &dmvccAccount{
		address: addr,
		slots:   make(map[common.Hash]struct{}),
	}
	return ai
}

func (d *dmvccAccount) appendSlot(hash common.Hash) {
	d.slots[hash] = struct{}{}
}

type DMVCCProcessor struct {
	config         *params.ChainConfig // Chain configuration options
	chainDb        ethdb.Database      // Canonical block chain
	block          *types.Block
	cknTxs         chuKoNuTxs
	executorNumCPU int
	executorCh     chan *chuKoNuTx

	locks    map[string]*sequenceAndLockManager
	accounts map[common.Address]*dmvccAccount
	feeCh    feeChan
}

func NewDMVCCProcessor(config *params.ChainConfig, chainDb ethdb.Database, block *types.Block, statedb *state.ChuKoNuStateDB, thread int) *DMVCCProcessor {
	cp := &DMVCCProcessor{
		config:         config,
		chainDb:        chainDb,
		block:          block,
		cknTxs:         make(chuKoNuTxs, block.Transactions().Len()),
		executorNumCPU: thread,
		feeCh:          make(feeChan, block.Transactions().Len()+10),

		locks:    make(map[string]*sequenceAndLockManager),
		accounts: make(map[common.Address]*dmvccAccount),
	}
	for i, tx := range block.Transactions() {
		tx.Index = i
		cp.cknTxs[i] = newChuKoNuTx(tx, state.NewChuKoNuTxStateDB(statedb))
	}
	return cp
}

func (p *DMVCCProcessor) DMVCC(block *types.Block, statedb *state.ChuKoNuStateDB, cfg vm.Config) float64 {
	p.conflictDetection(statedb) // 依赖队列的长度

	active, inActive, allTxs := make(chuKoNuTxs, 0), make(chuKoNuTxs, len(p.cknTxs)), make(chuKoNuTxs, len(p.cknTxs))
	inActive = p.cknTxs

	startTime := time.Now()
	for {
		allTxs = allTxs[:0]
		allTxs = inActive
		active = active[:0]
		inActive = inActive[:0]
		for _, tx := range allTxs {
			addrMapSlots := make(map[common.Address][]common.Hash)
			readyLockNum := 0
			for addr, addressRW := range *tx.tx.AccessPre {
				slots := make([]common.Hash, 0)

				//if tx.tx.Hash() == common.HexToHash("0xc0fdaabf57278a94d074d292509efe69d7d91d13bac5fabb7d5308315fd9b29c") {
				//	fmt.Println(addr.Hex(), p.locks[addr.Hex()].lock(tx, addressRW.IsWrite), p.locks[addr.Hex()].requireLockIndex)
				//	fmt.Println(p.locks[addr.Hex()].key, p.locks[addr.Hex()].requireLockIndex, p.locks[addr.Hex()].isWrite)
				//}

				if p.locks[addr.Hex()].lock(tx, addressRW.IsWrite) {
					readyLockNum += 1
				}

				for slot, slotRW := range *addressRW.Slots {
					key := addr.Hex() + "_" + slot.Hex()
					slots = append(slots, slot)
					//if tx.tx.Hash() == common.HexToHash("0xc0fdaabf57278a94d074d292509efe69d7d91d13bac5fabb7d5308315fd9b29c") {
					//	fmt.Println(key, p.locks[key].lock(tx, slotRW.IsWrite))
					//	fmt.Println(p.locks[key].key, p.locks[key].requireLockIndex, p.locks[key].isWrite)
					//}

					if p.locks[key].lock(tx, slotRW.IsWrite) {
						readyLockNum += 1
					}
				}
				addrMapSlots[addr] = slots
			}

			// 查看事务是否获取了所有的锁
			if readyLockNum == tx.tx.AccessSum {
				active = append(active, tx) // 事务可以执行
				for addr, _ := range *tx.tx.AccessPre {
					//if tx.tx.Hash() == common.HexToHash("0xc0fdaabf57278a94d074d292509efe69d7d91d13bac5fabb7d5308315fd9b29c") {
					//	fmt.Println(tx.tx.Hash(), addr, addrMapSlots[addr])
					//}
					stateToTxDB := state.NewStateTokenToTxDB(addr, p.accounts[addr].accountState, addrMapSlots[addr], p.accounts[addr].accountState.Deleted())
					stateToTxDB.UpdateTxState(tx.txdb)
				}

			} else {
				inActive = append(inActive, tx) // 事务还不能执行
			}
			//fmt.Println()
			//if tx.tx.Hash() == common.HexToHash("0xc0fdaabf57278a94d074d292509efe69d7d91d13bac5fabb7d5308315fd9b29c") {
			//	fmt.Println(readyLockNum, tx.tx.AccessSum)
			//}

			//return 0
		}

		p.executorCh = make(chan *chuKoNuTx, chanSize)
		var executorWg sync.WaitGroup
		executorWg.Add(p.executorNumCPU)
		for i := 0; i < p.executorNumCPU; i++ {
			go p.executor(&executorWg, cfg)
		}
		for _, tx := range active {
			p.executorCh <- tx
		}
		close(p.executorCh)
		executorWg.Wait()

		for _, tx := range active {
			for addr, addressRW := range *tx.tx.AccessPre {
				stateTokenToAccountState := tx.txdb.Token(addr)
				if stateTokenToAccountState != nil { // 只读状态无需更新
					stateTokenToAccountState.UpdateAccountState(p.accounts[addr].accountState) // 更新账户的状态
				}

				// 释放锁
				p.locks[addr.Hex()].release(tx, addressRW.IsWrite)
				for slot, slotRW := range *addressRW.Slots {
					key := addr.Hex() + "_" + slot.Hex()
					p.locks[key].release(tx, slotRW.IsWrite)
				}
			}
		}
		//for _, tx := range inActive {
		//	fmt.Printf("%d ", tx.tx.Index)
		//}
		//fmt.Println()
		if len(inActive) == 0 {
			break
		}
	}

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

func (p *DMVCCProcessor) conflictDetection(statedb *state.ChuKoNuStateDB) {
	// 	地址对应的队列
	for _, tx := range p.cknTxs {
		for addr, addressRW := range *tx.tx.AccessPre {
			tx.tx.AccessSum += 1 // 用于确定要获取多少个锁才能执行
			if _, ok := p.accounts[addr]; !ok {
				p.accounts[addr] = newDMVCCAccount(addr)
				p.locks[addr.Hex()] = newSequenceAndLockManager(addr.Hex())
			}
			p.locks[addr.Hex()].append(tx)

			for slot, _ := range *addressRW.Slots {
				tx.tx.AccessSum += 1
				key := addr.Hex() + "_" + slot.Hex()
				if _, ok := p.locks[key]; !ok {
					p.locks[key] = newSequenceAndLockManager(key)
				}
				p.locks[key].append(tx) // 加入锁
				p.accounts[addr].appendSlot(slot)
			}
		}
	}

	for addr, accInfo := range p.accounts {
		// 获取状态
		accInfo.accountState = statedb.CreateAccountObject(addr)
		for slot, _ := range accInfo.slots {
			accInfo.accountState.SetOriginState(slot, statedb.GetOriginState(addr, slot))
		}
	}
}

func (p *DMVCCProcessor) executor(wg *sync.WaitGroup, cfg vm.Config) {
	defer wg.Done()
	for tx := range p.executorCh {
		var (
			usedGas = new(uint64)
			header  = p.block.Header()
			gp      = new(GasPool).AddGas(p.block.GasLimit())
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
	}
}
