package core

import (
	"chukonu/ethdb"
	"chukonu/setting"
	"sync"
	"time"

	"chukonu/core/state"
	"chukonu/core/types"
	"chukonu/core/vm"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"math/big"
)

type ChuKoNuProcessor struct {
	config  *params.ChainConfig // Chain configuration options
	chainDb ethdb.Database      // Canonical block chain
}

// NewChuKoNuProcessor initialises a new ChuKoNuProcessor.
func NewChuKoNuProcessor(config *params.ChainConfig, chainDb ethdb.Database) *ChuKoNuProcessor {
	return &ChuKoNuProcessor{
		config:  config,
		chainDb: chainDb,
	}
}

func (p *ChuKoNuProcessor) SerialSimulation(block *types.Block, statedb *state.ChuKoNuStateDB, cfg vm.Config) *[]*types.AccessAddressMap {
	var (
		receipts    types.Receipts
		usedGas     = new(uint64)
		header      = block.Header()
		blockHash   = block.Hash()
		blockNumber = block.Number()
		allLogs     []*types.Log
		allFee      = new(big.Int).SetInt64(0)
		gp          = new(GasPool).AddGas(block.GasLimit())
	)
	blockContext := NewEVMBlockContext(header, p.chainDb, nil)
	vmenv := vm.NewEVM(blockContext, vm.TxContext{}, statedb, p.config, cfg)

	txsAccessAddress := make([]*types.AccessAddressMap, 0)

	// Iterate over and process the individual transactions
	for i, tx := range block.Transactions() {
		msg, err := TransactionToMessage(tx, types.MakeSigner(p.config, header.Number), header.BaseFee)
		if err != nil {
			fmt.Println(fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err))
			return nil
		}
		tx.Index = i
		statedb.SetTxContext(tx.Hash(), i, true)
		receipt, fee, err, _ := applyTransactionChuKoNu(msg, p.config, gp, statedb, blockNumber, blockHash, tx, usedGas, vmenv)
		allFee.Add(allFee, fee)
		if err != nil {
			fmt.Println(fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err))
			return nil
		}
		receipts = append(receipts, receipt)
		allLogs = append(allLogs, receipt.Logs...)
		tx.AccessPre = statedb.AccessAddress()
		txsAccessAddress = append(txsAccessAddress, statedb.AccessAddress())
	}
	statedb.SetTxContext(common.Hash{}, -1, true)
	statedb.AddBalance(blockContext.Coinbase, allFee)
	// Fail if Shanghai not enabled and len(withdrawals) is non-zero.
	withdrawals := block.Withdrawals()
	if len(withdrawals) > 0 && !p.config.IsShanghai(block.Time()) {
		return &txsAccessAddress
	}
	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	accumulateRewardsChuKoNu(p.config, statedb, header, block.Uncles())

	statedb.IntermediateRoot(p.config.IsEIP158(header.Number))

	return &txsAccessAddress
}

func (p *ChuKoNuProcessor) ConcurrentSimulation(block *types.Block, statedb *state.ChuKoNuStateDB, cfg vm.Config) *[]*types.AccessAddressMap {
	var (
		header = block.Header()

		txsLen           = block.Transactions().Len()
		txsAccessAddress = make([]*types.AccessAddressMap, txsLen)
		wg               sync.WaitGroup
		accessAddrChan   = make(chan *accessAddressChStruct, 2048)
	)
	if txsLen == 0 {
		return &txsAccessAddress
	}
	blockContext := NewEVMBlockContext(header, p.chainDb, nil)
	// Iterate over and process the individual transactions
	wg.Add(txsLen)
	for i, tx := range block.Transactions() {
		go p.applyConcurrentSimulation(header, tx, i, statedb, &blockContext, cfg, accessAddrChan, &wg)
	}
	received := 0
	for aam := range accessAddrChan {
		txsAccessAddress[aam.index] = aam.aam
		received += 1
		if received == txsLen {
			close(accessAddrChan)
		}
	}
	wg.Wait()
	return &txsAccessAddress
}

func (p *ChuKoNuProcessor) applyConcurrentSimulation(header *types.Header, tx *types.Transaction, txIndex int, statedb *state.ChuKoNuStateDB, blockContext *vm.BlockContext, cfg vm.Config, accessAddrChan chan *accessAddressChStruct, wg *sync.WaitGroup) {
	defer wg.Done()
	var (
		//receipts    types.Receipts
		usedGas                        = new(uint64)
		blockHash                      = header.Hash()
		blockNumber                    = header.Number
		gp                             = new(GasPool).AddGas(header.GasLimit)
		lastSlot    []*state.TraceSlot = make([]*state.TraceSlot, 0)
		gasAdd      uint64             = 1
		result      *ExecutionResult   = nil
	)

	//statedb.CreateAccountObject()
	txdb, _ := state.NewChuKoNuTxStateDB(statedb)
	vmenv := vm.NewEVM(*blockContext, vm.TxContext{}, txdb, p.config, cfg)
	msg, _ := TransactionToMessage(tx, types.MakeSigner(p.config, header.Number), header.BaseFee)

	for i := 0; i < 10; i++ {
		txdbCopy := txdb.Copy()
		txdbCopy.SetTxContext(tx.Hash(), tx.Index, true)
		// 避免Nonce错误, Nonce是必须的
		txdbCopy.SetNonce(msg.From, msg.Nonce)

		// 避免Balance错误
		mgval := new(big.Int).SetUint64(msg.GasLimit * gasAdd)
		mgval = mgval.Mul(mgval, msg.GasPrice)
		balanceCheck := mgval
		if msg.GasFeeCap != nil {
			balanceCheck = new(big.Int).SetUint64(msg.GasLimit)
			balanceCheck = balanceCheck.Mul(balanceCheck, msg.GasFeeCap)
			balanceCheck.Add(balanceCheck, msg.Value)
		}
		txdbCopy.AddBalance(msg.From, balanceCheck)

		// 避免Transfer错误
		newData, ok := setting.IsERCTransfer(msg.Data)
		if ok {
			msg.Data = common.CopyBytes(newData)
		}
		if len(lastSlot) != 0 && result != nil && result.Err != nil {
			for _, tunedSlot := range lastSlot {
				txdbCopy.SetState(tunedSlot.Address, tunedSlot.Slot,
					common.HexToHash("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"))
			}
		}
		_, _, _, result = applyTransactionChuKoNuConcurrent(msg, gp, txdbCopy, blockNumber, blockHash, tx, usedGas, vmenv)
		//result = nil // 原生的模拟执行
		if result != nil && result.Err != nil {
			if result.Err.Error() == "out of gas" {
				gasAdd += 1
				if gasAdd > 3 {
					gasAdd = 3
				}
			} else if result.Err.Error() == "execution reverted" && len(txdbCopy.Slots) > 0 {
				lastSlot = append(lastSlot, txdbCopy.Slots[len(txdbCopy.Slots)-1])
			} else {
				result = nil
			}
		} else {
			result = nil
		}
		if result == nil || i == 9 {
			accessAddrChan <- newAccessAddressChStruct(txdbCopy.AccessAddress(), txIndex)
			break
		}
	}
}

// Process processes the state changes according to the Ethereum rules by running
// the transaction messages using the statedb and applying any rewards to both
// the processor (coinbase) and any included uncles.
//
// Process returns the receipts and logs accumulated during the process and
// returns the amount of gas that was used in the process. If any of the
// transactions failed to execute due to insufficient gas it will return an error.
func (p *ChuKoNuProcessor) Process(block *types.Block, statedb *state.ChuKoNuStateDB, cfg vm.Config) (*common.Hash, *[]*types.AccessAddressMap, types.Receipts, []*types.Log, uint64, error) {
	var (
		receipts    types.Receipts
		usedGas     = new(uint64)
		header      = block.Header()
		blockHash   = block.Hash()
		blockNumber = block.Number()
		allLogs     []*types.Log
		allFee      = new(big.Int).SetInt64(0)
		gp          = new(GasPool).AddGas(block.GasLimit())
	)
	startTime := time.Now()
	blockContext := NewEVMBlockContext(header, p.chainDb, nil)
	vmenv := vm.NewEVM(blockContext, vm.TxContext{}, statedb, p.config, cfg)

	txsAccessAddress := make([]*types.AccessAddressMap, 0)

	// Iterate over and process the individual transactions
	//fmt.Println(len(block.Transactions()))
	for i, tx := range block.Transactions() {
		msg, err := TransactionToMessage(tx, types.MakeSigner(p.config, header.Number), header.BaseFee)
		if err != nil {
			return nil, nil, nil, nil, 0, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
		}
		tx.Index = i
		statedb.SetTxContext(tx.Hash(), i, false)
		receipt, fee, err, _ := applyTransactionChuKoNu(msg, p.config, gp, statedb, blockNumber, blockHash, tx, usedGas, vmenv)
		allFee.Add(allFee, fee)
		if err != nil {
			fmt.Println("applyTransactionChuKoNu error", tx.Hash(), err)
			return nil, nil, nil, nil, 0, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
		}
		receipts = append(receipts, receipt)
		allLogs = append(allLogs, receipt.Logs...)
		tx.AccessPre = statedb.AccessAddress()
		txsAccessAddress = append(txsAccessAddress, statedb.AccessAddress())
	}
	statedb.SetTxContext(common.Hash{}, -1, false)
	statedb.AddBalance(blockContext.Coinbase, allFee)
	//statedb.SetTxContext(common.Hash{}, 0) // 避免因叔父区块添加其矿工奖励而导致最后一个交易的读写集变化
	// Fail if Shanghai not enabled and len(withdrawals) is non-zero.
	withdrawals := block.Withdrawals()
	if len(withdrawals) > 0 && !p.config.IsShanghai(block.Time()) {
		return nil, nil, nil, nil, 0, fmt.Errorf("withdrawals before shanghai")
	}
	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	accumulateRewardsChuKoNu(p.config, statedb, header, block.Uncles())

	root := statedb.IntermediateRoot(p.config.IsEIP158(header.Number))

	fmt.Println(root, "ChuKoNuSerial", float64(block.Transactions().Len())/time.Since(startTime).Seconds())
	return &root, &txsAccessAddress, receipts, allLogs, *usedGas, nil
}

func (p *ChuKoNuProcessor) SerialProcess(block *types.Block, statedb *state.ChuKoNuStateDB, cfg vm.Config) (*common.Hash, *[]*types.AccessAddressMap, types.Receipts, []*types.Log, uint64, error) {
	var (
		receipts    types.Receipts
		usedGas     = new(uint64)
		header      = block.Header()
		blockHash   = block.Hash()
		blockNumber = block.Number()
		allLogs     []*types.Log
		allFee      = new(big.Int).SetInt64(0)
		gp          = new(GasPool).AddGas(block.GasLimit())
	)
	startTime := time.Now()
	blockContext := NewEVMBlockContext(header, p.chainDb, nil)
	vmenv := vm.NewEVM(blockContext, vm.TxContext{}, statedb, p.config, cfg)

	txsAccessAddress := make([]*types.AccessAddressMap, 0)

	// Iterate over and process the individual transactions
	for i, tx := range block.Transactions() {
		msg, err := TransactionToMessage(tx, types.MakeSigner(p.config, header.Number), header.BaseFee)
		if err != nil {
			return nil, nil, nil, nil, 0, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
		}
		tx.Index = i
		statedb.SetTxContext(tx.Hash(), i, false)
		receipt, fee, err, _ := applyTransactionChuKoNu(msg, p.config, gp, statedb, blockNumber, blockHash, tx, usedGas, vmenv)
		allFee.Add(allFee, fee)
		if err != nil {
			fmt.Println("applyTransactionChuKoNu error", tx.Hash(), err)
			return nil, nil, nil, nil, 0, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
		}
		receipts = append(receipts, receipt)
		allLogs = append(allLogs, receipt.Logs...)
		tx.AccessPre = statedb.AccessAddress()
		txsAccessAddress = append(txsAccessAddress, statedb.AccessAddress())
	}
	statedb.SetTxContext(common.Hash{}, -1, false)
	statedb.AddBalance(blockContext.Coinbase, allFee)
	// Fail if Shanghai not enabled and len(withdrawals) is non-zero.
	withdrawals := block.Withdrawals()
	if len(withdrawals) > 0 && !p.config.IsShanghai(block.Time()) {
		return nil, nil, nil, nil, 0, fmt.Errorf("withdrawals before shanghai")
	}
	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	accumulateRewardsChuKoNu(p.config, statedb, header, block.Uncles())

	root := statedb.IntermediateRoot(p.config.IsEIP158(header.Number))

	fmt.Println(root, "ChuKoNuSerial", float64(block.Transactions().Len())/time.Since(startTime).Seconds())
	return &root, &txsAccessAddress, receipts, allLogs, *usedGas, nil
}

func applyTransactionChuKoNu(msg *Message, config *params.ChainConfig, gp *GasPool, statedb *state.ChuKoNuStateDB, blockNumber *big.Int, blockHash common.Hash, tx *types.Transaction, usedGas *uint64, evm *vm.EVM) (*types.Receipt, *big.Int, error, *ExecutionResult) {
	// Create a new context to be used in the EVM environment.
	txContext := NewEVMTxContext(msg)
	evm.Reset(txContext, statedb)

	// Apply the transaction to the current state (included in the env).
	result, err, fee := ApplyMessage(evm, msg, gp)
	if err != nil {
		return nil, fee, err, result
	}

	// Update the state with pending changes.
	var root []byte
	if config.IsByzantium(blockNumber) {
		statedb.Finalise(true)
	} else {
		root = statedb.IntermediateRoot(config.IsEIP158(blockNumber)).Bytes()
	}
	*usedGas += result.UsedGas

	// Create a new receipt for the transaction, storing the intermediate root and gas used
	// by the tx.
	receipt := &types.Receipt{Type: tx.Type(), PostState: root, CumulativeGasUsed: *usedGas}
	if result.Failed() {
		receipt.Status = types.ReceiptStatusFailed
	} else {
		receipt.Status = types.ReceiptStatusSuccessful
	}
	receipt.TxHash = tx.Hash()
	receipt.GasUsed = result.UsedGas

	// If the transaction created a contract, store the creation address in the receipt.
	if msg.To == nil {
		receipt.ContractAddress = crypto.CreateAddress(evm.TxContext.Origin, tx.Nonce())
	}

	// Set the receipt logs and create the bloom filter.
	receipt.Logs = statedb.GetLogs(tx.Hash(), blockNumber.Uint64(), blockHash)
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	receipt.BlockHash = blockHash
	receipt.BlockNumber = blockNumber
	receipt.TransactionIndex = uint(statedb.TxIndex())
	return receipt, fee, err, result
}

// AccumulateRewards credits the coinbase of the given block with the mining
// reward. The total reward consists of the static block reward and rewards for
// included uncles. The coinbase of each uncle block is also rewarded.
func accumulateRewardsChuKoNu(config *params.ChainConfig, state *state.ChuKoNuStateDB, header *types.Header, uncles []*types.Header) {
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

func applyTransactionChuKoNuConcurrent(msg *Message, gp *GasPool, statedb *state.ChuKoNuTxStateDB, blockNumber *big.Int, blockHash common.Hash, tx *types.Transaction, usedGas *uint64, evm *vm.EVM) (*types.Receipt, *big.Int, error, *ExecutionResult) {
	// Create a new context to be used in the EVM environment.
	txContext := NewEVMTxContext(msg)
	evm.Reset(txContext, statedb)

	// Apply the transaction to the current state (included in the env).
	result, err, fee := ApplyMessage(evm, msg, gp)
	if err != nil {
		return nil, fee, err, result
	}

	// Update the state with pending changes.
	var root []byte
	//if config.IsByzantium(blockNumber) {
	//	statedb.Finalise(true)
	//} else {
	//	root = statedb.IntermediateRoot(concfig.IsEIP158(blockNumber)).Bytes()
	//}
	*usedGas += result.UsedGas

	// Create a new receipt for the transaction, storing the intermediate root and gas used
	// by the tx.
	receipt := &types.Receipt{Type: tx.Type(), PostState: root, CumulativeGasUsed: *usedGas}
	if result.Failed() {
		receipt.Status = types.ReceiptStatusFailed
	} else {
		receipt.Status = types.ReceiptStatusSuccessful
	}
	receipt.TxHash = tx.Hash()
	receipt.GasUsed = result.UsedGas

	// If the transaction created a contract, store the creation address in the receipt.
	if msg.To == nil {
		receipt.ContractAddress = crypto.CreateAddress(evm.TxContext.Origin, tx.Nonce())
	}

	// Set the receipt logs and create the bloom filter.
	receipt.Logs = statedb.GetLogs(tx.Hash(), blockNumber.Uint64(), blockHash)
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	receipt.BlockHash = blockHash
	receipt.BlockNumber = blockNumber
	receipt.TransactionIndex = uint(statedb.TxIndex())
	return receipt, fee, err, result
}
