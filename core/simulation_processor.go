package core

import (
	"chukonu/ethdb"
	"chukonu/setting"
	"sync"

	"chukonu/core/state"
	"chukonu/core/types"
	"chukonu/core/vm"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"math/big"
)

type SimulationProcessor struct {
	config  *params.ChainConfig // Chain configuration options
	chainDb ethdb.Database      // Canonical block chain
}

func NewSimulationProcessor(config *params.ChainConfig, chainDb ethdb.Database) *SimulationProcessor {
	return &SimulationProcessor{
		config:  config,
		chainDb: chainDb,
	}
}

func (p *SimulationProcessor) SerialSimulation(block *types.Block, stmStateDB *state.StmStateDB, cfg vm.Config) (*common.Hash, types.Receipts, []*types.Log, uint64, error) {
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
	//fmt.Println(stmStateDB.GetBalance(common.HexToAddress("0xf85219B9bB810894020f2c19eA2952f3aaBf916e,")))

	// Iterate over and process the individual transactions
	for i, tx := range block.Transactions() {
		stmTxDB := state.NewStmTransaction(tx, i, stmStateDB, true)
		//stmTxDB.GetAccountState()
		vmenv := vm.NewEVM(blockContext, vm.TxContext{}, stmTxDB, p.config, cfg)
		msg, err := TransactionToMessage(tx, types.MakeSigner(p.config, header.Number), header.BaseFee)
		if err != nil {
			fmt.Println(err)
			return nil, nil, nil, 0, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
		}
		receipt, fee, err, _ := applySimulationProcessor(msg, gp, stmTxDB, blockNumber, blockHash, tx, usedGas, vmenv)
		allFee.Add(allFee, fee)
		if err != nil {
			fmt.Println(err)
			return nil, nil, nil, 0, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
		}
		stmTxDB.Validation(true)
		receipts = append(receipts, receipt)
		allLogs = append(allLogs, receipt.Logs...)
		tx.AccessSim = stmTxDB.AccessAddress()
	}
	stmStateDB.AddBalance(blockContext.Coinbase, allFee)
	// Fail if Shanghai not enabled and len(withdrawals) is non-zero.
	withdrawals := block.Withdrawals()
	if len(withdrawals) > 0 && !p.config.IsShanghai(block.Time()) {
		return nil, nil, nil, 0, fmt.Errorf("withdrawals before shanghai")
	}
	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	simulationAccumulateRewards(p.config, stmStateDB, header, block.Uncles())

	root := stmStateDB.IntermediateRoot(p.config.IsEIP158(header.Number), -1)

	return &root, receipts, allLogs, *usedGas, nil
}

func (p *SimulationProcessor) ConcurrentSimulation(block *types.Block, stmStateDB *state.StmStateDB, cfg vm.Config, tune bool) *[]*types.AccessAddressMap {
	var (
		header           = block.Header()
		txsLen           = block.Transactions().Len()
		txsAccessAddress = make([]*types.AccessAddressMap, txsLen)
		wg               sync.WaitGroup
		accessAddrChan   = make(chan *accessAddressChStruct, 2048)
	)
	if txsLen == 0 {
		return &txsAccessAddress
	}
	blockContext := NewEVMBlockContext(header, p.chainDb, nil)

	wg.Add(txsLen)
	for i, tx := range block.Transactions() {
		go p.applyConcurrent(header, tx, i, stmStateDB, &blockContext, cfg, accessAddrChan, &wg, tune)
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

func (p *SimulationProcessor) applyConcurrent(header *types.Header, tx *types.Transaction, i int, stmStateDB *state.StmStateDB, blockContext *vm.BlockContext, cfg vm.Config, accessAddrChan chan *accessAddressChStruct, wg *sync.WaitGroup, tune bool) {
	defer wg.Done()
	var (
		receipts    types.Receipts
		usedGas     = new(uint64)
		blockHash   = header.Hash()
		blockNumber = header.Number
		gp          = new(GasPool).AddGas(header.GasLimit)
	)

	msg, _ := TransactionToMessage(tx, types.MakeSigner(p.config, header.Number), header.BaseFee)

	var (
		ok           bool = false
		receipt      *types.Receipt
		result       *ExecutionResult
		tuneTransfer *state.SlotList = nil
		stmTxDB      *state.StmTransaction
		vmenv        *vm.EVM
	)
	var newData []byte
	for {
		stmTxDB = state.NewStmTransaction(tx, i, stmStateDB, true)
		vmenv = vm.NewEVM(*blockContext, vm.TxContext{}, stmTxDB, p.config, cfg)

		if tune {
			// 避免Nonce错误
			stmTxDB.SetNonce(msg.From, msg.Nonce)

			// 避免Balance错误
			mgval := new(big.Int).SetUint64(msg.GasLimit)
			mgval = mgval.Mul(mgval, msg.GasPrice)
			balanceCheck := mgval
			if msg.GasFeeCap != nil {
				balanceCheck = new(big.Int).SetUint64(msg.GasLimit)
				balanceCheck = balanceCheck.Mul(balanceCheck, msg.GasFeeCap)
				balanceCheck.Add(balanceCheck, msg.Value)
			}
			stmTxDB.AddBalance(msg.From, balanceCheck)

			// 避免Transfer错误
			newData, ok = setting.IsERCTransfer(msg.Data)
			if tuneTransfer != nil {
				if ok {
					msg.Data = common.CopyBytes(newData)
				}
				stmTxDB.SetState(tuneTransfer.Addr, tuneTransfer.Slot, common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000009999"))
			}

		}

		receipt, _, _, result = applySimulationProcessor(msg, gp, stmTxDB, blockNumber, blockHash, tx, usedGas, vmenv)
		if !tune || result == nil || !ok {
			break
		} else {
			fmt.Println(tx.Hash())
			len_ := len(stmTxDB.Slots)
			if len_ > 0 {
				tuneTransfer = stmTxDB.Slots[len_-1]
			}
		}
	}

	receipts = append(receipts, receipt)

	accessAddrChan <- newAccessAddressChStruct(stmTxDB.AccessAddress(), i)
}

func applySimulationProcessor(msg *Message, gp *GasPool, statedb *state.StmTransaction, blockNumber *big.Int, blockHash common.Hash, tx *types.Transaction, usedGas *uint64, evm *vm.EVM) (*types.Receipt, *big.Int, error, *ExecutionResult) {
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
	*usedGas += result.UsedGas

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

func simulationAccumulateRewards(config *params.ChainConfig, state *state.StmStateDB, header *types.Header, uncles []*types.Header) {
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
