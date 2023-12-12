// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package core

import (
	"chukonu/ethdb"
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

// StateProcessor is a basic Processor, which takes care of transitioning
// state from one point to another.
//
// StateProcessor implements Processor.
type StateProcessor struct {
	config  *params.ChainConfig // Chain configuration options
	chainDb ethdb.Database      // Canonical block chain
}

// NewStateProcessor initialises a new StateProcessor.
func NewStateProcessor(config *params.ChainConfig, chainDb ethdb.Database) *StateProcessor {
	return &StateProcessor{
		config:  config,
		chainDb: chainDb,
	}
}

func (p *StateProcessor) SerialSimulation(block *types.Block, statedb *state.StateDB, cfg vm.Config) (*common.Hash, *[]*types.AccessAddressMap, types.Receipts, []*types.Log, uint64, error) {
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
			return nil, nil, nil, nil, 0, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
		}
		tx.Index = i
		statedb.SetTxContext(tx.Hash(), i, true)
		receipt, fee, err, _ := applyTransaction(msg, p.config, gp, statedb, blockNumber, blockHash, tx, usedGas, vmenv)
		allFee.Add(allFee, fee)
		if err != nil {
			fmt.Println("applyTransaction error", tx.Hash(), err)
			return nil, nil, nil, nil, 0, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
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
		return nil, nil, nil, nil, 0, fmt.Errorf("withdrawals before shanghai")
	}
	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	accumulateRewards(p.config, statedb, header, block.Uncles())

	root := statedb.IntermediateRoot(p.config.IsEIP158(header.Number))

	return &root, &txsAccessAddress, receipts, allLogs, *usedGas, nil
}

// Process processes the state changes according to the Ethereum rules by running
// the transaction messages using the statedb and applying any rewards to both
// the processor (coinbase) and any included uncles.
//
// Process returns the receipts and logs accumulated during the process and
// returns the amount of gas that was used in the process. If any of the
// transactions failed to execute due to insufficient gas it will return an error.
func (p *StateProcessor) Process(block *types.Block, statedb *state.StateDB, cfg vm.Config) (*common.Hash, *[]*types.AccessAddressMap, types.Receipts, []*types.Log, uint64, error) {
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
		receipt, fee, err, _ := applyTransaction(msg, p.config, gp, statedb, blockNumber, blockHash, tx, usedGas, vmenv)
		allFee.Add(allFee, fee)
		if err != nil {
			fmt.Println("applyTransaction error", tx.Hash(), err)
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
	accumulateRewards(p.config, statedb, header, block.Uncles())

	root := statedb.IntermediateRoot(p.config.IsEIP158(header.Number))

	fmt.Println(root, "Serial", float64(block.Transactions().Len())/time.Since(startTime).Seconds())
	return &root, &txsAccessAddress, receipts, allLogs, *usedGas, nil
}

func (p *StateProcessor) SerialProcess(block *types.Block, statedb *state.StateDB, cfg vm.Config) (*common.Hash, *[]*types.AccessAddressMap, types.Receipts, []*types.Log, uint64, error) {
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
		receipt, fee, err, _ := applyTransaction(msg, p.config, gp, statedb, blockNumber, blockHash, tx, usedGas, vmenv)
		allFee.Add(allFee, fee)
		if err != nil {
			fmt.Println("applyTransaction error", tx.Hash(), err)
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
	accumulateRewards(p.config, statedb, header, block.Uncles())

	root := statedb.IntermediateRoot(p.config.IsEIP158(header.Number))

	fmt.Println(root, "Serial", float64(block.Transactions().Len())/time.Since(startTime).Seconds())
	return &root, &txsAccessAddress, receipts, allLogs, *usedGas, nil
}

func applyTransaction(msg *Message, config *params.ChainConfig, gp *GasPool, statedb *state.StateDB, blockNumber *big.Int, blockHash common.Hash, tx *types.Transaction, usedGas *uint64, evm *vm.EVM) (*types.Receipt, *big.Int, error, *ExecutionResult) {
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
func accumulateRewards(config *params.ChainConfig, state *state.StateDB, header *types.Header, uncles []*types.Header) {
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
