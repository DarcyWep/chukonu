package state

import (
	"bytes"
	"chukonu/core/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"math/big"
)

type SStorage map[common.Hash]*SSlot

type stmTxStateObject struct {
	txIndex       int
	txIncarnation int

	address  common.Address
	addrHash common.Hash // hash of ethereum address of the account
	data     types.StateAccount
	//data    *SStateAccount
	txdb    *stmTxStateDB
	statedb *StmStateDB

	// Write caches.
	//trie Trie // storage trie, which becomes non-nil on first access
	code Code // contract bytecode, which gets set when code is loaded

	originStorage Storage // Storage cache of original entries to dedup rewrites, reset for every transaction
	dirtyStorage  Storage // Storage entries that have been modified in the current transaction execution

	// Cache flags.
	// When an object is marked suicided it will be deleted from the trie
	// during the "update" phase of the state transition.
	dirtyCode bool // true if the code was updated
	suicided  bool
	deleted   bool
}

// empty returns whether the account is considered empty.
func (s *stmTxStateObject) empty() bool {
	return s.data.Nonce == 0 && s.data.Balance.Sign() == 0 && bytes.Equal(s.data.CodeHash, types.EmptyCodeHash.Bytes())
}

// newStmTxStateObject, SStateAccount 记录了其数据所读取的版本
func newStmTxStateObject(txdb *stmTxStateDB, statedb *StmStateDB, address common.Address, data types.StateAccount, txIndex, txIncarnation int) *stmTxStateObject {
	if data.Balance == nil {
		data.Balance = new(big.Int)
	}
	if data.CodeHash == nil {
		data.CodeHash = types.EmptyCodeHash.Bytes()
	}
	if data.Root == (common.Hash{}) {
		data.Root = types.EmptyRootHash
	}
	return &stmTxStateObject{
		txIndex:       txIndex,
		txIncarnation: txIncarnation,
		txdb:          txdb,
		statedb:       statedb,
		address:       address,
		addrHash:      crypto.Keccak256Hash(address[:]),
		data:          data,
		originStorage: make(Storage),
		dirtyStorage:  make(Storage),
	}
}

func (s *stmTxStateObject) markSuicided() {
	s.suicided = true
}

func (s *stmTxStateObject) touch() {
	s.txdb.journal.append(stmTouchChange{
		account: &s.address,
	})
	if s.address == ripemd {
		// Explicitly put it in the dirty-cache, which is otherwise generated from
		// flattened journals.
		s.txdb.journal.dirty(s.address)
	}
}

// GetState retrieves a value from the account storage trie.
func (s *stmTxStateObject) GetState(key common.Hash) common.Hash {
	// If we have a dirty value for this state entry, return it
	if value, dirty := s.dirtyStorage[key]; dirty {
		return value
	}
	// 如果本交易没有写入，查看本交易是否获取过
	if value, origin := s.originStorage[key]; origin {
		return value
	}
	return common.Hash{}
}

// GetCommittedState retrieves a value from the committed account storage trie.
func (s *stmTxStateObject) GetCommittedState(key common.Hash) common.Hash {
	// 如果本交易没有写入，查看本交易是否获取过
	if value, origin := s.originStorage[key]; origin {
		return value
	}
	return common.Hash{}
}

// SetState updates a value in account storage.
func (s *stmTxStateObject) SetState(key, value common.Hash) {
	// If the new value is the same as old, don't set
	prev := s.GetState(key)
	if prev == value {
		return
	}
	// New value is different, update and journal the change
	s.txdb.journal.append(stmStorageChange{
		account:  &s.address,
		key:      key,
		prevalue: prev,
	})
	s.setState(key, value)
}

func (s *stmTxStateObject) setState(key, value common.Hash) {
	s.dirtyStorage[key] = value
}

// AddBalance adds amount to s's balance.
// It is used to add funds to the destination account of a transfer.
func (s *stmTxStateObject) AddBalance(amount *big.Int) {
	// EIP161: We must check emptiness for the objects such that the account
	// clearing (0,0,0 objects) can take effect.
	if amount.Sign() == 0 {
		if s.empty() {
			s.touch()
		}
		return
	}
	//fmt.Println("SetBalance Error", s.Balance(), amount)
	s.SetBalance(new(big.Int).Add(s.Balance(), amount))
}

// SubBalance removes amount from s's balance.
// It is used to remove funds from the origin account of a transfer.
func (s *stmTxStateObject) SubBalance(amount *big.Int) {
	if amount.Sign() == 0 {
		return
	}
	s.SetBalance(new(big.Int).Sub(s.Balance(), amount))
}

func (s *stmTxStateObject) SetBalance(amount *big.Int) {
	s.txdb.journal.append(stmBalanceChange{
		account: &s.address,
		prev:    new(big.Int).Set(s.data.Balance),
	})
	s.setBalance(amount)
}

func (s *stmTxStateObject) setBalance(amount *big.Int) {
	s.data.Balance = amount
}

//
// Attribute accessors
//

// Address returns the address of the contract/account
func (s *stmTxStateObject) Address() common.Address {
	return s.address
}

// Code returns the contract code associated with this object, if any.
func (s *stmTxStateObject) Code() []byte {
	if s.code != nil {
		return s.code
	}
	if bytes.Equal(s.CodeHash(), types.EmptyCodeHash.Bytes()) {
		return nil
	}
	//code := s.statedb.GetCode(s.address)
	//txInfo := TxInfoMini{Index: code.TxInfo.Index, Incarnation: code.TxInfo.Incarnation}
	//s.code = &SCode{Code: common.CopyBytes(code.Code), TxInfo: txInfo}
	//return s.code.Code
	// 开始时须先获取code
	return nil
}

// CodeSize returns the size of the contract code associated with this object,
// or zero if none. This method is an almost mirror of Code, but uses a cache
// inside the database to avoid loading codes seen recently.
func (s *stmTxStateObject) CodeSize() int {
	code := s.Code()
	return len(code)
}

func (s *stmTxStateObject) SetCode(codeHash common.Hash, code []byte) {
	prevcode := s.Code()
	s.txdb.journal.append(stmCodeChange{
		account:  &s.address,
		prevhash: s.CodeHash(),
		prevcode: prevcode,
	})
	s.setCode(codeHash, code)
}

func (s *stmTxStateObject) setCode(codeHash common.Hash, code []byte) {
	s.code = code
	s.data.CodeHash = codeHash[:]
	s.dirtyCode = true
}

func (s *stmTxStateObject) SetNonce(nonce uint64) {
	s.txdb.journal.append(stmNonceChange{
		account: &s.address,
		prev:    s.data.Nonce,
	})
	s.setNonce(nonce)
}

func (s *stmTxStateObject) setNonce(nonce uint64) {
	s.data.Nonce = nonce
}

func (s *stmTxStateObject) CodeHash() []byte {
	return s.data.CodeHash
}

func (s *stmTxStateObject) Balance() *big.Int {
	return s.data.Balance
}

func (s *stmTxStateObject) Nonce() uint64 {
	return s.data.Nonce
}

func (s *stmTxStateObject) Root() common.Hash {
	return s.data.Root
}
