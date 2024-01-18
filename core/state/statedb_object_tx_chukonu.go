package state

import (
	"bytes"
	"chukonu/core/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
	"io"
	"math/big"
)

type ChuKoNuTxStateObject struct {
	address    common.Address
	addrHash   common.Hash // hash of ethereum address of the account
	data       types.StateAccount
	originData *types.StateAccount
	txdb       *ChuKoNuTxStateDB
	statedb    *ChuKoNuStateDB

	code       Code // contract bytecode, which gets set when code is loaded
	originCode Code

	originStableStorage Storage // last snapshot
	originStorage       Storage // Storage cache of original entries to dedup rewrites, reset for every transaction
	dirtyStorage        Storage // Storage entries that have been modified in the current transaction execution

	// Cache flags.
	// When an object is marked suicided it will be deleted from the trie
	// during the "update" phase of the state transition.
	dirtyCode bool // true if the code was updated
	suicided  bool
	deleted   bool
}

// empty returns whether the account is considered empty.
func (s *ChuKoNuTxStateObject) empty() bool {
	return s.data.Nonce == 0 && s.data.Balance.Sign() == 0 && bytes.Equal(s.data.CodeHash, types.EmptyCodeHash.Bytes())
}

// NewChuKoNuTxObject creates a state object.
func NewChuKoNuTxObject(txdb *ChuKoNuTxStateDB, statedb *ChuKoNuStateDB, address common.Address, data types.StateAccount, origin bool, code []byte, deleted bool) *ChuKoNuTxStateObject {
	if data.Balance == nil {
		data.Balance = new(big.Int)
	}
	if data.CodeHash == nil {
		data.CodeHash = types.EmptyCodeHash.Bytes()
	}
	if data.Root == (common.Hash{}) {
		data.Root = types.EmptyRootHash
	}
	obj := &ChuKoNuTxStateObject{
		txdb:                txdb,
		statedb:             statedb,
		address:             address,
		addrHash:            crypto.Keccak256Hash(address[:]),
		data:                data,
		originStableStorage: make(Storage),
		originStorage:       make(Storage),
		dirtyStorage:        make(Storage),
		code:                code,
		deleted:             deleted,
	}
	if origin {
		obj.originData = data.Copy()
		obj.originCode = common.CopyBytes(obj.Code())
	} else {
		obj.originData = nil
		obj.originCode = nil
	}

	return obj
}

// EncodeRLP implements rlp.Encoder.
func (s *ChuKoNuTxStateObject) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, &s.data)
}

func (s *ChuKoNuTxStateObject) markSuicided() {
	s.suicided = true
}

func (s *ChuKoNuTxStateObject) touch() {
	s.txdb.journal.append(chuKoNuTouchChange{
		account: &s.address,
	})
	if s.address == ripemd {
		// Explicitly put it in the dirty-cache, which is otherwise generated from
		// flattened journals.
		s.txdb.journal.dirty(s.address)
	}
}

// GetState retrieves a value from the account storage trie.
func (s *ChuKoNuTxStateObject) GetState(key common.Hash) common.Hash {
	// If we have a dirty value for this state entry, return it
	value, dirty := s.dirtyStorage[key]
	if dirty {
		return value
	}
	// Otherwise return the entry's original value
	return s.GetCommittedState(key)
}

// GetCommittedState retrieves a value from the committed account storage trie.
func (s *ChuKoNuTxStateObject) GetCommittedState(key common.Hash) common.Hash {
	if value, cached := s.originStorage[key]; cached {
		return value
	}
	if value, cached := s.originStableStorage[key]; cached {
		return value
	}
	// If the object was destructed in *this* block (and potentially resurrected),
	// the storage has been cleared out, and we should *not* consult the previous
	// database about any storage values. The only possible alternatives are:
	//   1) resurrect happened, and new slot values were set -- those should
	//      have been handles via pendingStorage above.
	//   2) we don't have new values, and can deliver empty response back
	if _, destructed := s.txdb.stateObjectsDestruct[s.address]; destructed {
		return common.Hash{}
	}
	return common.Hash{}
	//return s.statedb.GetOriginState(s.address, key) // 需并发的从statedb中读取
}

// SetState updates a value in account storage.
func (s *ChuKoNuTxStateObject) SetState(key, value common.Hash) {
	// If the new value is the same as old, don't set
	prev := s.GetState(key)
	if prev == value {
		return
	}
	// New value is different, update and journal the change
	s.txdb.journal.append(chuKoNuStorageChange{
		account:  &s.address,
		key:      key,
		prevalue: prev,
	})
	s.setState(key, value)
}

func (s *ChuKoNuTxStateObject) setState(key, value common.Hash) {
	s.dirtyStorage[key] = value
}

// AddBalance adds amount to s's balance.
// It is used to add funds to the destination account of a transfer.
func (s *ChuKoNuTxStateObject) AddBalance(amount *big.Int) {
	// EIP161: We must check emptiness for the objects such that the account
	// clearing (0,0,0 objects) can take effect.
	if amount.Sign() == 0 {
		if s.empty() {
			s.touch()
		}
		return
	}
	s.SetBalance(new(big.Int).Add(s.Balance(), amount))
}

// SubBalance removes amount from s's balance.
// It is used to remove funds from the origin account of a transfer.
func (s *ChuKoNuTxStateObject) SubBalance(amount *big.Int) {
	if amount.Sign() == 0 {
		return
	}
	s.SetBalance(new(big.Int).Sub(s.Balance(), amount))
}

func (s *ChuKoNuTxStateObject) SetBalance(amount *big.Int) {
	s.txdb.journal.append(chuKoNuBalanceChange{
		account: &s.address,
		prev:    new(big.Int).Set(s.data.Balance),
	})
	s.setBalance(amount)
}

func (s *ChuKoNuTxStateObject) setBalance(amount *big.Int) {
	s.data.Balance = amount
}

func (s *ChuKoNuTxStateObject) deepCopy(txdb *ChuKoNuTxStateDB, statedb *ChuKoNuStateDB) *ChuKoNuTxStateObject {
	chuKoNuStateObject := NewChuKoNuTxObject(txdb, statedb, s.address, s.data, false, nil, false)
	if s.originData == nil {
		chuKoNuStateObject.originData = nil
	} else {
		chuKoNuStateObject.originData = s.originData.Copy()
	}
	chuKoNuStateObject.originCode = common.CopyBytes(chuKoNuStateObject.originCode)

	chuKoNuStateObject.code = s.code
	chuKoNuStateObject.originStableStorage = s.originStableStorage.Copy()
	chuKoNuStateObject.dirtyStorage = s.dirtyStorage.Copy()
	chuKoNuStateObject.originStorage = s.originStorage.Copy()
	chuKoNuStateObject.suicided = s.suicided
	chuKoNuStateObject.dirtyCode = s.dirtyCode
	chuKoNuStateObject.deleted = s.deleted
	return chuKoNuStateObject
}

//
// Attribute accessors
//

// Address returns the address of the contract/account
func (s *ChuKoNuTxStateObject) Address() common.Address {
	return s.address
}

// Code returns the contract code associated with this object, if any.
func (s *ChuKoNuTxStateObject) Code() []byte {
	if s.code != nil {
		return s.code
	}
	if bytes.Equal(s.CodeHash(), types.EmptyCodeHash.Bytes()) {
		return nil
	}
	code := s.statedb.GetOriginCode(s.address)
	s.code = code
	return code
}

// CodeSize returns the size of the contract code associated with this object,
// or zero if none. This method is an almost mirror of Code, but uses a cache
// inside the database to avoid loading codes seen recently.
func (s *ChuKoNuTxStateObject) CodeSize() int {
	code := s.Code()
	if code != nil {
		return len(code)
	}
	return 0
}

func (s *ChuKoNuTxStateObject) SetCode(codeHash common.Hash, code []byte) {
	prevcode := s.Code()
	s.txdb.journal.append(chuKoNuCodeChange{
		account:  &s.address,
		prevhash: s.CodeHash(),
		prevcode: prevcode,
	})
	s.setCode(codeHash, code)
}

func (s *ChuKoNuTxStateObject) setCode(codeHash common.Hash, code []byte) {
	s.code = code
	s.data.CodeHash = codeHash[:]
	s.dirtyCode = true
}

func (s *ChuKoNuTxStateObject) SetNonce(nonce uint64) {
	s.txdb.journal.append(chuKoNuNonceChange{
		account: &s.address,
		prev:    s.data.Nonce,
	})
	s.setNonce(nonce)
}

func (s *ChuKoNuTxStateObject) setNonce(nonce uint64) {
	s.data.Nonce = nonce
}

func (s *ChuKoNuTxStateObject) CodeHash() []byte {
	return s.data.CodeHash
}

func (s *ChuKoNuTxStateObject) Balance() *big.Int {
	return s.data.Balance
}

func (s *ChuKoNuTxStateObject) Nonce() uint64 {
	return s.data.Nonce
}

func (s *ChuKoNuTxStateObject) OriginNonce() uint64 {
	if s.originData == nil {
		return 0
	}
	return s.originData.Nonce
}

func (s *ChuKoNuTxStateObject) OriginBalance() *big.Int {
	if s.originData == nil {
		return new(big.Int).SetInt64(0)
	}
	return new(big.Int).Set(s.originData.Balance)
}

func (s *ChuKoNuTxStateObject) OriginState(key common.Hash) common.Hash {
	if value, cached := s.originStableStorage[key]; cached {
		return value
	}
	value := s.GetCommittedState(key)
	s.originStableStorage[key] = value
	s.originStorage[key] = value
	return value
}

func (s *ChuKoNuTxStateObject) OriginCodeHash() []byte {
	if s.originData == nil {
		return types.EmptyCodeHash.Bytes()
	}
	return common.CopyBytes(s.originData.CodeHash)
}

func (s *ChuKoNuTxStateObject) OriginCode() []byte {
	if s.originCode != nil {
		return common.CopyBytes(s.originCode)
	}

	if s.originData == nil {
		return nil
	}
	if bytes.Equal(s.OriginCodeHash(), types.EmptyCodeHash.Bytes()) {
		return nil
	}
	code := s.statedb.GetOriginCode(s.address)
	s.originCode = code
	return code
}

func (s *ChuKoNuTxStateObject) OriginCodeSize() int {
	code := s.OriginCode()
	if code != nil {
		return len(code)
	}
	return 0
}

func (s *ChuKoNuTxStateObject) IsDataChange() bool {
	if s.originData == nil && (s.data.Nonce != 0 || s.data.Balance.Sign() != 0) {
		return true
	}
	if s.originData != nil && (s.data.Nonce != s.originData.Nonce || s.data.Balance.Cmp(s.originData.Balance) != 0) {
		return true
	}
	return false
}
