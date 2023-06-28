package state

import (
	"bytes"
	"fmt"
	"io"
	"math/big"
	"sync"
	"time"

	"chukonu/core/types"
	"chukonu/trie"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/rlp"
)

type TxInfoMini struct {
	Index       int
	Incarnation int
}

func (t TxInfoMini) Copy() TxInfoMini {
	return TxInfoMini{
		Index:       t.Index,
		Incarnation: t.Incarnation,
	}
}

type SSlot struct {
	Value  common.Hash
	TxInfo TxInfoMini
}

func newSSlot(value common.Hash, txInfo TxInfoMini) *SSlot {
	return &SSlot{
		Value:  value,
		TxInfo: txInfo.Copy(),
	}
}

func (slot *SSlot) Copy() *SSlot {
	return &SSlot{
		Value:  common.BytesToHash(common.CopyBytes(slot.Value.Bytes())),
		TxInfo: slot.TxInfo.Copy(),
	}
}

type Slot struct {
	Value []*SSlot
	len   int
}

// newSlot, 初始化为从leveldb中读取
func newEmptySlot() *Slot {
	return &Slot{
		Value: make([]*SSlot, 0),
		len:   0,
	}
}

// newSlot, 初始化为从leveldb中读取
func newSlot(value common.Hash) *Slot {
	s := newEmptySlot()
	// 初始化为 -1, 无效为-2
	s.Value = append(s.Value, newSSlot(value, TxInfoMini{Index: -1, Incarnation: -1}))
	s.len += 1
	return s
}

func (s *Slot) Copy() *Slot {
	cpSlot := newEmptySlot()
	for _, value := range s.Value {
		cpSlot.Value = append(cpSlot.Value, &SSlot{Value: value.Value, TxInfo: value.TxInfo.Copy()})
	}
	cpSlot.len = s.len
	return cpSlot
}

type StmStorage map[common.Hash]*Slot

func (s StmStorage) String() (str string) {
	for key, value := range s {
		str += fmt.Sprintf("%X : %X\n", key, value)
	}
	return
}

func (s StmStorage) Copy() StmStorage {
	cpy := make(StmStorage, len(s))
	for key, value := range s {
		cpy[key] = value.Copy()
	}
	return cpy
}

type SStateAccount struct {
	StateAccount *types.StateAccount
	Code         []byte
	// Cache flags.
	// When an object is marked suicided it will be deleted from the trie
	// during the "update" phase of the state transition.
	dirtyCode bool // true if the code was updated
	suicided  bool
	deleted   bool
	TxInfo    TxInfoMini
}

func newSStateAccount(data *types.StateAccount, code []byte, dirtyCode, suicided, deleted bool, txInfo TxInfoMini) *SStateAccount {
	return &SStateAccount{
		StateAccount: data.Copy(),
		Code:         common.CopyBytes(code),
		dirtyCode:    dirtyCode,
		suicided:     suicided,
		deleted:      deleted,
		TxInfo:       txInfo.Copy(),
	}
}

func (ssa *SStateAccount) Copy() *SStateAccount {
	return &SStateAccount{
		StateAccount: ssa.StateAccount.Copy(),
		Code:         common.CopyBytes(ssa.Code),
		dirtyCode:    ssa.dirtyCode,
		suicided:     ssa.suicided,
		deleted:      ssa.deleted,
		TxInfo:       ssa.TxInfo.Copy(),
	}
}

type StmStateAccount struct {
	StateAccount []*SStateAccount
	len          int
}

func newEmptyStmStateAccount() *StmStateAccount {
	return &StmStateAccount{
		StateAccount: make([]*SStateAccount, 0),
		len:          0,
	}
}

// newStmStateAccount 新建一个空的，或者是从leveldb中读取
func newStmStateAccount(data *types.StateAccount, statedb *StmStateDB, addrHash common.Hash) *StmStateAccount {
	sa := newEmptyStmStateAccount()
	// 初始阶段为 -1, 无效为-2
	stateAccount := newSStateAccount(data, nil, false, false, false, TxInfoMini{Index: -1, Incarnation: -1})
	// 该地址为合约地址，则需提取Code
	if !bytes.Equal(data.CodeHash, types.EmptyCodeHash.Bytes()) {
		code, err := statedb.db.ContractCode(addrHash, common.BytesToHash(data.CodeHash))
		if err != nil {
			statedb.setError(fmt.Errorf("can't load code hash %x: %v", data.CodeHash, err))
		}
		stateAccount.Code = common.CopyBytes(code)
	}
	sa.StateAccount = append(sa.StateAccount, stateAccount)
	sa.len += 1
	return sa
}

//func (ssa *StmStateAccount) Copy() *StmStateAccount {
//	cpStmStateAccount := &StmStateAccount{
//		StateAccount: make([]SStateAccount, 0),
//		len:          ssa.len,
//	}
//	for _, value := range ssa.StateAccount {
//		var code []byte = nil
//		if value.Code != nil {
//			code = common.CopyBytes(value.Code)
//		}
//		stateAccount := SStateAccount{
//			StateAccount: value.StateAccount,
//			Code:         code,
//			dirtyCode:    false,
//			suicided:     false,
//			deleted:      false,
//			TxInfo:       value.TxInfo.Copy(),
//		}
//		cpStmStateAccount.StateAccount = append(cpStmStateAccount.StateAccount, stateAccount)
//	}
//	return cpStmStateAccount
//}

// stmStateObject represents an Ethereum account which is being modified.
//
// The usage pattern is as follows:
// First you need to obtain a state object.
// Account values can be accessed and modified through the object.
// Finally, call commitTrie to write the modified storage trie into a database.
type stmStateObject struct {
	address  common.Address
	addrHash common.Hash // hash of ethereum address of the account
	data     *StmStateAccount
	db       *StmStateDB

	// Write caches.
	trie Trie // storage trie, which becomes non-nil on first access

	originMutex    sync.RWMutex
	dirtyMutex     sync.RWMutex
	originStorage  StmStorage // StmStorage cache of original entries to dedup rewrites, reset for every transaction
	pendingStorage StmStorage // StmStorage entries that need to be flushed to disk, at the end of an entire block
	dirtyStorage   StmStorage // StmStorage entries that have been modified in the current transaction execution
}

// empty returns whether the account is considered empty.
// 没有 或 最后一个为空
func (s *stmStateObject) empty() bool {
	if s.data.len == 0 {
		return true
	}
	stateAccount := s.data.StateAccount[s.data.len-1].StateAccount
	return stateAccount.Nonce == 0 && stateAccount.Balance.Sign() == 0 && bytes.Equal(stateAccount.CodeHash, types.EmptyCodeHash.Bytes())
}

// newStmStateObject creates a state object, 开始读读时候会新建
func newStmStateObject(db *StmStateDB, address common.Address, data types.StateAccount) *stmStateObject {
	if data.Balance == nil {
		data.Balance = new(big.Int)
	}
	if data.CodeHash == nil {
		data.CodeHash = types.EmptyCodeHash.Bytes()
	}
	if data.Root == (common.Hash{}) {
		data.Root = types.EmptyRootHash
	}

	return &stmStateObject{
		db:             db,
		address:        address,
		addrHash:       crypto.Keccak256Hash(address[:]),
		data:           newStmStateAccount(&data, db, crypto.Keccak256Hash(address[:])),
		originStorage:  make(StmStorage),
		pendingStorage: make(StmStorage),
		dirtyStorage:   make(StmStorage),
	}
}

func createStmStateObject(db *StmStateDB, address common.Address) *stmStateObject {
	return &stmStateObject{
		db:             db,
		address:        address,
		addrHash:       crypto.Keccak256Hash(address[:]),
		data:           newEmptyStmStateAccount(),
		originStorage:  make(StmStorage),
		pendingStorage: make(StmStorage),
		dirtyStorage:   make(StmStorage),
	}
}

// EncodeRLP implements rlp.Encoder.
func (s *stmStateObject) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, s.data.StateAccount[s.data.len-1].StateAccount)
}

// getTrie returns the associated storage trie. The trie will be opened
// if it's not loaded previously. An error will be returned if trie can't
// be loaded.
func (s *stmStateObject) getTrie(db Database) (Trie, error) {
	stateAccount := s.data.StateAccount[s.data.len-1].StateAccount // 选择最后一个stateAccount
	if s.trie == nil {
		// Try fetching from prefetcher first
		// We don't prefetch empty tries
		if stateAccount.Root != types.EmptyRootHash && s.db.prefetcher != nil {
			// When the miner is creating the pending state, there is no
			// prefetcher
			s.trie = s.db.prefetcher.trie(s.addrHash, stateAccount.Root)
		}
		if s.trie == nil {
			tr, err := db.OpenStorageTrie(s.db.originalRoot, s.addrHash, stateAccount.Root)
			if err != nil {
				return nil, err
			}
			s.trie = tr
		}
	}
	return s.trie, nil
}

// GetState retrieves a value from the account storage trie.
func (s *stmStateObject) GetState(db Database, key common.Hash, txIndex, txIncarnation int) *SSlot {
	// If we have a dirty value for this state entry, return it
	slot, dirty := s.dirtyStorage[key]
	if dirty {
		return slot.Value[slot.len-1].Copy()
	}
	// Otherwise return the entry's original value, 需要从db中获取
	return s.GetCommittedState(db, key, txIndex, txIncarnation)
}

// GetCommittedState retrieves a value from the committed account storage trie.
func (s *stmStateObject) GetCommittedState(db Database, key common.Hash, txIndex, txIncarnation int) *SSlot {
	// If we have a pending write or clean cached, return that
	if slot, pending := s.pendingStorage[key]; pending {
		return slot.Value[slot.len-1].Copy()
	}
	s.originMutex.Lock()
	slot, cached := s.originStorage[key]
	s.originMutex.Unlock()
	if cached {
		return slot.Value[slot.len-1].Copy()
	}
	// If the object was destructed in *this* block (and potentially resurrected),
	// the storage has been cleared out, and we should *not* consult the previous
	// database about any storage values. The only possible alternatives are:
	//   1) resurrect happened, and new slot values were set -- those should
	//      have been handles via pendingStorage above.
	//   2) we don't have new values, and can deliver empty response back
	if _, destructed := s.db.stateObjectsDestruct[s.address]; destructed {
		return newSSlot(common.Hash{}, TxInfoMini{Index: -2, Incarnation: -2})
	}
	// If no live objects are available, attempt to use snapshots
	var (
		enc []byte
		err error
	)
	if s.db.snap != nil {
		start := time.Now()
		enc, err = s.db.snap.Storage(s.addrHash, crypto.Keccak256Hash(key.Bytes()))
		if metrics.EnabledExpensive {
			s.db.SnapshotStorageReads += time.Since(start)
		}
	}
	// If the snapshot is unavailable or reading from it fails, load from the database.
	if s.db.snap == nil || err != nil {
		start := time.Now()
		tr, err := s.getTrie(db)
		if err != nil {
			s.db.setError(err)
			return newSSlot(common.Hash{}, TxInfoMini{Index: -2, Incarnation: -2})
		}
		enc, err = tr.TryGet(key.Bytes())
		if metrics.EnabledExpensive {
			s.db.StorageReads += time.Since(start)
		}
		if err != nil {
			s.db.setError(err)
			return newSSlot(common.Hash{}, TxInfoMini{Index: -2, Incarnation: -2})
		}
	}
	var value common.Hash
	if len(enc) > 0 {
		_, content, _, err := rlp.Split(enc)
		if err != nil {
			s.db.setError(err)
		}
		value.SetBytes(content)
	}
	newSlot1 := newSlot(value)
	s.originMutex.Lock()
	newSlot2, ok := s.originStorage[key]
	if !ok {
		s.originStorage[key] = newSlot1
	}
	s.originMutex.Unlock()
	if ok {
		return newSlot2.Value[newSlot2.len-1].Copy()
	} else {
		return newSlot1.Value[newSlot1.len-1].Copy()
	}
}

// finalise moves all dirty storage slots into the pending area to be hashed or
// committed later. It is invoked at the end of every transaction.
func (s *stmStateObject) finalise(prefetch bool, txIndex int) {
	slotsToPrefetch := make([][]byte, 0, len(s.dirtyStorage))
	for key, dirtySlot := range s.dirtyStorage {
		//if txIndex == 11 {
		//	fmt.Println(key, dirtySlot.Value[dirtySlot.len-1].Value)
		//}
		s.pendingStorage[key] = dirtySlot.Copy()
		originSlot := s.originStorage[key]
		if dirtySlot.Value[dirtySlot.len-1].Value != originSlot.Value[originSlot.len-1].Value {
			slotsToPrefetch = append(slotsToPrefetch, common.CopyBytes(key[:])) // Copy needed for closure
		}
	}
	data := s.data.StateAccount[s.data.len-1].StateAccount
	if s.db.prefetcher != nil && prefetch && len(slotsToPrefetch) > 0 && data.Root != types.EmptyRootHash {
		s.db.prefetcher.prefetch(s.addrHash, data.Root, slotsToPrefetch)
	}
	if len(s.dirtyStorage) > 0 {
		s.dirtyStorage = make(StmStorage)
	}
}

// updateTrie writes cached storage modifications into the object's storage trie.
// It will return nil if the trie has not been loaded and no changes have been
// made. An error will be returned if the trie can't be loaded/updated correctly.
func (s *stmStateObject) updateTrie(db Database) (Trie, error) {
	// Make sure all dirty slots are finalized into the pending storage area
	s.finalise(false, -1) // Don't prefetch anymore, pull directly if need be
	if len(s.pendingStorage) == 0 {
		return s.trie, nil
	}
	// Track the amount of time wasted on updating the storage trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { s.db.StorageUpdates += time.Since(start) }(time.Now())
	}
	// The snapshot storage map for the object
	var (
		storage map[common.Hash][]byte
		hasher  = s.db.hasher
	)
	tr, err := s.getTrie(db)
	if err != nil {
		s.db.setError(err)
		return nil, err
	}
	// Insert all the pending updates into the trie
	usedStorage := make([][]byte, 0, len(s.pendingStorage))
	for key, pendingSlot := range s.pendingStorage {
		// Skip noop changes, persist actual changes
		originSlot := s.originStorage[key]
		pendingValue, originValue := pendingSlot.Value[pendingSlot.len-1].Value, originSlot.Value[originSlot.len-1].Value
		if pendingValue == originValue {
			continue
		}
		s.originStorage[key] = newSlot(pendingValue)

		var v []byte
		if (pendingValue == common.Hash{}) {
			if err := tr.TryDelete(key[:]); err != nil {
				s.db.setError(err)
				return nil, err
			}
			s.db.StorageDeleted += 1
		} else {
			// Encoding []byte cannot fail, ok to ignore the error.
			v, _ = rlp.EncodeToBytes(common.TrimLeftZeroes(pendingValue[:]))
			if err := tr.TryUpdate(key[:], v); err != nil {
				s.db.setError(err)
				return nil, err
			}
			s.db.StorageUpdated += 1
		}
		// If state snapshotting is active, cache the data til commit
		if s.db.snap != nil {
			if storage == nil {
				// Retrieve the old storage map, if available, create a new one otherwise
				if storage = s.db.snapStorage[s.addrHash]; storage == nil {
					storage = make(map[common.Hash][]byte)
					s.db.snapStorage[s.addrHash] = storage
				}
			}
			storage[crypto.HashData(hasher, key[:])] = v // v will be nil if it's deleted
		}
		usedStorage = append(usedStorage, common.CopyBytes(key[:])) // Copy needed for closure
	}
	if s.db.prefetcher != nil {
		data := s.data.StateAccount[s.data.len-1].StateAccount
		s.db.prefetcher.used(s.addrHash, data.Root, usedStorage)
	}
	if len(s.pendingStorage) > 0 {
		s.pendingStorage = make(StmStorage)
	}
	return tr, nil
}

// UpdateRoot sets the trie root to the current root hash of. An error
// will be returned if trie root hash is not computed correctly.
func (s *stmStateObject) updateRoot(db Database) {
	tr, err := s.updateTrie(db)
	if err != nil {
		return
	}
	// If nothing changed, don't bother with hashing anything
	if tr == nil {
		return
	}
	// Track the amount of time wasted on hashing the storage trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { s.db.StorageHashes += time.Since(start) }(time.Now())
	}
	//data := s.data.StateAccount[s.data.len-1].StateAccount
	//data.Root = tr.Hash()
	s.data.StateAccount[s.data.len-1].StateAccount.Root = tr.Hash()
}

// commitTrie submits the storage changes into the storage trie and re-computes
// the root. Besides, all trie changes will be collected in a nodeset and returned.
func (s *stmStateObject) commitTrie(db Database) (*trie.NodeSet, error) {
	tr, err := s.updateTrie(db)
	if err != nil {
		return nil, err
	}
	// If nothing changed, don't bother with committing anything
	if tr == nil {
		return nil, nil
	}
	// Track the amount of time wasted on committing the storage trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { s.db.StorageCommits += time.Since(start) }(time.Now())
	}
	root, nodes := tr.Commit(false)
	data := s.data.StateAccount[s.data.len-1].StateAccount
	data.Root = root
	return nodes, nil
}

func (s *stmStateObject) setStateAccount(txObj *stmTxStateObject, txIndex, txIncarnation int) {
	s.data.StateAccount = append(s.data.StateAccount, newSStateAccount(txObj.data.StateAccount, txObj.data.Code, txObj.data.dirtyCode, txObj.data.suicided, txObj.data.deleted, TxInfoMini{Index: txIndex, Incarnation: txIncarnation}))
	s.data.len += 1
}

// Address returns the address of the contract/account
func (s *stmStateObject) Address() common.Address {
	return s.address
}

func (s *stmStateObject) CodeHash() []byte {
	data := s.data.StateAccount[s.data.len-1].StateAccount
	return data.CodeHash
}

func (s *stmStateObject) Balance() *big.Int {
	data := s.data.StateAccount[s.data.len-1].StateAccount
	return data.Balance
}

func (s *stmStateObject) Nonce() uint64 {
	data := s.data.StateAccount[s.data.len-1].StateAccount
	return data.Nonce
}

func (s *stmStateObject) Root() common.Hash {
	data := s.data.StateAccount[s.data.len-1].StateAccount
	return data.Root
}
