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
	//data     *StmStateAccount
	data types.StateAccount
	db   *StmStateDB

	// Write caches.
	trieMutex sync.Mutex
	trie      Trie // storage trie, which becomes non-nil on first access
	code      Code // contract bytecode, which gets set when code is loaded

	originMutex    sync.RWMutex
	dirtyMutex     sync.RWMutex
	originStorage  Storage // Storage cache of original entries to dedup rewrites, reset for every transaction
	pendingStorage Storage // Storage entries that need to be flushed to disk, at the end of an entire block
	dirtyStorage   Storage // Storage entries that have been modified in the current transaction execution

	// Cache flags.
	// When an object is marked suicided it will be deleted from the trie
	// during the "update" phase of the state transition.
	dirtyCode bool // true if the code was updated
	suicided  bool
	deleted   bool
}

// empty returns whether the account is considered empty.
// 没有 或 最后一个为空
func (s *stmStateObject) empty() bool {
	return s.data.Nonce == 0 && s.data.Balance.Sign() == 0 && bytes.Equal(s.data.CodeHash, types.EmptyCodeHash.Bytes())
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
		data:           data,
		originStorage:  make(Storage),
		pendingStorage: make(Storage),
		dirtyStorage:   make(Storage),
	}
}

func createStmStateObject(db *StmStateDB, address common.Address) *stmStateObject {
	return &stmStateObject{
		db:             db,
		address:        address,
		addrHash:       crypto.Keccak256Hash(address[:]),
		data:           types.StateAccount{Root: types.EmptyRootHash, CodeHash: types.EmptyCodeHash.Bytes()},
		originStorage:  make(Storage),
		pendingStorage: make(Storage),
		dirtyStorage:   make(Storage),
	}
}

// EncodeRLP implements rlp.Encoder.
func (s *stmStateObject) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, &s.data)
}

// getTrie returns the associated storage trie. The trie will be opened
// if it's not loaded previously. An error will be returned if trie can't
// be loaded.
func (s *stmStateObject) getTrie(db Database) (Trie, error) {
	if s.trie == nil {
		// Try fetching from prefetcher first
		// We don't prefetch empty tries
		if s.data.Root != types.EmptyRootHash && s.db.prefetcher != nil {
			// When the miner is creating the pending state, there is no
			// prefetcher
			s.trie = s.db.prefetcher.trie(s.addrHash, s.data.Root)
		}
		if s.trie == nil {
			s.trieMutex.Lock()
			tr, err := db.OpenStorageTrie(s.db.originalRoot, s.addrHash, s.data.Root)
			s.trieMutex.Unlock()
			if err != nil {
				return nil, err
			}
			s.trie = tr
		}
	}
	return s.trie, nil
}

// GetState retrieves a value from the account storage trie.
func (s *stmStateObject) GetState(db Database, key common.Hash, txIndex, txIncarnation int) common.Hash {
	// If we have a dirty value for this state entry, return it
	s.dirtyMutex.RLock()
	value, dirty := s.dirtyStorage[key]
	s.dirtyMutex.RUnlock()
	if dirty {
		return value
	}
	// Otherwise return the entry's original value, 需要从db中获取
	return s.GetCommittedState(db, key, txIndex, txIncarnation)
}

// GetCommittedState retrieves a value from the committed account storage trie.
func (s *stmStateObject) GetCommittedState(db Database, key common.Hash, txIndex, txIncarnation int) common.Hash {
	// If we have a pending write or clean cached, return that
	if value, pending := s.pendingStorage[key]; pending {
		return value
	}

	s.originMutex.RLock()
	value, cached := s.originStorage[key]
	s.originMutex.RUnlock()
	if cached {
		return value
	}
	// If the object was destructed in *this* block (and potentially resurrected),
	// the storage has been cleared out, and we should *not* consult the previous
	// database about any storage values. The only possible alternatives are:
	//   1) resurrect happened, and new slot values were set -- those should
	//      have been handles via pendingStorage above.
	//   2) we don't have new values, and can deliver empty response back
	if _, destructed := s.db.stateObjectsDestruct[s.address]; destructed {
		return common.Hash{}
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
			fmt.Println("get trie", err)
			s.db.setError(err)
			return common.Hash{}
		}
		s.trieMutex.Lock()
		enc, err = tr.TryGet(key.Bytes())
		s.trieMutex.Unlock()
		if metrics.EnabledExpensive {
			s.db.StorageReads += time.Since(start)
		}
		if err != nil {
			s.db.setError(err)
			return common.Hash{}
		}
	}
	var newValue common.Hash
	if len(enc) > 0 {
		_, content, _, err := rlp.Split(enc)
		if err != nil {
			s.db.setError(err)
		}
		newValue.SetBytes(content)
	}
	s.originMutex.Lock()
	s.originStorage[key] = newValue
	s.originMutex.Unlock()
	return newValue
}

// finalise moves all dirty storage slots into the pending area to be hashed or
// committed later. It is invoked at the end of every transaction.
func (s *stmStateObject) finalise(prefetch bool, txIndex int) {
	slotsToPrefetch := make([][]byte, 0, len(s.dirtyStorage))
	for key, value := range s.dirtyStorage {
		s.pendingStorage[key] = value
		if value != s.originStorage[key] {
			slotsToPrefetch = append(slotsToPrefetch, common.CopyBytes(key[:])) // Copy needed for closure
		}
	}
	if s.db.prefetcher != nil && prefetch && len(slotsToPrefetch) > 0 && s.data.Root != types.EmptyRootHash {
		s.db.prefetcher.prefetch(s.addrHash, s.data.Root, slotsToPrefetch)
	}

	if len(s.dirtyStorage) > 0 {
		s.dirtyStorage = make(Storage)
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
		hasher  = crypto.NewKeccakState()
	)
	tr, err := s.getTrie(db)
	if err != nil {
		fmt.Println("get trie", err)
		s.db.setError(err)
		return nil, err
	}
	// Insert all the pending updates into the trie
	usedStorage := make([][]byte, 0, len(s.pendingStorage))
	for key, value := range s.pendingStorage {
		// Skip noop changes, persist actual changes
		if value == s.originStorage[key] {
			continue
		}
		s.originStorage[key] = value

		var v []byte
		if (value == common.Hash{}) {
			if err := tr.TryDelete(key[:]); err != nil {
				s.db.setError(err)
				return nil, err
			}
			s.db.StorageDeleted += 1
		} else {
			// Encoding []byte cannot fail, ok to ignore the error.
			v, _ = rlp.EncodeToBytes(common.TrimLeftZeroes(value[:]))
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
		s.db.prefetcher.used(s.addrHash, s.data.Root, usedStorage)
	}
	if len(s.pendingStorage) > 0 {
		s.pendingStorage = make(Storage)
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
	s.data.Root = tr.Hash()
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
	s.data.Root = root
	return nodes, nil
}

func (s *stmStateObject) setStateAccount(txObj *stmTxStateObject, txIndex, txIncarnation int) {
	s.data.Nonce = txObj.data.Nonce
	s.data.Balance.Set(txObj.data.Balance)
	s.data.CodeHash = common.CopyBytes(txObj.data.CodeHash)
	s.code = common.CopyBytes(txObj.code)
	s.dirtyCode = txObj.dirtyCode
	s.suicided = txObj.suicided
	s.deleted = txObj.deleted
}

// Address returns the address of the contract/account
func (s *stmStateObject) Address() common.Address {
	return s.address
}

func (s *stmStateObject) CodeHash() []byte {
	return s.data.CodeHash
}

func (s *stmStateObject) Balance() *big.Int {
	return s.data.Balance
}

func (s *stmStateObject) Nonce() uint64 {
	return s.data.Nonce
}

func (s *stmStateObject) Root() common.Hash {
	return s.data.Root
}
