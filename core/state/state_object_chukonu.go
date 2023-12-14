package state

import (
	"bytes"
	"fmt"
	"io"
	"math/big"
	"time"

	"chukonu/core/types"
	"chukonu/trie"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/rlp"
)

type ChuKoNuStateObject struct {
	address    common.Address
	addrHash   common.Hash // hash of ethereum address of the account
	data       types.StateAccount
	originData *types.StateAccount
	db         *ChuKoNuStateDB

	// Write caches.
	trie       Trie // storage trie, which becomes non-nil on first access
	code       Code // contract bytecode, which gets set when code is loaded
	originCode Code

	originStableStorage Storage // last snapshot
	originStorage       Storage // Storage cache of original entries to dedup rewrites, reset for every transaction
	pendingStorage      Storage // Storage entries that need to be flushed to disk, at the end of an entire block
	dirtyStorage        Storage // Storage entries that have been modified in the current transaction execution

	dirty bool // for single account state

	// Cache flags.
	// When an object is marked suicided it will be deleted from the trie
	// during the "update" phase of the state transition.
	dirtyCode bool // true if the code was updated
	suicided  bool
	deleted   bool
}

// empty returns whether the account is considered empty.
func (s *ChuKoNuStateObject) empty() bool {
	return s.data.Nonce == 0 && s.data.Balance.Sign() == 0 && bytes.Equal(s.data.CodeHash, types.EmptyCodeHash.Bytes())
}

// newChuKoNuObject creates a state object.
func newChuKoNuObject(db *ChuKoNuStateDB, address common.Address, data types.StateAccount, origin bool) *ChuKoNuStateObject {
	if data.Balance == nil {
		data.Balance = new(big.Int)
	}
	if data.CodeHash == nil {
		data.CodeHash = types.EmptyCodeHash.Bytes()
	}
	if data.Root == (common.Hash{}) {
		data.Root = types.EmptyRootHash
	}
	obj := &ChuKoNuStateObject{
		db:                  db,
		address:             address,
		addrHash:            crypto.Keccak256Hash(address[:]),
		data:                data,
		originStableStorage: make(Storage),
		originStorage:       make(Storage),
		pendingStorage:      make(Storage),
		dirtyStorage:        make(Storage),
	}
	if origin {
		obj.originData = data.Copy()
		obj.originCode = common.CopyBytes(obj.Code(db.db))
	} else {
		obj.originData = nil
		obj.originCode = nil
	}

	return obj
}

// EncodeRLP implements rlp.Encoder.
func (s *ChuKoNuStateObject) EncodeRLP(w io.Writer) error {
	return rlp.Encode(w, &s.data)
}

func (s *ChuKoNuStateObject) markSuicided() {
	s.suicided = true
}

func (s *ChuKoNuStateObject) touch() {
	//s.db.appendJournal()
	s.db.journal.append(chuKoNuTouchChange{
		account: &s.address,
	})
	if s.address == ripemd {
		// Explicitly put it in the dirty-cache, which is otherwise generated from
		// flattened journals.
		s.db.journal.dirty(s.address)
	}
}

// getTrie returns the associated storage trie. The trie will be opened
// if it's not loaded previously. An error will be returned if trie can't
// be loaded.
func (s *ChuKoNuStateObject) getTrie(db Database) (Trie, error) {
	if s.trie == nil {
		// Try fetching from prefetcher first
		// We don't prefetch empty tries
		if s.data.Root != types.EmptyRootHash && s.db.prefetcher != nil {
			// When the miner is creating the pending state, there is no
			// prefetcher
			s.trie = s.db.prefetcher.trie(s.addrHash, s.data.Root)
		}
		if s.trie == nil {
			tr, err := db.OpenStorageTrie(s.db.originalRoot, s.addrHash, s.data.Root)
			if err != nil {
				return nil, err
			}
			s.trie = tr
		}
	}
	return s.trie, nil
}

// GetState retrieves a value from the account storage trie.
func (s *ChuKoNuStateObject) GetState(db Database, key common.Hash) common.Hash {
	// If we have a dirty value for this state entry, return it
	value, dirty := s.dirtyStorage[key]
	if dirty {
		return value
	}
	// Otherwise return the entry's original value
	return s.GetCommittedState(db, key)
}

// GetCommittedState retrieves a value from the committed account storage trie.
func (s *ChuKoNuStateObject) GetCommittedState(db Database, key common.Hash) common.Hash {
	// If we have a pending write or clean cached, return that
	if value, pending := s.pendingStorage[key]; pending {
		return value
	}
	if value, cached := s.originStorage[key]; cached {
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
			s.db.setError(err)
			s.originStableStorage[key] = common.Hash{}
			return common.Hash{}
		}
		enc, err = tr.TryGet(key.Bytes())
		if metrics.EnabledExpensive {
			s.db.StorageReads += time.Since(start)
		}
		if err != nil {
			s.db.setError(err)
			s.originStableStorage[key] = common.Hash{}
			return common.Hash{}
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
	s.originStorage[key] = value
	s.originStableStorage[key] = value
	return value
}

// SetState updates a value in account storage.
func (s *ChuKoNuStateObject) SetState(db Database, key, value common.Hash) {
	// If the new value is the same as old, don't set
	prev := s.GetState(db, key)
	if prev == value {
		return
	}
	// New value is different, update and journal the change
	s.db.journal.append(chuKoNuStorageChange{
		account:  &s.address,
		key:      key,
		prevalue: prev,
	})
	s.setState(key, value)
}

func (s *ChuKoNuStateObject) setState(key, value common.Hash) {
	s.dirtyStorage[key] = value
}

// finalise moves all dirty storage slots into the pending area to be hashed or
// committed later. It is invoked at the end of every transaction.
func (s *ChuKoNuStateObject) finalise(prefetch bool, txIndex int) {
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
func (s *ChuKoNuStateObject) updateTrie(db Database) (Trie, error) {
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
		hasher  = crypto.NewKeccakState() // origin: hasher  = s.db.hasher
	)
	tr, err := s.getTrie(db)
	if err != nil {
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
func (s *ChuKoNuStateObject) updateRoot(db Database) {
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
func (s *ChuKoNuStateObject) commitTrie(db Database) (*trie.NodeSet, error) {
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

// AddBalance adds amount to s's balance.
// It is used to add funds to the destination account of a transfer.
func (s *ChuKoNuStateObject) AddBalance(amount *big.Int) {
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
func (s *ChuKoNuStateObject) SubBalance(amount *big.Int) {
	if amount.Sign() == 0 {
		return
	}
	s.SetBalance(new(big.Int).Sub(s.Balance(), amount))
}

func (s *ChuKoNuStateObject) SetBalance(amount *big.Int) {
	s.db.journal.append(chuKoNuBalanceChange{
		account: &s.address,
		prev:    new(big.Int).Set(s.data.Balance),
	})
	s.setBalance(amount)
}

func (s *ChuKoNuStateObject) setBalance(amount *big.Int) {
	s.data.Balance = amount
}

func (s *ChuKoNuStateObject) deepCopy(db *ChuKoNuStateDB) *ChuKoNuStateObject {
	chuKoNuStateObject := newChuKoNuObject(db, s.address, s.data, false)
	if s.originData == nil {
		chuKoNuStateObject.originData = nil
	} else {
		chuKoNuStateObject.originData = s.originData.Copy()
	}
	chuKoNuStateObject.originCode = common.CopyBytes(chuKoNuStateObject.originCode)

	if s.trie != nil {
		chuKoNuStateObject.trie = db.db.CopyTrie(s.trie)
	}
	chuKoNuStateObject.code = s.code
	chuKoNuStateObject.originStableStorage = s.originStableStorage.Copy()
	chuKoNuStateObject.dirtyStorage = s.dirtyStorage.Copy()
	chuKoNuStateObject.originStorage = s.originStorage.Copy()
	chuKoNuStateObject.pendingStorage = s.pendingStorage.Copy()
	chuKoNuStateObject.suicided = s.suicided
	chuKoNuStateObject.dirtyCode = s.dirtyCode
	chuKoNuStateObject.deleted = s.deleted
	chuKoNuStateObject.dirty = s.dirty
	return chuKoNuStateObject
}

//
// Attribute accessors
//

// Address returns the address of the contract/account
func (s *ChuKoNuStateObject) Address() common.Address {
	return s.address
}

// Code returns the contract code associated with this object, if any.
func (s *ChuKoNuStateObject) Code(db Database) []byte {
	if s.code != nil {
		return s.code
	}
	if bytes.Equal(s.CodeHash(), types.EmptyCodeHash.Bytes()) {
		return nil
	}
	code, err := db.ContractCode(s.addrHash, common.BytesToHash(s.CodeHash()))
	if err != nil {
		s.db.setError(fmt.Errorf("can't load code hash %x: %v", s.CodeHash(), err))
	}
	s.code = code
	return code
}

// CodeSize returns the size of the contract code associated with this object,
// or zero if none. This method is an almost mirror of Code, but uses a cache
// inside the database to avoid loading codes seen recently.
func (s *ChuKoNuStateObject) CodeSize(db Database) int {
	if s.code != nil {
		return len(s.code)
	}
	if bytes.Equal(s.CodeHash(), types.EmptyCodeHash.Bytes()) {
		return 0
	}
	size, err := db.ContractCodeSize(s.addrHash, common.BytesToHash(s.CodeHash()))
	if err != nil {
		s.db.setError(fmt.Errorf("can't load code size %x: %v", s.CodeHash(), err))
	}
	return size
}

func (s *ChuKoNuStateObject) SetCode(codeHash common.Hash, code []byte) {
	prevcode := s.Code(s.db.db)
	s.db.journal.append(chuKoNuCodeChange{
		account:  &s.address,
		prevhash: s.CodeHash(),
		prevcode: prevcode,
	})
	s.setCode(codeHash, code)
}

func (s *ChuKoNuStateObject) setCode(codeHash common.Hash, code []byte) {
	s.code = code
	s.data.CodeHash = codeHash[:]
	s.dirtyCode = true
}

func (s *ChuKoNuStateObject) SetNonce(nonce uint64) {
	s.db.journal.append(chuKoNuNonceChange{
		account: &s.address,
		prev:    s.data.Nonce,
	})
	s.setNonce(nonce)
}

func (s *ChuKoNuStateObject) setNonce(nonce uint64) {
	s.data.Nonce = nonce
}

func (s *ChuKoNuStateObject) CodeHash() []byte {
	return s.data.CodeHash
}

func (s *ChuKoNuStateObject) Balance() *big.Int {
	return s.data.Balance
}

func (s *ChuKoNuStateObject) Nonce() uint64 {
	return s.data.Nonce
}

func (s *ChuKoNuStateObject) OriginNonce() uint64 {
	if s.originData == nil {
		return 0
	}
	return s.originData.Nonce
}

func (s *ChuKoNuStateObject) OriginBalance() *big.Int {
	if s.originData == nil {
		return new(big.Int).SetInt64(0)
	}
	return new(big.Int).Set(s.originData.Balance)
}

func (s *ChuKoNuStateObject) OriginState(db Database, key common.Hash) common.Hash {
	if value, cached := s.originStableStorage[key]; cached {
		return value
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
			s.originStableStorage[key] = common.Hash{}
			return common.Hash{}
		}
		enc, err = tr.TryGet(key.Bytes())
		if metrics.EnabledExpensive {
			s.db.StorageReads += time.Since(start)
		}
		if err != nil {
			s.db.setError(err)
			s.originStableStorage[key] = common.Hash{}
			return common.Hash{}
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
	s.originStableStorage[key] = value
	s.originStorage[key] = value
	return value
}

func (s *ChuKoNuStateObject) OriginCodeHash() []byte {
	if s.originData == nil {
		return types.EmptyCodeHash.Bytes()
	}
	return common.CopyBytes(s.originData.CodeHash)
}

func (s *ChuKoNuStateObject) OriginCode(db Database) []byte {
	if s.originCode != nil {
		return common.CopyBytes(s.originCode)
	}

	if s.originData == nil {
		return nil
	}
	if bytes.Equal(s.OriginCodeHash(), types.EmptyCodeHash.Bytes()) {
		return nil
	}
	code, err := db.ContractCode(s.addrHash, common.BytesToHash(s.CodeHash()))
	if err != nil {
		s.db.setError(fmt.Errorf("can't load code hash %x: %v", s.CodeHash(), err))
	}
	s.originCode = code
	return code
}

func (s *ChuKoNuStateObject) OriginCodeSize(db Database) int {
	code := s.OriginCode(db)
	if code != nil {
		return len(code)
	}
	return 0
}
