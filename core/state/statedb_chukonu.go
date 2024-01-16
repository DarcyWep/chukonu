package state

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"time"

	"chukonu/core/rawdb"
	"chukonu/core/state/snapshot"
	"chukonu/core/types"
	"chukonu/trie"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/params"
	"github.com/ethereum/go-ethereum/rlp"
)

// ChuKoNuStateDB structs within the ethereum protocol are used to store anything
// within the merkle trie. StateDBs take care of caching and storing
// nested states. It's the general query interface to retrieve:
// * Contracts
// * Accounts
type ChuKoNuStateDB struct {
	db         Database
	prefetcher *triePrefetcher

	trieMutex sync.Mutex
	trie      Trie
	//hasher     crypto.KeccakState

	// originalRoot is the pre-state root, before any changes were made.
	// It will be updated when the Commit is called.
	originalRoot common.Hash

	snaps        *snapshot.Tree
	snap         snapshot.Snapshot
	snapAccounts map[common.Hash][]byte
	snapStorage  map[common.Hash]map[common.Hash][]byte

	// This map holds 'live' objects, which will get modified while processing a state transition.
	objectMutex         sync.RWMutex // 并发读取使用
	stateObjects        map[common.Address]*ChuKoNuStateObject
	stateObjectsPending map[common.Address]struct{} // State objects finalized but not yet written to the trie

	stateObjectsDirtyMutex sync.Mutex
	stateObjectsDirty      map[common.Address]struct{} // State objects modified in the current execution
	stateObjectsDestruct   map[common.Address]struct{} // State objects destructed in the block

	// DB error.
	// State objects are used by the consensus core and VM which are
	// unable to deal with database-level errors. Any error that occurs
	// during a database read is memoized here and will eventually be
	// returned by ChuKoNuStateDB.Commit. Notably, this error is also shared
	// by all cached state objects in case the database failure occurs
	// when accessing state of accounts.
	dbErr error

	// The refund counter, also used by state transitioning.
	refund uint64

	thash             common.Hash
	txIndex           int
	accessAddress     *types.AccessAddressMap
	detectStateChange bool
	txStateObjects    map[common.Address]*ChuKoNuStateObject

	logs    map[common.Hash][]*types.Log
	logSize uint

	preimages map[common.Hash][]byte

	// Per-transaction access list
	accessList *accessList

	// Transient storage
	transientStorage transientStorage

	// Journal of state modifications. This is the backbone of
	// Snapshot and RevertToSnapshot.
	journal        *chuKoNuJournal
	validRevisions []revision
	nextRevisionId int

	// Measurements gathered during execution for debugging purposes
	AccountReads         time.Duration
	AccountHashes        time.Duration
	AccountUpdates       time.Duration
	AccountCommits       time.Duration
	StorageReads         time.Duration
	StorageHashes        time.Duration
	StorageUpdates       time.Duration
	StorageCommits       time.Duration
	SnapshotAccountReads time.Duration
	SnapshotStorageReads time.Duration
	SnapshotCommits      time.Duration
	TrieDBCommits        time.Duration

	AccountUpdated int
	StorageUpdated int
	AccountDeleted int
	StorageDeleted int
}

// NewChuKoNuStateDB creates a new state from a given trie.
func NewChuKoNuStateDB(root common.Hash, db Database, snaps *snapshot.Tree, trie Trie) (*ChuKoNuStateDB, error) {
	var tr Trie
	var err error = nil
	if trie != nil { // 已经有树，直接copy已经在内存生成的树
		tr = db.CopyTrie(trie)
	} else {
		tr, err = db.OpenTrie(root)
	}
	if err != nil {
		return nil, err
	}
	sdb := &ChuKoNuStateDB{
		db:                   db,
		trie:                 tr,
		originalRoot:         root,
		snaps:                snaps,
		stateObjects:         make(map[common.Address]*ChuKoNuStateObject),
		stateObjectsPending:  make(map[common.Address]struct{}),
		stateObjectsDirty:    make(map[common.Address]struct{}),
		stateObjectsDestruct: make(map[common.Address]struct{}),
		logs:                 make(map[common.Hash][]*types.Log),
		preimages:            make(map[common.Hash][]byte),
		journal:              newChuKoNuJournal(),
		accessList:           newAccessList(),
		transientStorage:     newTransientStorage(),
		//hasher:               crypto.NewKeccakState(),
	}
	if sdb.snaps != nil {
		if sdb.snap = sdb.snaps.Snapshot(root); sdb.snap != nil {
			sdb.snapAccounts = make(map[common.Hash][]byte)
			sdb.snapStorage = make(map[common.Hash]map[common.Hash][]byte)
		}
	}
	return sdb, nil
}

// StartPrefetcher initializes a new trie prefetcher to pull in nodes from the
// state trie concurrently while the state is mutated so that when we reach the
// commit phase, most of the needed data is already hot.
func (s *ChuKoNuStateDB) StartPrefetcher(namespace string) {
	if s.prefetcher != nil {
		s.prefetcher.close()
		s.prefetcher = nil
	}
	if s.snap != nil {
		s.prefetcher = newTriePrefetcher(s.db, s.originalRoot, namespace)
	}
}

// StopPrefetcher terminates a running prefetcher and reports any leftover stats
// from the gathered metrics.
func (s *ChuKoNuStateDB) StopPrefetcher() {
	if s.prefetcher != nil {
		s.prefetcher.close()
		s.prefetcher = nil
	}
}

// setError remembers the first non-nil error it is called with.
func (s *ChuKoNuStateDB) setError(err error) {
	if s.dbErr == nil {
		s.dbErr = err
	}
}

// Error returns the memorized database failure occurred earlier.
func (s *ChuKoNuStateDB) Error() error {
	return s.dbErr
}

func (s *ChuKoNuStateDB) AddLog(log *types.Log) {
	s.journal.append(chuKoNuAddLogChange{txhash: s.thash})

	log.TxHash = s.thash
	log.TxIndex = uint(s.txIndex)
	log.Index = s.logSize
	s.logs[s.thash] = append(s.logs[s.thash], log)
	s.logSize++
}

// GetLogs returns the logs matching the specified transaction hash, and annotates
// them with the given blockNumber and blockHash.
func (s *ChuKoNuStateDB) GetLogs(hash common.Hash, blockNumber uint64, blockHash common.Hash) []*types.Log {
	logs := s.logs[hash]
	for _, l := range logs {
		l.BlockNumber = blockNumber
		l.BlockHash = blockHash
	}
	return logs
}

func (s *ChuKoNuStateDB) Logs() []*types.Log {
	var logs []*types.Log
	for _, lgs := range s.logs {
		logs = append(logs, lgs...)
	}
	return logs
}

// AddPreimage records a SHA3 preimage seen by the VM.
func (s *ChuKoNuStateDB) AddPreimage(hash common.Hash, preimage []byte) {
	if _, ok := s.preimages[hash]; !ok {
		s.journal.append(chuKoNuAddPreimageChange{hash: hash})
		pi := make([]byte, len(preimage))
		copy(pi, preimage)
		s.preimages[hash] = pi
	}
}

// Preimages returns a list of SHA3 preimages that have been submitted.
func (s *ChuKoNuStateDB) Preimages() map[common.Hash][]byte {
	return s.preimages
}

// AddRefund adds gas to the refund counter
func (s *ChuKoNuStateDB) AddRefund(gas uint64) {
	s.journal.append(chuKoNuRefundChange{prev: s.refund})
	s.refund += gas
}

// SubRefund removes gas from the refund counter.
// This method will panic if the refund counter goes below zero
func (s *ChuKoNuStateDB) SubRefund(gas uint64) {
	s.journal.append(chuKoNuRefundChange{prev: s.refund})
	if gas > s.refund {
		panic(fmt.Sprintf("Refund counter below zero (gas: %d > refund: %d)", gas, s.refund))
	}
	s.refund -= gas
}

// Exist reports whether the given account address exists in the state.
// Notably this also returns true for suicided accounts.
func (s *ChuKoNuStateDB) Exist(addr common.Address) bool {
	addAccessAddr(s.accessAddress, addr, true, false)
	return s.getStateObject(addr) != nil
}

// Empty returns whether the state object is either non-existent
// or empty according to the EIP161 specification (balance = nonce = code = 0)
func (s *ChuKoNuStateDB) Empty(addr common.Address) bool {
	addAccessAddr(s.accessAddress, addr, true, false)
	so := s.getStateObject(addr)
	return so == nil || so.empty()
}

// GetBalance retrieves the balance from the given address or 0 if object not found
func (s *ChuKoNuStateDB) GetBalance(addr common.Address) *big.Int {
	addAccessAddr(s.accessAddress, addr, true, false)
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		return stateObject.Balance()
	}
	return common.Big0
}

func (s *ChuKoNuStateDB) GetNonce(addr common.Address) uint64 {
	addAccessAddr(s.accessAddress, addr, true, false)
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		return stateObject.Nonce()
	}

	return 0
}

// TxIndex returns the current transaction index set by Prepare.
func (s *ChuKoNuStateDB) TxIndex() int {
	return s.txIndex
}

func (s *ChuKoNuStateDB) GetCode(addr common.Address) []byte {
	addAccessAddr(s.accessAddress, addr, true, false)
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		return stateObject.Code(s.db)
	}
	return nil
}

func (s *ChuKoNuStateDB) GetCodeSize(addr common.Address) int {
	addAccessAddr(s.accessAddress, addr, true, false)
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		return stateObject.CodeSize(s.db)
	}
	return 0
}

func (s *ChuKoNuStateDB) GetCodeHash(addr common.Address) common.Hash {
	addAccessAddr(s.accessAddress, addr, true, false)
	stateObject := s.getStateObject(addr)
	if stateObject == nil {
		return common.Hash{}
	}
	return common.BytesToHash(stateObject.CodeHash())
}

// GetState retrieves a value from the given account's storage trie.
func (s *ChuKoNuStateDB) GetState(addr common.Address, hash common.Hash) common.Hash {
	addAccessSlot(s.accessAddress, addr, hash, true, s.txIndex)
	//if s.accessAddress != nil && s.thash == common.HexToHash("0xa9ee630fdade62b511ebfd21f222980ddc45aca09bb9a3c9128c23192b812013") &&
	//	addr == common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2") &&
	//	hash == common.HexToHash("0xdc51a0a44317b550a6b5ddb34687ce2bd40e4750eec27343e32b23e686739208") {
	//	fmt.Println((*(*s.accessAddress)[addr].Slots)[hash])
	//}
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		return stateObject.GetState(s.db, hash)
	}
	return common.Hash{}
}

// GetProof returns the Merkle proof for a given account.
func (s *ChuKoNuStateDB) GetProof(addr common.Address) ([][]byte, error) {
	return s.GetProofByHash(crypto.Keccak256Hash(addr.Bytes()))
}

// GetProofByHash returns the Merkle proof for a given account.
func (s *ChuKoNuStateDB) GetProofByHash(addrHash common.Hash) ([][]byte, error) {
	var proof proofList
	err := s.trie.Prove(addrHash[:], 0, &proof)
	return proof, err
}

// GetStorageProof returns the Merkle proof for given storage slot.
func (s *ChuKoNuStateDB) GetStorageProof(a common.Address, key common.Hash) ([][]byte, error) {
	trie, err := s.StorageTrie(a)
	if err != nil {
		return nil, err
	}
	if trie == nil {
		return nil, errors.New("storage trie for requested address does not exist")
	}
	var proof proofList
	err = trie.Prove(crypto.Keccak256(key.Bytes()), 0, &proof)
	if err != nil {
		return nil, err
	}
	return proof, nil
}

// GetCommittedState retrieves a value from the given account's committed storage trie.
func (s *ChuKoNuStateDB) GetCommittedState(addr common.Address, hash common.Hash) common.Hash {
	addAccessSlot(s.accessAddress, addr, hash, true, s.txIndex)
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		return stateObject.GetCommittedState(s.db, hash)
	}
	return common.Hash{}
}

// Database retrieves the low level database supporting the lower level trie ops.
func (s *ChuKoNuStateDB) Database() Database {
	return s.db
}

// StorageTrie returns the storage trie of an account. The return value is a copy
// and is nil for non-existent accounts. An error will be returned if storage trie
// is existent but can't be loaded correctly.
func (s *ChuKoNuStateDB) StorageTrie(addr common.Address) (Trie, error) {
	stateObject := s.getStateObject(addr)
	if stateObject == nil {
		return nil, nil
	}
	cpy := stateObject.deepCopy(s)
	if _, err := cpy.updateTrie(s.db); err != nil {
		return nil, err
	}
	return cpy.getTrie(s.db)
}

func (s *ChuKoNuStateDB) HasSuicided(addr common.Address) bool {
	addAccessAddr(s.accessAddress, addr, true, false)
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		return stateObject.suicided
	}
	return false
}

/*
 * SETTERS
 */

// AddBalance adds amount to the account associated with addr.
func (s *ChuKoNuStateDB) AddBalance(addr common.Address, amount *big.Int) {
	if amount.Cmp(common.Big0) != 0 {
		addAccessAddr(s.accessAddress, addr, false, false)
	}
	stateObject := s.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.AddBalance(amount)
	}
}

// SubBalance subtracts amount from the account associated with addr.
func (s *ChuKoNuStateDB) SubBalance(addr common.Address, amount *big.Int) {
	if amount.Cmp(common.Big0) != 0 {
		addAccessAddr(s.accessAddress, addr, false, false)
	}
	stateObject := s.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SubBalance(amount)
	}
}

func (s *ChuKoNuStateDB) SetBalance(addr common.Address, amount *big.Int) {
	stateObject := s.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetBalance(amount)
	}
}

func (s *ChuKoNuStateDB) SetNonce(addr common.Address, nonce uint64) {
	addAccessAddr(s.accessAddress, addr, false, false)
	stateObject := s.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetNonce(nonce)
	}
}

func (s *ChuKoNuStateDB) SetCode(addr common.Address, code []byte) {
	addAccessAddr(s.accessAddress, addr, false, false)
	stateObject := s.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetCode(crypto.Keccak256Hash(code), code)
	}
}

func (s *ChuKoNuStateDB) SetState(addr common.Address, key, value common.Hash) {
	addAccessSlot(s.accessAddress, addr, key, false, s.txIndex)
	//if s.accessAddress != nil && s.thash == common.HexToHash("0xa9ee630fdade62b511ebfd21f222980ddc45aca09bb9a3c9128c23192b812013") &&
	//	addr == common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2") &&
	//	key == common.HexToHash("0xdc51a0a44317b550a6b5ddb34687ce2bd40e4750eec27343e32b23e686739208") {
	//	fmt.Println((*(*s.accessAddress)[addr].Slots)[key])
	//}
	stateObject := s.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetState(s.db, key, value)
	}
}

// SetStorage replaces the entire storage for the specified account with given
// storage. This function should only be used for debugging.
func (s *ChuKoNuStateDB) SetStorage(addr common.Address, storage map[common.Hash]common.Hash) {
	// SetStorage needs to wipe existing storage. We achieve this by pretending
	// that the account self-destructed earlier in this block, by flagging
	// it in stateObjectsDestruct. The effect of doing so is that storage lookups
	// will not hit disk, since it is assumed that the disk-data is belonging
	// to a previous incarnation of the object.
	//fmt.Println("SetStorage")
	s.stateObjectsDestruct[addr] = struct{}{}
	stateObject := s.GetOrNewStateObject(addr)
	for k, v := range storage {
		stateObject.SetState(s.db, k, v)
	}
}

// Suicide marks the given account as suicided.
// This clears the account balance.
//
// The account's state object is still available until the state is committed,
// getStateObject will return a non-nil account after Suicide.
func (s *ChuKoNuStateDB) Suicide(addr common.Address) bool {
	addAccessAddr(s.accessAddress, addr, false, false)
	stateObject := s.getStateObject(addr)
	if stateObject == nil {
		return false
	}
	s.journal.append(chuKoNuSuicideChange{
		account:     &addr,
		prev:        stateObject.suicided,
		prevbalance: new(big.Int).Set(stateObject.Balance()),
	})
	stateObject.markSuicided()
	stateObject.data.Balance = new(big.Int)

	return true
}

// SetTransientState sets transient storage for a given account. It
// adds the change to the journal so that it can be rolled back
// to its previous value if there is a revert.
func (s *ChuKoNuStateDB) SetTransientState(addr common.Address, key, value common.Hash) {
	addAccessAddr(s.accessAddress, addr, true, false)
	prev := s.GetTransientState(addr, key)
	if prev == value {
		return
	}
	s.journal.append(chuKoNuTransientStorageChange{
		account:  &addr,
		key:      key,
		prevalue: prev,
	})
	s.setTransientState(addr, key, value)
}

// setTransientState is a lower level setter for transient storage. It
// is called during a revert to prevent modifications to the journal.
func (s *ChuKoNuStateDB) setTransientState(addr common.Address, key, value common.Hash) {
	s.transientStorage.Set(addr, key, value)
}

// GetTransientState gets transient storage for a given account.
func (s *ChuKoNuStateDB) GetTransientState(addr common.Address, key common.Hash) common.Hash {
	addAccessAddr(s.accessAddress, addr, true, false)
	return s.transientStorage.Get(addr, key)
}

//
// Setting, updating & deleting state object methods.
//

// updateStateObject writes the given object to the trie.
func (s *ChuKoNuStateDB) updateStateObject(obj *ChuKoNuStateObject) {
	// Track the amount of time wasted on updating the account from the trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { s.AccountUpdates += time.Since(start) }(time.Now())
	}
	// Encode the account and update the account trie
	addr := obj.Address()
	if err := s.trie.TryUpdateAccount(addr, &obj.data); err != nil {
		s.setError(fmt.Errorf("updateStateObject (%x) error: %v", addr[:], err))
	}

	// If state snapshotting is active, cache the data til commit. Note, this
	// update mechanism is not symmetric to the deletion, because whereas it is
	// enough to track account updates at commit time, deletions need tracking
	// at transaction boundary level to ensure we capture state clearing.
	if s.snap != nil {
		s.snapAccounts[obj.addrHash] = snapshot.SlimAccountRLP(obj.data.Nonce, obj.data.Balance, obj.data.Root, obj.data.CodeHash)
	}
}

// deleteStateObject removes the given object from the state trie.
func (s *ChuKoNuStateDB) deleteStateObject(obj *ChuKoNuStateObject) {
	// Track the amount of time wasted on deleting the account from the trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { s.AccountUpdates += time.Since(start) }(time.Now())
	}
	// Delete the account from the trie
	addr := obj.Address()
	if err := s.trie.TryDeleteAccount(addr); err != nil {
		s.setError(fmt.Errorf("deleteStateObject (%x) error: %v", addr[:], err))
	}
}

// getStateObject retrieves a state object given by the address, returning nil if
// the object is not found or was deleted in this execution context. If you need
// to differentiate between non-existent/just-deleted, use getDeletedStateObject.
func (s *ChuKoNuStateDB) getStateObject(addr common.Address) *ChuKoNuStateObject {
	if obj := s.getDeletedStateObject(addr); obj != nil && !obj.deleted {
		return obj
	}
	return nil
}

// getDeletedStateObject is similar to getStateObject, but instead of returning
// nil for a deleted state object, it returns the actual object with the deleted
// flag set. This is needed by the state journal to revert to the correct s-
// destructed object instead of wiping all knowledge about the state object.
func (s *ChuKoNuStateDB) getDeletedStateObject(addr common.Address) *ChuKoNuStateObject {
	// Prefer live objects if any is available
	if obj := s.stateObjects[addr]; obj != nil {
		s.setTxStateObjects(obj)
		return obj
	}
	// If no live objects are available, attempt to use snapshots
	var data *types.StateAccount
	if s.snap != nil {
		start := time.Now()
		// origin -> acc, err := s.snap.Account(crypto.HashData(s.hasher, addr.Bytes()))
		acc, err := s.snap.Account(crypto.HashData(crypto.NewKeccakState(), addr.Bytes()))
		if metrics.EnabledExpensive {
			s.SnapshotAccountReads += time.Since(start)
		}
		if err == nil {
			if acc == nil {
				return nil
			}
			data = &types.StateAccount{
				Nonce:    acc.Nonce,
				Balance:  acc.Balance,
				CodeHash: acc.CodeHash,
				Root:     common.BytesToHash(acc.Root),
			}
			if len(data.CodeHash) == 0 {
				data.CodeHash = types.EmptyCodeHash.Bytes()
			}
			if data.Root == (common.Hash{}) {
				data.Root = types.EmptyRootHash
			}
		}
	}
	// If snapshot unavailable or reading from it failed, load from the database
	if data == nil {
		start := time.Now()
		var err error
		data, err = s.trie.TryGetAccount(addr)
		if metrics.EnabledExpensive {
			s.AccountReads += time.Since(start)
		}
		if err != nil {
			s.setError(fmt.Errorf("getDeleteStateObject (%x) error: %w", addr.Bytes(), err))
			return nil
		}
		if data == nil {
			obj := newChuKoNuObject(s, addr, types.StateAccount{}, true)
			s.setTxStateObjects(obj)
			s.setStateObject(obj)
			return nil
		}
	}
	// Insert into the live set
	obj := newChuKoNuObject(s, addr, *data, true)
	s.setTxStateObjects(obj)
	s.setStateObject(obj)
	return obj
}

func (s *ChuKoNuStateDB) setStateObject(object *ChuKoNuStateObject) {
	s.stateObjects[object.Address()] = object
}

// GetOrNewStateObject retrieves a state object or create a new state object if nil.
func (s *ChuKoNuStateDB) GetOrNewStateObject(addr common.Address) *ChuKoNuStateObject {
	stateObject := s.getStateObject(addr)
	if stateObject == nil {
		stateObject, _ = s.createObject(addr)
	}
	return stateObject
}

// createObject creates a new state object. If there is an existing account with
// the given address, it is overwritten and returned as the second return value.
func (s *ChuKoNuStateDB) createObject(addr common.Address) (newobj, prev *ChuKoNuStateObject) {
	prev = s.getDeletedStateObject(addr) // Note, prev might have been deleted, we need that!

	var prevdestruct bool
	if prev != nil {
		_, prevdestruct = s.stateObjectsDestruct[prev.address]
		if !prevdestruct {
			s.stateObjectsDestruct[prev.address] = struct{}{}
		}
	}
	newobj = newChuKoNuObject(s, addr, types.StateAccount{}, false)
	if prev == nil {
		s.journal.append(chuKoNuCreateObjectChange{account: &addr})
	} else {
		s.journal.append(chuKoNuResetObjectChange{prev: prev, prevdestruct: prevdestruct})
	}
	s.setTxStateObjects(newobj)
	s.setStateObject(newobj)
	if prev != nil {
		newobj.originData = prev.originData
		newobj.originCode = prev.originCode
		newobj.originStableStorage = prev.originStableStorage.Copy()
		if !prev.deleted {
			return newobj, prev
		}
	}
	return newobj, nil
}

// CreateAccount explicitly creates a state object. If a state object with the address
// already exists the balance is carried over to the new account.
//
// CreateAccount is called during the EVM CREATE operation. The situation might arise that
// a contract does the following:
//
//  1. sends funds to sha(account ++ (nonce + 1))
//  2. tx_create(sha(account ++ nonce)) (note that this gets the address of 1)
//
// Carrying over the balance ensures that Ether doesn't disappear.
func (s *ChuKoNuStateDB) CreateAccount(addr common.Address) {
	addAccessAddr(s.accessAddress, addr, true, false)
	newObj, prev := s.createObject(addr)
	if prev != nil {
		newObj.setBalance(prev.data.Balance)
	}
}

func (db *ChuKoNuStateDB) ForEachStorage(addr common.Address, cb func(key, value common.Hash) bool) error {
	so := db.getStateObject(addr)
	if so == nil {
		return nil
	}
	tr, err := so.getTrie(db.db)
	if err != nil {
		return err
	}
	it := trie.NewIterator(tr.NodeIterator(nil))

	for it.Next() {
		key := common.BytesToHash(db.trie.GetKey(it.Key))
		if value, dirty := so.dirtyStorage[key]; dirty {
			if !cb(key, value) {
				return nil
			}
			continue
		}

		if len(it.Value) > 0 {
			_, content, _, err := rlp.Split(it.Value)
			if err != nil {
				return err
			}
			if !cb(key, common.BytesToHash(content)) {
				return nil
			}
		}
	}
	return nil
}

func (s *ChuKoNuStateDB) Trie() Trie {
	return s.trie
}

// Copy creates a deep, independent copy of the state.
// Snapshots of the copied state cannot be applied to the copy.
func (s *ChuKoNuStateDB) Copy() *ChuKoNuStateDB {
	// Copy all the basic fields, initialize the memory ones
	state := &ChuKoNuStateDB{
		db:                   s.db,
		trie:                 s.db.CopyTrie(s.trie),
		originalRoot:         s.originalRoot,
		stateObjects:         make(map[common.Address]*ChuKoNuStateObject, len(s.journal.dirties)),
		stateObjectsPending:  make(map[common.Address]struct{}, len(s.stateObjectsPending)),
		stateObjectsDirty:    make(map[common.Address]struct{}, len(s.journal.dirties)),
		stateObjectsDestruct: make(map[common.Address]struct{}, len(s.stateObjectsDestruct)),
		refund:               s.refund,
		logs:                 make(map[common.Hash][]*types.Log, len(s.logs)),
		logSize:              s.logSize,
		preimages:            make(map[common.Hash][]byte, len(s.preimages)),
		journal:              newChuKoNuJournal(),
		//hasher:               crypto.NewKeccakState(),
	}
	// Copy the dirty states, logs, and preimages
	for addr := range s.journal.dirties {
		// As documented [here](https://github.com/ethereum/go-ethereum/pull/16485#issuecomment-380438527),
		// and in the Finalise-method, there is a case where an object is in the journal but not
		// in the stateObjects: OOG after touch on ripeMD prior to Byzantium. Thus, we need to check for
		// nil
		if object, exist := s.stateObjects[addr]; exist {
			// Even though the original object is dirty, we are not copying the journal,
			// so we need to make sure that any side-effect the journal would have caused
			// during a commit (or similar op) is already applied to the copy.
			state.stateObjects[addr] = object.deepCopy(state)

			state.stateObjectsDirty[addr] = struct{}{}   // Mark the copy dirty to force internal (code/state) commits
			state.stateObjectsPending[addr] = struct{}{} // Mark the copy pending to force external (account) commits
		}
	}
	// Above, we don't copy the actual journal. This means that if the copy
	// is copied, the loop above will be a no-op, since the copy's journal
	// is empty. Thus, here we iterate over stateObjects, to enable copies
	// of copies.
	for addr := range s.stateObjectsPending {
		if _, exist := state.stateObjects[addr]; !exist {
			state.stateObjects[addr] = s.stateObjects[addr].deepCopy(state)
		}
		state.stateObjectsPending[addr] = struct{}{}
	}
	for addr := range s.stateObjectsDirty {
		if _, exist := state.stateObjects[addr]; !exist {
			state.stateObjects[addr] = s.stateObjects[addr].deepCopy(state)
		}
		state.stateObjectsDirty[addr] = struct{}{}
	}
	// Deep copy the destruction flag.
	for addr := range s.stateObjectsDestruct {
		state.stateObjectsDestruct[addr] = struct{}{}
	}
	for hash, logs := range s.logs {
		cpy := make([]*types.Log, len(logs))
		for i, l := range logs {
			cpy[i] = new(types.Log)
			*cpy[i] = *l
		}
		state.logs[hash] = cpy
	}
	for hash, preimage := range s.preimages {
		state.preimages[hash] = preimage
	}
	// Do we need to copy the access list and transient storage?
	// In practice: No. At the start of a transaction, these two lists are empty.
	// In practice, we only ever copy state _between_ transactions/blocks, never
	// in the middle of a transaction. However, it doesn't cost us much to copy
	// empty lists, so we do it anyway to not blow up if we ever decide copy them
	// in the middle of a transaction.
	state.accessList = s.accessList.Copy()
	state.transientStorage = s.transientStorage.Copy()

	// If there's a prefetcher running, make an inactive copy of it that can
	// only access data but does not actively preload (since the user will not
	// know that they need to explicitly terminate an active copy).
	if s.prefetcher != nil {
		state.prefetcher = s.prefetcher.copy()
	}
	if s.snaps != nil {
		// In order for the miner to be able to use and make additions
		// to the snapshot tree, we need to copy that as well.
		// Otherwise, any block mined by ourselves will cause gaps in the tree,
		// and force the miner to operate trie-backed only
		state.snaps = s.snaps
		state.snap = s.snap

		// deep copy needed
		state.snapAccounts = make(map[common.Hash][]byte)
		for k, v := range s.snapAccounts {
			state.snapAccounts[k] = v
		}
		state.snapStorage = make(map[common.Hash]map[common.Hash][]byte)
		for k, v := range s.snapStorage {
			temp := make(map[common.Hash][]byte)
			for kk, vv := range v {
				temp[kk] = vv
			}
			state.snapStorage[k] = temp
		}
	}
	return state
}

// Snapshot returns an identifier for the current revision of the state.
func (s *ChuKoNuStateDB) Snapshot() int {
	id := s.nextRevisionId
	s.nextRevisionId++
	s.validRevisions = append(s.validRevisions, revision{id, s.journal.length()})
	return id
}

// RevertToSnapshot reverts all state changes made since the given revision.
func (s *ChuKoNuStateDB) RevertToSnapshot(revid int) {
	// Find the snapshot in the stack of valid snapshots.
	idx := sort.Search(len(s.validRevisions), func(i int) bool {
		return s.validRevisions[i].id >= revid
	})
	if idx == len(s.validRevisions) || s.validRevisions[idx].id != revid {
		panic(fmt.Errorf("revision id %v cannot be reverted", revid))
	}
	snapshot := s.validRevisions[idx].journalIndex

	// Replay the journal to undo changes and remove invalidated snapshots
	s.journal.revert(s, snapshot)
	s.validRevisions = s.validRevisions[:idx]
}

// GetRefund returns the current value of the refund counter.
func (s *ChuKoNuStateDB) GetRefund() uint64 {
	return s.refund
}

// Finalise finalises the state by removing the destructed objects and clears
// the journal as well as the refunds. Finalise, however, will not push any updates
// into the tries just yet. Only IntermediateRoot or Commit will do that.
func (s *ChuKoNuStateDB) Finalise(deleteEmptyObjects bool) {
	addressesToPrefetch := make([][]byte, 0, len(s.journal.dirties))
	for addr := range s.journal.dirties {
		obj, exist := s.stateObjects[addr]
		if !exist {
			// ripeMD is 'touched' at block 1714175, in tx 0x1237f737031e40bcde4a8b7e717b2d15e3ecadfe49bb1bbc71ee9deb09c6fcf2
			// That tx goes out of gas, and although the notion of 'touched' does not exist there, the
			// touch-event will still be recorded in the journal. Since ripeMD is a special snowflake,
			// it will persist in the journal even though the journal is reverted. In this special circumstance,
			// it may exist in `s.journal.dirties` but not in `s.stateObjects`.
			// Thus, we can safely ignore it here
			continue
		}
		if obj.suicided || (deleteEmptyObjects && obj.empty()) {
			obj.deleted = true

			// We need to maintain account deletions explicitly (will remain
			// set indefinitely).
			s.stateObjectsDestruct[obj.address] = struct{}{}

			// If state snapshotting is active, also mark the destruction there.
			// Note, we can't do this only at the end of a block because multiple
			// transactions within the same block might self destruct and then
			// resurrect an account; but the snapshotter needs both events.
			if s.snap != nil {
				delete(s.snapAccounts, obj.addrHash) // Clear out any previously updated account data (may be recreated via a resurrect)
				delete(s.snapStorage, obj.addrHash)  // Clear out any previously updated storage data (may be recreated via a resurrect)
			}
		} else {
			s.deleteAccessSlot(obj)
			obj.finalise(true, s.txIndex) // Prefetch slots in the background
		}
		s.stateObjectsPending[addr] = struct{}{}
		s.stateObjectsDirty[addr] = struct{}{}

		// At this point, also ship the address off to the precacher. The precacher
		// will start loading tries, and when the change is eventually committed,
		// the commit-phase will be a lot faster
		addressesToPrefetch = append(addressesToPrefetch, common.CopyBytes(addr[:])) // Copy needed for closure

		// 判断obj是否和其原始状态相同
		s.deleteAccessAddress(obj)
	}
	if s.prefetcher != nil && len(addressesToPrefetch) > 0 {
		s.prefetcher.prefetch(common.Hash{}, s.originalRoot, addressesToPrefetch)
	}
	// Invalidate journal because reverting across transactions is not allowed.
	s.clearJournalAndRefund()
}

// IntermediateRoot computes the current root hash of the state trie.
// It is called in between transactions to get the root hash that
// goes into transaction receipts.
func (s *ChuKoNuStateDB) IntermediateRoot(deleteEmptyObjects bool) common.Hash {
	// Finalise all the dirty storage states and write them into the tries
	s.Finalise(deleteEmptyObjects)

	// If there was a trie prefetcher operating, it gets aborted and irrevocably
	// modified after we start retrieving tries. Remove it from the statedb after
	// this round of use.
	//
	// This is weird pre-byzantium since the first tx runs with a prefetcher and
	// the remainder without, but pre-byzantium even the initial prefetcher is
	// useless, so no sleep lost.
	prefetcher := s.prefetcher
	if s.prefetcher != nil {
		defer func() {
			s.prefetcher.close()
			s.prefetcher = nil
		}()
	}
	// Although naively it makes sense to retrieve the account trie and then do
	// the contract storage and account updates sequentially, that short circuits
	// the account prefetcher. Instead, let's process all the storage updates
	// first, giving the account prefetches just a few more milliseconds of time
	// to pull useful data from disk.
	for addr := range s.stateObjectsPending {
		if obj := s.stateObjects[addr]; !obj.deleted {
			obj.updateRoot(s.db) // update storage
		}
	}
	// Now we're about to start to write changes to the trie. The trie is so far
	// _untouched_. We can check with the prefetcher, if it can give us a trie
	// which has the same root, but also has some content loaded into it.
	if prefetcher != nil {
		if trie := prefetcher.trie(common.Hash{}, s.originalRoot); trie != nil {
			s.trie = trie
		}
	}
	usedAddrs := make([][]byte, 0, len(s.stateObjectsPending))
	for addr := range s.stateObjectsPending {
		if obj := s.stateObjects[addr]; obj.deleted {
			s.deleteStateObject(obj)
			s.AccountDeleted += 1
		} else {
			s.updateStateObject(obj) // update account
			s.AccountUpdated += 1
		}
		usedAddrs = append(usedAddrs, common.CopyBytes(addr[:])) // Copy needed for closure
	}
	if prefetcher != nil {
		prefetcher.used(common.Hash{}, s.originalRoot, usedAddrs)
	}
	if len(s.stateObjectsPending) > 0 {
		s.stateObjectsPending = make(map[common.Address]struct{})
	}
	// Track the amount of time wasted on hashing the account trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { s.AccountHashes += time.Since(start) }(time.Now())
	}
	return s.trie.Hash()
}

// SetTxContext sets the current transaction hash and index which are
// used when the EVM emits new state logs. It should be invoked before
// transaction execution.
func (s *ChuKoNuStateDB) SetTxContext(thash common.Hash, ti int, simulation bool, detectStateChange bool) {
	//fmt.Println(s)
	s.thash = thash
	s.txIndex = ti
	s.accessAddress = nil
	s.detectStateChange = detectStateChange
	if simulation {
		s.accessAddress = types.NewAccessAddressMap()
	}
	if detectStateChange {
		s.txStateObjects = make(map[common.Address]*ChuKoNuStateObject)
	}
}

func (s *ChuKoNuStateDB) clearJournalAndRefund() {
	if len(s.journal.entries) > 0 {
		s.journal = newChuKoNuJournal()
		s.refund = 0
	}
	s.validRevisions = s.validRevisions[:0] // Snapshots can be created without journal entries
}

// Commit writes the state to the underlying in-memory trie database.
func (s *ChuKoNuStateDB) Commit(deleteEmptyObjects bool) (common.Hash, error) {
	// Short circuit in case any database failure occurred earlier.
	if s.dbErr != nil {
		return common.Hash{}, fmt.Errorf("commit aborted due to earlier error: %v", s.dbErr)
	}
	// Finalize any pending changes and merge everything into the tries
	s.IntermediateRoot(deleteEmptyObjects)

	// Commit objects to the trie, measuring the elapsed time
	var (
		accountTrieNodesUpdated int
		accountTrieNodesDeleted int
		storageTrieNodesUpdated int
		storageTrieNodesDeleted int
		nodes                   = trie.NewMergedNodeSet()
		codeWriter              = s.db.DiskDB().NewBatch()
	)
	for addr := range s.stateObjectsDirty {
		if obj := s.stateObjects[addr]; !obj.deleted {
			// Write any contract code associated with the state object
			if obj.code != nil && obj.dirtyCode {
				rawdb.WriteCode(codeWriter, common.BytesToHash(obj.CodeHash()), obj.code)
				obj.dirtyCode = false
			}
			// Write any storage changes in the state object to its storage trie
			set, err := obj.commitTrie(s.db)
			if err != nil {
				return common.Hash{}, err
			}
			// Merge the dirty nodes of storage trie into global set
			if set != nil {
				if err := nodes.Merge(set); err != nil {
					return common.Hash{}, err
				}
				updates, deleted := set.Size()
				storageTrieNodesUpdated += updates
				storageTrieNodesDeleted += deleted
			}
		}
		// If the contract is destructed, the storage is still left in the
		// database as dangling data. Theoretically it's should be wiped from
		// database as well, but in hash-based-scheme it's extremely hard to
		// determine that if the trie nodes are also referenced by other storage,
		// and in path-based-scheme some technical challenges are still unsolved.
		// Although it won't affect the correctness but please fix it TODO(rjl493456442).
	}
	if len(s.stateObjectsDirty) > 0 {
		s.stateObjectsDirty = make(map[common.Address]struct{})
	}
	if codeWriter.ValueSize() > 0 {
		if err := codeWriter.Write(); err != nil {
			log.Crit("Failed to commit dirty codes", "error", err)
		}
	}
	// Write the account trie changes, measuring the amount of wasted time
	var start time.Time
	if metrics.EnabledExpensive {
		start = time.Now()
	}
	root, set := s.trie.Commit(true)
	// Merge the dirty nodes of account trie into global set
	if set != nil {
		if err := nodes.Merge(set); err != nil {
			return common.Hash{}, err
		}
		accountTrieNodesUpdated, accountTrieNodesDeleted = set.Size()
	}
	if metrics.EnabledExpensive {
		s.AccountCommits += time.Since(start)

		accountUpdatedMeter.Mark(int64(s.AccountUpdated))
		storageUpdatedMeter.Mark(int64(s.StorageUpdated))
		accountDeletedMeter.Mark(int64(s.AccountDeleted))
		storageDeletedMeter.Mark(int64(s.StorageDeleted))
		accountTrieUpdatedMeter.Mark(int64(accountTrieNodesUpdated))
		accountTrieDeletedMeter.Mark(int64(accountTrieNodesDeleted))
		storageTriesUpdatedMeter.Mark(int64(storageTrieNodesUpdated))
		storageTriesDeletedMeter.Mark(int64(storageTrieNodesDeleted))
		s.AccountUpdated, s.AccountDeleted = 0, 0
		s.StorageUpdated, s.StorageDeleted = 0, 0
	}
	// If snapshotting is enabled, update the snapshot tree with this new version
	if s.snap != nil {
		start := time.Now()
		// Only update if there's a state transition (skip empty Clique blocks)
		if parent := s.snap.Root(); parent != root {
			if err := s.snaps.Update(root, parent, s.convertAccountSet(s.stateObjectsDestruct), s.snapAccounts, s.snapStorage); err != nil {
				log.Warn("Failed to update snapshot tree", "from", parent, "to", root, "err", err)
			}
			// Keep 128 diff layers in the memory, persistent layer is 129th.
			// - head layer is paired with HEAD state
			// - head-1 layer is paired with HEAD-1 state
			// - head-127 layer(bottom-most diff layer) is paired with HEAD-127 state
			if err := s.snaps.Cap(root, 128); err != nil {
				log.Warn("Failed to cap snapshot tree", "root", root, "layers", 128, "err", err)
			}
		}
		if metrics.EnabledExpensive {
			s.SnapshotCommits += time.Since(start)
		}
		s.snap, s.snapAccounts, s.snapStorage = nil, nil, nil
	}
	if len(s.stateObjectsDestruct) > 0 {
		s.stateObjectsDestruct = make(map[common.Address]struct{})
	}
	if root == (common.Hash{}) {
		root = types.EmptyRootHash
	}
	origin := s.originalRoot
	if origin == (common.Hash{}) {
		origin = types.EmptyRootHash
	}
	if root != origin {
		start := time.Now()
		if err := s.db.TrieDB().Update(nodes); err != nil {
			return common.Hash{}, err
		}
		s.originalRoot = root
		if metrics.EnabledExpensive {
			s.TrieDBCommits += time.Since(start)
		}
	}
	return root, nil
}

// Prepare handles the preparatory steps for executing a state transition with.
// This method must be invoked before state transition.
//
// Berlin fork:
// - Add sender to access list (2929)
// - Add destination to access list (2929)
// - Add precompiles to access list (2929)
// - Add the contents of the optional tx access list (2930)
//
// Potential EIPs:
// - Reset access list (Berlin)
// - Add coinbase to access list (EIP-3651)
// - Reset transient storage (EIP-1153)
func (s *ChuKoNuStateDB) Prepare(rules params.Rules, sender, coinbase common.Address, dst *common.Address, precompiles []common.Address, list types.AccessList) {
	if rules.IsBerlin {
		// Clear out any leftover from previous executions
		al := newAccessList()
		s.accessList = al

		al.AddAddress(sender)
		if dst != nil {
			al.AddAddress(*dst)
			// If it's a create-tx, the destination will be added inside evm.create
		}
		for _, addr := range precompiles {
			al.AddAddress(addr)
		}
		for _, el := range list {
			al.AddAddress(el.Address)
			for _, key := range el.StorageKeys {
				al.AddSlot(el.Address, key)
			}
		}
		if rules.IsShanghai { // EIP-3651: warm coinbase
			al.AddAddress(coinbase)
		}
	}
	// Reset transient storage at the beginning of transaction execution
	s.transientStorage = newTransientStorage()
}

// AddAddressToAccessList adds the given address to the access list
func (s *ChuKoNuStateDB) AddAddressToAccessList(addr common.Address) {
	if s.accessList.AddAddress(addr) {
		s.journal.append(chuKoNuAccessListAddAccountChange{&addr})
	}
}

// AddSlotToAccessList adds the given (address, slot)-tuple to the access list
func (s *ChuKoNuStateDB) AddSlotToAccessList(addr common.Address, slot common.Hash) {
	addrMod, slotMod := s.accessList.AddSlot(addr, slot)
	if addrMod {
		// In practice, this should not happen, since there is no way to enter the
		// scope of 'address' without having the 'address' become already added
		// to the access list (via call-variant, create, etc).
		// Better safe than sorry, though
		s.journal.append(chuKoNuAccessListAddAccountChange{&addr})
	}
	if slotMod {
		s.journal.append(chuKoNuAccessListAddSlotChange{
			address: &addr,
			slot:    &slot,
		})
	}
}

// AddressInAccessList returns true if the given address is in the access list.
func (s *ChuKoNuStateDB) AddressInAccessList(addr common.Address) bool {
	return s.accessList.ContainsAddress(addr)
}

// SlotInAccessList returns true if the given (address, slot)-tuple is in the access list.
func (s *ChuKoNuStateDB) SlotInAccessList(addr common.Address, slot common.Hash) (addressPresent bool, slotPresent bool) {
	return s.accessList.Contains(addr, slot)
}

// convertAccountSet converts a provided account set from address keyed to hash keyed.
func (s *ChuKoNuStateDB) convertAccountSet(set map[common.Address]struct{}) map[common.Hash]struct{} {
	ret := make(map[common.Hash]struct{})
	for addr := range set {
		obj, exist := s.stateObjects[addr]
		if !exist {
			ret[crypto.Keccak256Hash(addr[:])] = struct{}{}
		} else {
			ret[obj.addrHash] = struct{}{}
		}
	}
	return ret
}

func (s *ChuKoNuStateDB) AccessAddress() *types.AccessAddressMap {
	return s.accessAddress
}

func (s *ChuKoNuStateDB) StateObjects() {

}

func (s *ChuKoNuStateDB) appendJournal(journal *chuKoNuJournal, entry chuKoNuJournalEntry) {

}

func (s *ChuKoNuStateDB) setOriginStateObject(object *ChuKoNuStateObject) {
	s.objectMutex.Lock()
	s.stateObjects[object.Address()] = object
	s.objectMutex.Unlock()
}

func (s *ChuKoNuStateDB) getOriginStateObject(addr common.Address) *ChuKoNuStateObject {
	// Prefer live objects if any is available
	s.objectMutex.RLock()
	obj := s.stateObjects[addr]
	s.objectMutex.RUnlock()
	if obj != nil {
		return obj
	}
	// If no live objects are available, attempt to use snapshots
	var data *types.StateAccount
	if s.snap != nil {
		start := time.Now()
		// origin -> acc, err := s.snap.Account(crypto.HashData(s.hasher, addr.Bytes()))
		acc, err := s.snap.Account(crypto.HashData(crypto.NewKeccakState(), addr.Bytes()))
		if metrics.EnabledExpensive {
			s.SnapshotAccountReads += time.Since(start)
		}
		if err == nil {
			if acc == nil {
				return nil
			}
			data = &types.StateAccount{
				Nonce:    acc.Nonce,
				Balance:  acc.Balance,
				CodeHash: acc.CodeHash,
				Root:     common.BytesToHash(acc.Root),
			}
			if len(data.CodeHash) == 0 {
				data.CodeHash = types.EmptyCodeHash.Bytes()
			}
			if data.Root == (common.Hash{}) {
				data.Root = types.EmptyRootHash
			}
		}
	}
	// If snapshot unavailable or reading from it failed, load from the database
	if data == nil {
		start := time.Now()
		var err error
		s.trieMutex.Lock()
		data, err = s.trie.TryGetAccount(addr)
		s.trieMutex.Unlock()
		if metrics.EnabledExpensive {
			s.AccountReads += time.Since(start)
		}
		if err != nil {
			s.setError(fmt.Errorf("getDeleteStateObject (%x) error: %w", addr.Bytes(), err))
			return nil
		}
		if data == nil {
			data = &types.StateAccount{}
		}
	}
	// Insert into the live set
	obj = newChuKoNuObject(s, addr, *data, true)
	s.setOriginStateObject(obj)
	return obj
}

// GetOriginBalance retrieves the balance from the given address or 0 if object not found
func (s *ChuKoNuStateDB) GetOriginBalance(addr common.Address) *big.Int {
	obj := s.getOriginStateObject(addr)
	return obj.OriginBalance()
}

func (s *ChuKoNuStateDB) GetOriginNonce(addr common.Address) uint64 {
	obj := s.getOriginStateObject(addr)
	return obj.OriginNonce()
}

func (s *ChuKoNuStateDB) GetOriginCodeHash(addr common.Address) common.Hash {
	obj := s.getOriginStateObject(addr)
	return common.BytesToHash(obj.OriginCodeHash())
}

// GetOriginState retrieves a value from the given account's storage trie.
func (s *ChuKoNuStateDB) GetOriginState(addr common.Address, hash common.Hash) common.Hash {
	obj := s.getOriginStateObject(addr)
	return obj.OriginState(s.db, s, hash)
}

func (s *ChuKoNuStateDB) GetOriginCode(addr common.Address) []byte {
	obj := s.getOriginStateObject(addr)
	return obj.OriginCode(s.db)
}

func (s *ChuKoNuStateDB) GetOriginCodeSize(addr common.Address) int {
	obj := s.getOriginStateObject(addr)
	code := obj.OriginCode(s.db)
	if code != nil {
		return len(code)
	}
	return 0
}

func (s *ChuKoNuStateDB) GetOriginStateAccount(addr common.Address) *types.StateAccount {
	obj := s.getOriginStateObject(addr)
	if obj.originData == nil {
		return &types.StateAccount{}
	}
	return obj.originData.Copy()
}

func (s *ChuKoNuStateDB) CreateAccountObject(addr common.Address) *ChuKoNuStateObject {
	if obj := s.getOriginStateObject(addr); obj != nil { // 考虑每个区块构建一个statedb, 无需判断obj.deleted
		accountObj := obj.deepCopy(s)

		// 修改最新状态与初始状态一致
		if accountObj.originData == nil {
			accountObj.data.Balance = new(big.Int)
			accountObj.data.CodeHash = types.EmptyCodeHash.Bytes()
			accountObj.data.Root = types.EmptyRootHash
		} else {
			accountObj.data.Nonce = accountObj.originData.Nonce
			accountObj.data.Balance = new(big.Int).Set(accountObj.originData.Balance)
			accountObj.data.CodeHash = common.CopyBytes(accountObj.originData.CodeHash)
			accountObj.data.Root = accountObj.originData.Root
		}
		// 修改 code 状态
		accountObj.code = accountObj.originCode
		accountObj.dirty = false
		accountObj.dirtyCode = false
		accountObj.suicided = false

		accountObj.originStableStorage = make(Storage)
		accountObj.originStorage = make(Storage)
		accountObj.pendingStorage = make(Storage)
		accountObj.dirtyStorage = make(Storage)
		return accountObj
	}
	return newChuKoNuObject(s, addr, types.StateAccount{}, true)
}

// UpdateByAccountObject 事务结束之后, 更新statedb
func (s *ChuKoNuStateDB) UpdateByAccountObject(accObj *ChuKoNuStateObject) {
	var obj *ChuKoNuStateObject = s.getOriginStateObject(accObj.address)

	if obj == nil || obj.deleted {
		obj = newChuKoNuObject(s, accObj.address, types.StateAccount{}, true)
		s.objectMutex.Lock()
		s.setStateObject(obj)
		s.objectMutex.Unlock()
	}
	if accObj.dirty {
		//s.stateObjectsDirtyMutex.Lock()
		//s.stateObjectsDirty[obj.address] = struct{}{}
		//s.stateObjectsPending[obj.address] = struct{}{}
		//s.stateObjectsDirtyMutex.Unlock()

		obj.data.Nonce = accObj.data.Nonce
		obj.data.Balance = new(big.Int).Set(accObj.data.Balance)
		obj.setCode(crypto.Keccak256Hash(accObj.code), accObj.code)
		obj.deleted = accObj.deleted
	}
	if obj.deleted {
		//s.stateObjectsDirtyMutex.Lock()
		//s.stateObjectsDestruct[obj.address] = struct{}{}
		//s.stateObjectsPending[obj.address] = struct{}{}
		//s.stateObjectsDirtyMutex.Unlock()
	}
	for key, val := range accObj.dirtyStorage {
		obj.dirtyStorage[key] = val
	}

	//obj.updateRoot(s.db)
}

func (s *ChuKoNuStateDB) setTxStateObjects(obj *ChuKoNuStateObject) {
	if s.txStateObjects == nil {
		return
	}
	if _, ok := s.txStateObjects[obj.address]; !ok {
		s.txStateObjects[obj.address] = obj.copyTxObject(s) // 第一次出现的读为初始值
	}
}

func (s *ChuKoNuStateDB) setTxStorage(obj *ChuKoNuStateObject, key, value common.Hash) {
	if s.txStateObjects == nil {
		return
	}
	if _, ok := s.txStateObjects[obj.address]; !ok {
		s.txStateObjects[obj.address] = obj.copyTxObject(s) // 第一次出现的读为初始值
	}
	s.txStateObjects[obj.address].txStorage[key] = value
}

func (s *ChuKoNuStateDB) deleteAccessAddress(obj *ChuKoNuStateObject) {
	if s.txStateObjects == nil {
		return
	}
	txObj := s.txStateObjects[obj.address]
	if txObj == nil {
		return
	}
	if txObj.data.Nonce != obj.data.Nonce || txObj.data.Balance.Cmp(obj.data.Balance) != 0 || !bytes.Equal(txObj.data.CodeHash, obj.originData.CodeHash) {
		// 修改了
		return
	}
	if txObj.deleted != obj.deleted || txObj.dirtyCode != obj.dirtyCode || txObj.suicided != obj.suicided {
		return
	}

	accessAddr, ok := (*s.accessAddress)[obj.address]
	if !ok {
		fmt.Println("Address was been accessed, but not in r/w set")
		return
	}
	//fmt.Println("Remove address from r/w set", s.thash, obj.address)
	accessAddr.IsRead = true
	accessAddr.IsWrite = false
	//accessAddr.CoarseRead = true
	//accessAddr.CoarseWrite = false

	(*s.accessAddress)[obj.address] = accessAddr
}

// addAccessSlot 将交易所访问的存储槽进行记录
func (s *ChuKoNuStateDB) deleteAccessSlot(obj *ChuKoNuStateObject) {
	if s.txStateObjects == nil {
		return
	}
	txObj := s.txStateObjects[obj.address]
	if txObj == nil {
		return
	}
	accessAddr, ok := (*s.accessAddress)[obj.address]
	if !ok {
		fmt.Println("Address was been accessed, but not in r/w set")
		return
	}
	for key, value := range obj.dirtyStorage {
		if txObj.txStorage[key] == value {
			accessSlot, ok := (*accessAddr.Slots)[key]
			if !ok {
				accessSlot = types.NewAccessSlot()
			}
			//fmt.Println("Remove slot from r/w set", s.thash, obj.address, key)
			accessSlot.IsRead = true
			accessSlot.IsWrite = false
			(*accessAddr.Slots)[key] = accessSlot
		}
	}
}
