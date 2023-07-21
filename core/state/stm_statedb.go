package state

import (
	"bytes"
	"chukonu/core/rawdb"
	"chukonu/core/state/snapshot"
	"chukonu/core/types"
	"chukonu/trie"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"math/big"
	"sort"
	"strings"
	"sync"
	"time"
)

// StmStateDB structs within the ethereum protocol are used to store anything
// within the merkle trie. StateDBs take care of caching and storing
// nested states. It's the general query interface to retrieve:
// * Contracts
// * Accounts
type StmStateDB struct {
	db         Database
	prefetcher *triePrefetcher
	trie       Trie
	hashMutex  sync.Mutex
	hasher     crypto.KeccakState

	// originalRoot is the pre-state root, before any changes were made.
	// It will be updated when the Commit is called.
	originalRoot common.Hash

	snaps        *snapshot.Tree
	snap         snapshot.Snapshot
	snapAccounts map[common.Hash][]byte
	snapStorage  map[common.Hash]map[common.Hash][]byte

	// This map holds 'live' objects, which will get modified while processing a state transition.
	objectMutex          sync.RWMutex
	stateObjects         sync.Map                    // map[common.Address]*stmStateObject
	stateObjectsPending  map[common.Address]struct{} // State objects finalized but not yet written to the trie
	stateObjectsDirty    map[common.Address]struct{} // State objects modified in the current execution
	stateObjectsDestruct map[common.Address]struct{} // State objects destructed in the block (contract destruction)

	lastWrites sync.Map // map[int][]*Location
	lastReads  sync.Map // map[int][]*ReadLoc

	// DB error.
	// State objects are used by the consensus core and VM which are
	// unable to deal with database-level errors. Any error that occurs
	// during a database read is memoized here and will eventually be
	// returned by StmStateDB.Commit. Notably, this error is also shared
	// by all cached state objects in case the database failure occurs
	// when accessing state of accounts.
	dbErr error

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

type ReadLoc struct {
	Location *Location
	Version  *TxInfoMini
}

type ReadResult struct {
	Status       int
	Version      *TxInfoMini
	BlockingTx   int
	DirtyState   *stateOperation
	DirtyStorage *storageOperation
}

func newReadResult() *ReadResult {
	return &ReadResult{
		Status:     -1,
		Version:    &TxInfoMini{Index: -1, Incarnation: -1},
		BlockingTx: -1,
		//DirtyState:   new(stateOperation),
		//DirtyStorage: new(storageOperation),
	}
}

type Location struct {
	stateAddress  common.Address
	storageSlot   common.Hash
	storageMarker bool
}

func (loc *Location) Address() (common.Address, common.Hash) {
	return loc.stateAddress, loc.storageSlot
}

func newLocation(stateAddress common.Address, storageSlot *common.Hash) *Location {
	storageMarker := false
	if storageSlot != nil {
		storageMarker = true
		newLoc := &Location{
			stateAddress:  stateAddress,
			storageSlot:   *storageSlot,
			storageMarker: storageMarker,
		}
		return newLoc
	}
	return &Location{
		stateAddress:  stateAddress,
		storageSlot:   common.Hash{},
		storageMarker: storageMarker,
	}
}

func (loc *Location) exist(compared []*Location) bool {
	for _, l := range compared {
		if strings.Compare(loc.stateAddress.Hex(), l.stateAddress.Hex()) == 0 {
			if !loc.storageMarker && !l.storageMarker {
				return true
			} else if loc.storageMarker && l.storageMarker {
				if strings.Compare(loc.storageSlot.Hex(), l.storageSlot.Hex()) == 0 {
					return true
				}
			}
		}
	}
	return false
}

// NewStmStateDB creates a new state from a given trie.
func NewStmStateDB(root common.Hash, db Database, snaps *snapshot.Tree) (*StmStateDB, error) {
	tr, err := db.OpenTrie(root)
	if err != nil {
		return nil, err
	}
	sdb := &StmStateDB{
		db:           db,
		trie:         tr,
		originalRoot: root,
		snaps:        snaps,
		//stateObjects:         make(map[common.Address]*stmStateObject),
		stateObjectsPending:  make(map[common.Address]struct{}),
		stateObjectsDirty:    make(map[common.Address]struct{}),
		stateObjectsDestruct: make(map[common.Address]struct{}),
		hasher:               crypto.NewKeccakState(),
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
func (s *StmStateDB) StartPrefetcher(namespace string) {
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
func (s *StmStateDB) StopPrefetcher() {
	if s.prefetcher != nil {
		s.prefetcher.close()
		s.prefetcher = nil
	}
}

// setError remembers the first non-nil error it is called with.
func (s *StmStateDB) setError(err error) {
	if s.dbErr == nil {
		s.dbErr = err
	}
}

// Database retrieves the low level database supporting the lower level trie ops.
func (s *StmStateDB) Database() Database {
	return s.db
}

// GetState retrieves a value from the given account's storage trie.
//func (s *StmStateDB) GetState(addr common.Address, hash common.Hash, txIndex, txIncarnation int) common.Hash {
//	stmStateObject, _ := s.getDeletedStateObject(addr)
//	if stmStateObject != nil {
//		stateAccount := stmStateObject.data.StateAccount[stmStateObject.data.len-1]
//		if !stateAccount.deleted {
//			return stmStateObject.GetCommittedState(s.db, hash, txIndex, txIncarnation)
//		}
//	}
//	return nil
//}

// Record updates the read and write sets of a new incarnation of tx
func (s *StmStateDB) Record(ver *TxInfoMini, readSet []*ReadLoc, writeSet WriteSets) bool {
	s.lastReads.Store(ver.Index, readSet)
	wroteNew := s.updateWrittenLocations(ver.Index, writeSet)
	s.applyWrites(ver.Index, ver.Incarnation, writeSet)
	return wroteNew
}

// MarkEstimates marks the last written locations 'estimate' after being aborted
func (s *StmStateDB) MarkEstimates(index int) {
	value, _ := s.lastWrites.Load(index)
	prevLocations := value.([]*Location)
	for _, loc := range prevLocations {
		if v, exist := s.stateObjects.Load(loc.stateAddress); exist {
			obj := v.(*stmStateObject)
			if loc.storageMarker {
				obj.setStorageEstimate(index, loc.storageSlot)
			} else {
				obj.setStateEstimate(index)
			}
		}
	}
}

// readStorageVersion reads the latest version at a specific account address
func (s *StmStateDB) readStateVersion(addr common.Address, index int) *ReadResult {
	var txIndexes []int
	res := newReadResult()

	if obj := s.getDeletedStateObject(addr); obj != nil {
		obj.multiVersionState.Range(func(key, value any) bool {
			id := key.(int)
			// find the maximum id smaller than the current tx index
			if id < index {
				txIndexes = append(txIndexes, id)
			}
			return true
		})

		// initially loads the snapshot state (initial version is set to -1)
		if len(txIndexes) == 0 {
			res.Status = NOT_FOUND
			op := &stateOperation{
				incarnation: -1,
				account:     obj.data,
				estimate:    false,
			}
			obj.multiVersionState.Store(-1, op)
			res.DirtyState = op
			return res
		}

		sort.Ints(txIndexes)
		maxIndex := txIndexes[len(txIndexes)-1]
		v, _ := obj.multiVersionState.Load(maxIndex)
		op := v.(*stateOperation)
		if op.estimate == true {
			// if the latest version is aborted
			res.Status = READ_ERROR
			res.BlockingTx = maxIndex
		} else if op.estimate == false && !op.account.deleted {
			res.Status = READ_OK
		}
		res.Version = &TxInfoMini{Index: maxIndex, Incarnation: op.incarnation}
		res.DirtyState = op
	}
	// the status is -1 when the state object does not exist
	return res
}

// readStorageVersion reads the latest version at a specific storage slot
func (s *StmStateDB) readStorageVersion(addr common.Address, hash common.Hash, index int) *ReadResult {
	var txIndexes []int
	res := newReadResult()
	// Tx that creates a contract account may be aborted after the first incarnation,
	// it leads to that the account data in the corresponding object equals nil (fail to update writes).
	// To avoid read the nil account data at the subsequent incarnations,
	// we stipulate that the normal read can only be conducted if the data is not nil.
	if obj := s.getDeletedStateObject(addr); obj != nil && obj.data.StateAccount != nil {
		obj.mvStorageMutex.Lock()
		defer obj.mvStorageMutex.Unlock()
		smap, ok := obj.multiVersionStorage[hash]
		if !ok {
			// initially loads the snapshot storage (initial version is set to -1)
			res.Status = NOT_FOUND
			value := obj.GetCommittedState(s.db, hash)
			op := &storageOperation{
				incarnation:  -1,
				storageValue: value,
				estimate:     false,
			}
			smap = make(slotMap)
			smap[-1] = op
			obj.multiVersionStorage[hash] = smap
			res.DirtyStorage = op
			return res
		}

		for id := range smap {
			if id < index {
				txIndexes = append(txIndexes, id)
			}
		}
		sort.Ints(txIndexes)
		maxIndex := txIndexes[len(txIndexes)-1]
		op := smap[maxIndex]
		// if the latest version is aborted
		if op.estimate == true {
			res.Status = READ_ERROR
			res.BlockingTx = maxIndex
		} else {
			res.Status = READ_OK
		}

		res.Version = &TxInfoMini{Index: maxIndex, Incarnation: op.incarnation}
		res.DirtyStorage = op
	}
	// the status is -1 when the state object does not exist
	return res
}

// ValidateReadSet validates the fetched read-set during last execution
func (s *StmStateDB) ValidateReadSet(index int) bool {
	v, _ := s.lastReads.Load(index)
	res := v.([]*ReadLoc)
	for _, read := range res {
		var curRes *ReadResult
		if !read.Location.storageMarker {
			curRes = s.readStateVersion(read.Location.stateAddress, index)
		} else {
			curRes = s.readStorageVersion(read.Location.stateAddress, read.Location.storageSlot, index)
		}
		if curRes.Status == READ_ERROR || curRes.Status == NOT_FOUND {
			return false
		} else if curRes.Status == READ_OK && !curRes.Version.Compare(read.Version) {
			return false
		}
	}
	return true
}

// updateWrittenLocations updates the last written memory locations if a new incarnation of tx writes to new memory locations
func (s *StmStateDB) updateWrittenLocations(index int, writeSets WriteSets) bool {
	var (
		wroteNew  bool
		locations []*Location
	)

	// convert writeSets to a set of locations
	for addr, write := range writeSets {
		if write.stateModified {
			loc := newLocation(addr, nil)
			locations = append(locations, loc)
		}
		if write.storageModified {
			for key := range write.AccessedSlots {
				loc := newLocation(addr, &key)
				locations = append(locations, loc)
			}
		}
	}

	value, ok := s.lastWrites.Load(index)
	if !ok {
		// the first incarnation of tx
		wroteNew = true
	} else {
		prevLocations := value.([]*Location)
		for _, loc := range prevLocations {
			exist := loc.exist(locations)
			if !exist {
				// remove this stored entry
				v, _ := s.stateObjects.Load(loc.stateAddress)
				obj := v.(*stmStateObject)
				if loc.storageMarker {
					obj.mvStorageMutex.Lock()
					smap, ok2 := obj.multiVersionStorage[loc.storageSlot]
					if ok2 {
						if _, noDeleted := smap[index]; noDeleted {
							delete(smap, index)
						}
					}
					obj.mvStorageMutex.Unlock()
				} else {
					obj.multiVersionState.Delete(index)
				}
				wroteNew = true
			}
		}
	}

	s.lastWrites.Store(index, locations)
	return wroteNew
}

// applyWrites updates state or storage version  according to tx's write sets
func (s *StmStateDB) applyWrites(index, incarnation int, writeSets WriteSets) {
	for addr, write := range writeSets {
		obj := s.getDeletedStateObject(addr)
		// if the account is newly created in this block
		if obj == nil {
			obj = createStmStateObject(s, addr)
			s.stateObjects.LoadOrStore(addr, obj)
			if write.stateModified {
				obj.setState(index, incarnation, write)
			}
		} else {
			if write.stateModified {
				// Some contract accounts may invoke internal transactions without changing the account states,
				// yet the modifying operations are logged.
				if !write.Account.deleted && obj.data.StateAccount != nil && obj.data.StateAccount.Nonce == write.Account.StateAccount.Nonce &&
					obj.data.StateAccount.Balance.Cmp(write.Account.StateAccount.Balance) == 0 &&
					bytes.Equal(obj.data.StateAccount.CodeHash, write.Account.StateAccount.CodeHash) {
				} else {
					obj.setState(index, incarnation, write)
				}
			}
		}
		if write.storageModified {
			obj.setStorage(index, incarnation, write)
		}
	}
}

//func (s *StmStateDB) getStateAccount(addr common.Address, txIndex, txIncarnation int) *SStateAccount {
//	if obj, _ := s.getDeletedStateObject(addr); obj != nil {
//		// stateAccount := obj.data.StateAccount[obj.data.len-1]
//		stateAccount := obj.data.StateAccount[0]
//		if !stateAccount.deleted {
//			return stateAccount.Copy()
//		}
//	}
//	return nil
//}

// getDeletedStateObject is similar to getStateObject, but instead of returning
// nil for a deleted state object, it returns the actual object with the deleted
// flag set. This is needed by the state journal to revert to the correct s-
// destructed object instead of wiping all knowledge about the state object.
// the boolean value is returned to indicate whether the state can be fetched from the memory
func (s *StmStateDB) getDeletedStateObject(addr common.Address) *stmStateObject {
	// Prefer live objects if any is available
	s.objectMutex.Lock()
	defer s.objectMutex.Unlock()
	//s.objectMutex.Lock()
	//s.objectMutex.Lock()
	obj, ok := s.stateObjects.Load(addr)
	//s.objectMutex.Unlock()
	//s.objectMutex.Unlock()
	if ok {
		return obj.(*stmStateObject)
	}
	// If no live objects are available, attempt to use snapshots
	var data *types.StateAccount
	if s.snap != nil {
		start := time.Now()
		s.hashMutex.Lock()
		acc, err := s.snap.Account(crypto.HashData(s.hasher, addr.Bytes()))
		s.hashMutex.Unlock()
		if metrics.EnabledExpensive {
			s.SnapshotAccountReads += time.Since(start)
		}
		if err == nil {
			if acc == nil {
				return nil
			}
			data = &types.StateAccount{
				Nonce:    acc.Nonce,
				Balance:  new(big.Int).Set(acc.Balance),
				CodeHash: common.CopyBytes(acc.CodeHash),
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
			return nil
		}
	}
	// Insert into the live set
	newObj := newStmStateObject(s, addr, *data)
	newObj1, ok1 := s.setStateObject(newObj)
	if ok1 {
		return newObj1
	}
	return newObj
}

//func (s *StmStateDB) setStateObject(object *stmStateObject) {
//	s.stateObjects[object.Address()] = object
//}

func (s *StmStateDB) setStateObject(object *stmStateObject) (*stmStateObject, bool) {
	//s.objectMutex.Lock()
	//obj, ok := s.stateObjects.Load(object.Address())
	//if !ok {
	//	//s.stateObjects.[object.Address()] = object
	//}
	obj, ok := s.stateObjects.LoadOrStore(object.Address(), object)
	//s.objectMutex.Unlock()
	return obj.(*stmStateObject), ok
}

// updateStateObject writes the given object to the trie.
func (s *StmStateDB) updateStateObject(obj *stmStateObject) {
	// Track the amount of time wasted on updating the account from the trie
	if metrics.EnabledExpensive {
		defer func(start time.Time) { s.AccountUpdates += time.Since(start) }(time.Now())
	}
	// Encode the account and update the account trie
	addr := obj.Address()
	if err := s.trie.TryUpdateAccount(addr, obj.data.StateAccount); err != nil {
		s.setError(fmt.Errorf("updateStateObject (%x) error: %v", addr[:], err))
	}

	// If state snapshotting is active, cache the data til commit. Note, this
	// update mechanism is not symmetric to the deletion, because whereas it is
	// enough to track account updates at commit time, deletions need tracking
	// at transaction boundary level to ensure we capture state clearing.
	if s.snap != nil {
		s.snapAccounts[obj.addrHash] = snapshot.SlimAccountRLP(obj.Nonce(), obj.Balance(), obj.Root(), obj.CodeHash())
	}
}

// deleteStateObject removes the given object from the state trie.
func (s *StmStateDB) deleteStateObject(obj *stmStateObject) {
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

// FinaliseMVMemory finalises the latest states of multi-version state and storage
func (s *StmStateDB) FinaliseMVMemory() {
	s.stateObjects.Range(func(key, value interface{}) bool {
		obj := value.(*stmStateObject)
		obj.updateAccount()
		obj.updateStorage()
		return true
	})
}

// Finalise finalises the state by removing the destructed objects and clears
// the journal as well as the refunds. Finalise, however, will not push any updates
// into the tries just yet. Only IntermediateRoot or Commit will do that.
func (s *StmStateDB) Finalise(deleteEmptyObjects bool) {
	addressesToPrefetch := make([][]byte, 0)
	//for addr := range s.journal.dirties {
	// 如果 object 中 data的 len 超过1, 或者 data的长度为1时，txIndex 或 Incarnation 不为1
	s.stateObjects.Range(func(addr1, obj1 interface{}) bool {
		addr := addr1.(common.Address)
		obj := obj1.(*stmStateObject)
		//if obj.data.len == 1 && obj.data.StateAccount[0].TxInfo.Index == -1 && len(obj.dirtyStorage) == 0 {
		if !obj.dirtyMarker {
			// 如果长度为1，且是从leveldb中读取到的(index = -1), 则非经过修改的结点
			//if txIndex == 11 && addr == common.HexToAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7") {
			//	fmt.Println("len(obj.dirtyStorage):", len(obj.dirtyStorage))
			//}
			return true // sync map的遍历, 继续遍历下一个
		}

		objState := obj.data
		if objState.suicided || (deleteEmptyObjects && obj.empty()) {
			objState.deleted = true

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
			obj.finalise(true) // Prefetch slots in the background
		}
		s.stateObjectsPending[addr] = struct{}{}
		s.stateObjectsDirty[addr] = struct{}{}
		// At this point, also ship the address off to the precacher. The precacher
		// will start loading tries, and when the change is eventually committed,
		// the commit-phase will be a lot faster
		addressesToPrefetch = append(addressesToPrefetch, common.CopyBytes(addr[:])) // Copy needed for closure
		return true                                                                  // sync map的遍历
	})
	//for addr, obj := range s.stateObject { // 只有被修改了，才进入此循环
	//	//if txIndex == 11 && addr == common.HexToAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7") {
	//	//	fmt.Println("len(obj.dirtyStorage):", len(obj.dirtyStorage))
	//	//}
	//	if obj.data.len == 1 && obj.data.StateAccount[0].TxInfo.Index == -1 && len(obj.dirtyStorage) == 0 {
	//		// 如果长度为1，且是从leveldb中读取到的(index = -1), 则非经过修改的结点
	//		//if txIndex == 11 && addr == common.HexToAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7") {
	//		//	fmt.Println("len(obj.dirtyStorage):", len(obj.dirtyStorage))
	//		//}
	//		continue
	//	}
	//	//fmt.Println(addr)
	//	objState := obj.data.StateAccount[obj.data.len-1]
	//	if objState.suicided || (deleteEmptyObjects && obj.empty()) {
	//		objState.deleted = true
	//
	//		// We need to maintain account deletions explicitly (will remain
	//		// set indefinitely).
	//		s.stateObjectsDestruct[obj.address] = struct{}{}
	//
	//		// If state snapshotting is active, also mark the destruction there.
	//		// Note, we can't do this only at the end of a block because multiple
	//		// transactions within the same block might self destruct and then
	//		// resurrect an account; but the snapshotter needs both events.
	//		if s.snap != nil {
	//			delete(s.snapAccounts, obj.addrHash) // Clear out any previously updated account data (may be recreated via a resurrect)
	//			delete(s.snapStorage, obj.addrHash)  // Clear out any previously updated storage data (may be recreated via a resurrect)
	//		}
	//	} else {
	//		obj.finalise(true, txIndex) // Prefetch slots in the background
	//	}
	//	s.stateObjectsPending[addr] = struct{}{}
	//	s.stateObjectsDirty[addr] = struct{}{}
	//
	//	// At this point, also ship the address off to the precacher. The precacher
	//	// will start loading tries, and when the change is eventually committed,
	//	// the commit-phase will be a lot faster
	//	addressesToPrefetch = append(addressesToPrefetch, common.CopyBytes(addr[:])) // Copy needed for closure
	//}
	if s.prefetcher != nil && len(addressesToPrefetch) > 0 {
		s.prefetcher.prefetch(common.Hash{}, s.originalRoot, addressesToPrefetch)
	}
}

// IntermediateRoot computes the current root hash of the state trie.
// It is called in between transactions to get the root hash that
// goes into transaction receipts.
func (s *StmStateDB) IntermediateRoot(deleteEmptyObjects bool) common.Hash {
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
		//if obj := s.stateObjects.[addr]; !obj.data.StateAccount[obj.data.len-1].deleted {
		//	obj.updateRoot(s.db)
		//}
		obj1, _ := s.stateObjects.Load(addr)
		obj := obj1.(*stmStateObject)
		if !obj.data.deleted {
			obj.updateRoot(s.db)
		}
	}
	// Now we're about to start to write changes to the trie. The trie is so far
	// _untouched_. We can check with the prefetcher, if it can give us a trie
	// which has the same root, but also has some content loaded into it.
	if prefetcher != nil {
		if tr := prefetcher.trie(common.Hash{}, s.originalRoot); tr != nil {
			s.trie = tr
		}
	}
	usedAddrs := make([][]byte, 0, len(s.stateObjectsPending))
	for addr := range s.stateObjectsPending {
		//if obj := s.stateObjects[addr]; obj.data.StateAccount[obj.data.len-1].deleted {
		obj1, _ := s.stateObjects.Load(addr)
		obj := obj1.(*stmStateObject)
		if obj.data.deleted {
			s.deleteStateObject(obj)
			s.AccountDeleted += 1
		} else {
			s.updateStateObject(obj)
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

// Commit writes the state to the underlying in-memory trie database.
func (s *StmStateDB) Commit(deleteEmptyObjects bool) (common.Hash, error) {
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
		//if obj := s.stateObjects[addr]; !obj.data.StateAccount[obj.data.len-1].deleted {
		obj1, _ := s.stateObjects.Load(addr)
		obj := obj1.(*stmStateObject)
		if obj.data.deleted {
			// Write any contract code associated with the state object
			stateAccount := obj.data
			if stateAccount.Code != nil && stateAccount.dirtyCode {
				rawdb.WriteCode(codeWriter, common.BytesToHash(obj.CodeHash()), stateAccount.Code)
				stateAccount.dirtyCode = false
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

// convertAccountSet converts a provided account set from address keyed to hash keyed.
func (s *StmStateDB) convertAccountSet(set map[common.Address]struct{}) map[common.Hash]struct{} {
	ret := make(map[common.Hash]struct{})
	for addr := range set {
		//obj, exist := s.stateObjects[addr]
		obj1, exist := s.stateObjects.Load(addr)
		if !exist {
			ret[crypto.Keccak256Hash(addr[:])] = struct{}{}
		} else {
			obj := obj1.(*stmStateObject)
			ret[obj.addrHash] = struct{}{}
		}
	}
	return ret
}

//func (s *StmStateDB) Validation(valObjects map[common.Address]*stmTxStateObject, txIndex, txIncarnation int) {
//	for addr, txObj := range valObjects {
//		//obj, exist := s.stateObjects[addr]
//		obj1, exist := s.stateObjects.Load(addr)
//		if exist {
//			obj := obj1.(*stmStateObject)
//			objData := obj.data
//			// 没有被删除, 且data一致则state不变
//			if !txObj.data.deleted && txObj.data.StateAccount.Nonce == objData.StateAccount.Nonce &&
//				txObj.data.StateAccount.Balance.Cmp(objData.StateAccount.Balance) == 0 && bytes.Equal(txObj.data.StateAccount.CodeHash, objData.StateAccount.CodeHash) {
//
//			} else {
//				obj.setStateAccount(txObj, txIndex, txIncarnation)
//			}
//		} else {
//			obj := createStmStateObject(s, addr)
//			//fmt.Println(addr, obj)
//			s.stateObjects.LoadOrStore(addr, obj)
//			obj.setStateAccount(txObj, txIndex, txIncarnation)
//		}
//
//		for key, value := range txObj.dirtyStorage {
//			//if txIndex == 11 {
//			//	fmt.Println(txIndex, addr, key, value)
//			//}
//			obj1, _ := s.stateObjects.Load(addr)
//			obj := obj1.(*stmStateObject)
//			if _, dirty := obj.dirtyStorage[key]; !dirty { // 原先没有写入
//				obj.dirtyStorage[key] = newEmptySlot()
//			}
//			slot := obj.dirtyStorage[key]
//			slot.Value = append(slot.Value, &SSlot{Value: value, TxInfo: &TxInfoMini{Index: txIndex, Incarnation: txIncarnation}})
//			slot.len += 1
//			//if txIndex == 11 {
//			//	fmt.Println(obj.dirtyStorage[key].Value[obj.dirtyStorage[key].len-1].Value)
//			//}
//		}
//	}
//
//	//if txIndex == 11 {
//	//	obj := s.stateObjects[common.HexToAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7")]
//	//	fmt.Println("len(obj.dirtyStorage):", len(obj.dirtyStorage))
//	//}
//}

// Root converts a provided account set from address keyed to hash keyed.
func (s *StmStateDB) Root() common.Hash {
	return s.originalRoot
}

// AddBalance adds amount to the account associated with addr.
func (s *StmStateDB) AddBalance(addr common.Address, amount *big.Int) {
	//obj, exist := s.stateObjects[addr]
	obj1, exist := s.stateObjects.Load(addr)
	if exist {
		obj := obj1.(*stmStateObject)
		oldBalance := obj.data.StateAccount.Balance
		obj.data.StateAccount.Balance = new(big.Int).Add(oldBalance, amount)
	}
}

// Copy creates a deep, independent copy of the state.
// Snapshots of the copied state cannot be applied to the copy.
func (s *StmStateDB) Copy() *StmStateDB {
	// Copy all the basic fields, initialize the memory ones
	state := &StmStateDB{
		db:                   s.db,
		trie:                 s.db.CopyTrie(s.trie),
		originalRoot:         s.originalRoot,
		stateObjectsPending:  make(map[common.Address]struct{}, len(s.stateObjectsPending)),
		stateObjectsDirty:    make(map[common.Address]struct{}, len(s.stateObjectsDirty)),
		stateObjectsDestruct: make(map[common.Address]struct{}, len(s.stateObjectsDestruct)),
		hasher:               crypto.NewKeccakState(),
	}
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
