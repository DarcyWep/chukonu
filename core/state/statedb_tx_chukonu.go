package state

import (
	"bytes"
	"chukonu/core/types"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"math/big"
	"sort"
)

type ChuKoNuTxStateDB struct {
	statedb *ChuKoNuStateDB

	// This map holds 'live' objects, which will get modified while processing a state transition.
	stateObjects         map[common.Address]*ChuKoNuTxStateObject
	stateObjectsDirty    map[common.Address]struct{} // State objects modified in the current execution
	stateObjectsDestruct map[common.Address]struct{} // State objects destructed in the block

	tokens map[common.Address]*StateTokenToAccountState

	// DB error.
	// State objects are used by the consensus core and VM which are
	// unable to deal with database-level errors. Any error that occurs
	// during a database read is memoized here and will eventually be
	// returned by ChuKoNuTxStateDB.Commit. Notably, this error is also shared
	// by all cached state objects in case the database failure occurs
	// when accessing state of accounts.
	dbErr error

	// The refund counter, also used by state transitioning.
	refund uint64

	thash         common.Hash
	txIndex       int
	accessAddress *types.AccessAddressMap

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
}

// NewChuKoNuTxStateDB creates a new state from a given trie.
func NewChuKoNuTxStateDB(statedb *ChuKoNuStateDB) *ChuKoNuTxStateDB {
	return &ChuKoNuTxStateDB{
		statedb:              statedb,
		stateObjects:         make(map[common.Address]*ChuKoNuTxStateObject),
		stateObjectsDirty:    make(map[common.Address]struct{}),
		stateObjectsDestruct: make(map[common.Address]struct{}),
		tokens:               make(map[common.Address]*StateTokenToAccountState),
		logs:                 make(map[common.Hash][]*types.Log),
		preimages:            make(map[common.Hash][]byte),
		journal:              newChuKoNuJournal(),
		accessList:           newAccessList(),
		transientStorage:     newTransientStorage(),
	}
}

// setError remembers the first non-nil error it is called with.
func (s *ChuKoNuTxStateDB) setError(err error) {
	if s.dbErr == nil {
		s.dbErr = err
	}
}

// Error returns the memorized database failure occurred earlier.
func (s *ChuKoNuTxStateDB) Error() error {
	return s.dbErr
}

func (s *ChuKoNuTxStateDB) AddLog(log *types.Log) {
	s.journal.append(chuKoNuAddLogChange{txhash: s.thash})

	log.TxHash = s.thash
	log.TxIndex = uint(s.txIndex)
	log.Index = s.logSize
	s.logs[s.thash] = append(s.logs[s.thash], log)
	s.logSize++
}

// GetLogs returns the logs matching the specified transaction hash, and annotates
// them with the given blockNumber and blockHash.
func (s *ChuKoNuTxStateDB) GetLogs(hash common.Hash, blockNumber uint64, blockHash common.Hash) []*types.Log {
	logs := s.logs[hash]
	for _, l := range logs {
		l.BlockNumber = blockNumber
		l.BlockHash = blockHash
	}
	return logs
}

func (s *ChuKoNuTxStateDB) Logs() []*types.Log {
	var logs []*types.Log
	for _, lgs := range s.logs {
		logs = append(logs, lgs...)
	}
	return logs
}

// AddPreimage records a SHA3 preimage seen by the VM.
func (s *ChuKoNuTxStateDB) AddPreimage(hash common.Hash, preimage []byte) {
	if _, ok := s.preimages[hash]; !ok {
		s.journal.append(chuKoNuAddPreimageChange{hash: hash})
		pi := make([]byte, len(preimage))
		copy(pi, preimage)
		s.preimages[hash] = pi
	}
}

// Preimages returns a list of SHA3 preimages that have been submitted.
func (s *ChuKoNuTxStateDB) Preimages() map[common.Hash][]byte {
	return s.preimages
}

// AddRefund adds gas to the refund counter
func (s *ChuKoNuTxStateDB) AddRefund(gas uint64) {
	s.journal.append(chuKoNuRefundChange{prev: s.refund})
	s.refund += gas
}

// SubRefund removes gas from the refund counter.
// This method will panic if the refund counter goes below zero
func (s *ChuKoNuTxStateDB) SubRefund(gas uint64) {
	s.journal.append(chuKoNuRefundChange{prev: s.refund})
	if gas > s.refund {
		panic(fmt.Sprintf("Refund counter below zero (gas: %d > refund: %d)", gas, s.refund))
	}
	s.refund -= gas
}

// Exist reports whether the given account address exists in the state.
// Notably this also returns true for suicided accounts.
func (s *ChuKoNuTxStateDB) Exist(addr common.Address) bool {
	addAccessAddr(s.accessAddress, addr, true, false)
	return s.getStateObject(addr) != nil
}

// Empty returns whether the state object is either non-existent
// or empty according to the EIP161 specification (balance = nonce = code = 0)
func (s *ChuKoNuTxStateDB) Empty(addr common.Address) bool {
	addAccessAddr(s.accessAddress, addr, true, false)
	so := s.getStateObject(addr)
	return so == nil || so.empty()
}

// GetBalance retrieves the balance from the given address or 0 if object not found
func (s *ChuKoNuTxStateDB) GetBalance(addr common.Address) *big.Int {
	addAccessAddr(s.accessAddress, addr, true, false)
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		return stateObject.Balance()
	}
	return common.Big0
}

func (s *ChuKoNuTxStateDB) GetNonce(addr common.Address) uint64 {
	addAccessAddr(s.accessAddress, addr, true, false)
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		return stateObject.Nonce()
	}

	return 0
}

// TxIndex returns the current transaction index set by Prepare.
func (s *ChuKoNuTxStateDB) TxIndex() int {
	return s.txIndex
}

func (s *ChuKoNuTxStateDB) GetCode(addr common.Address) []byte {
	addAccessAddr(s.accessAddress, addr, true, false)
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		return stateObject.Code()
	}
	return nil
}

func (s *ChuKoNuTxStateDB) GetCodeSize(addr common.Address) int {
	addAccessAddr(s.accessAddress, addr, true, false)
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		return stateObject.CodeSize()
	}
	return 0
}

func (s *ChuKoNuTxStateDB) GetCodeHash(addr common.Address) common.Hash {
	addAccessAddr(s.accessAddress, addr, true, false)
	stateObject := s.getStateObject(addr)
	if stateObject == nil {
		return common.Hash{}
	}
	return common.BytesToHash(stateObject.CodeHash())
}

// GetState retrieves a value from the given account's storage trie.
func (s *ChuKoNuTxStateDB) GetState(addr common.Address, hash common.Hash) common.Hash {
	addAccessSlot(s.accessAddress, addr, hash, true, s.txIndex)
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		return stateObject.GetState(hash)
	}
	return common.Hash{}
}

// GetCommittedState retrieves a value from the given account's committed storage trie.
func (s *ChuKoNuTxStateDB) GetCommittedState(addr common.Address, hash common.Hash) common.Hash {
	addAccessSlot(s.accessAddress, addr, hash, true, s.txIndex)
	stateObject := s.getStateObject(addr)
	if stateObject != nil {
		return stateObject.GetCommittedState(hash)
	}
	return common.Hash{}
}

func (s *ChuKoNuTxStateDB) HasSuicided(addr common.Address) bool {
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
func (s *ChuKoNuTxStateDB) AddBalance(addr common.Address, amount *big.Int) {
	if amount.Cmp(common.Big0) != 0 {
		addAccessAddr(s.accessAddress, addr, false, false)
	}
	stateObject := s.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.AddBalance(amount)
	}
}

// SubBalance subtracts amount from the account associated with addr.
func (s *ChuKoNuTxStateDB) SubBalance(addr common.Address, amount *big.Int) {
	if amount.Cmp(common.Big0) != 0 {
		addAccessAddr(s.accessAddress, addr, false, false)
	}
	stateObject := s.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SubBalance(amount)
	}
}

func (s *ChuKoNuTxStateDB) SetBalance(addr common.Address, amount *big.Int) {
	stateObject := s.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetBalance(amount)
	}
}

func (s *ChuKoNuTxStateDB) SetNonce(addr common.Address, nonce uint64) {
	addAccessAddr(s.accessAddress, addr, false, false)
	stateObject := s.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetNonce(nonce)
	}
}

func (s *ChuKoNuTxStateDB) SetCode(addr common.Address, code []byte) {
	addAccessAddr(s.accessAddress, addr, false, false)
	stateObject := s.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetCode(crypto.Keccak256Hash(code), code)
	}
}

func (s *ChuKoNuTxStateDB) SetState(addr common.Address, key, value common.Hash) {
	addAccessSlot(s.accessAddress, addr, key, false, s.txIndex)
	stateObject := s.GetOrNewStateObject(addr)
	if stateObject != nil {
		stateObject.SetState(key, value)
	}
}

// Suicide marks the given account as suicided.
// This clears the account balance.
//
// The account's state object is still available until the state is committed,
// getStateObject will return a non-nil account after Suicide.
func (s *ChuKoNuTxStateDB) Suicide(addr common.Address) bool {
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
func (s *ChuKoNuTxStateDB) SetTransientState(addr common.Address, key, value common.Hash) {
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
func (s *ChuKoNuTxStateDB) setTransientState(addr common.Address, key, value common.Hash) {
	s.transientStorage.Set(addr, key, value)
}

// GetTransientState gets transient storage for a given account.
func (s *ChuKoNuTxStateDB) GetTransientState(addr common.Address, key common.Hash) common.Hash {
	addAccessAddr(s.accessAddress, addr, true, false)
	return s.transientStorage.Get(addr, key)
}

//
// Setting, updating & deleting state object methods.
//

// getStateObject retrieves a state object given by the address, returning nil if
// the object is not found or was deleted in this execution context. If you need
// to differentiate between non-existent/just-deleted, use getDeletedStateObject.
func (s *ChuKoNuTxStateDB) getStateObject(addr common.Address) *ChuKoNuTxStateObject {
	if obj := s.getDeletedStateObject(addr); obj != nil && !obj.deleted {
		return obj
	}
	return nil
}

// getDeletedStateObject is similar to getStateObject, but instead of returning
// nil for a deleted state object, it returns the actual object with the deleted
// flag set. This is needed by the state journal to revert to the correct s-
// destructed object instead of wiping all knowledge about the state object.
func (s *ChuKoNuTxStateDB) getDeletedStateObject(addr common.Address) *ChuKoNuTxStateObject {
	// Prefer live objects if any is available
	if obj := s.stateObjects[addr]; obj != nil {
		return obj
	}
	//originObj := s.statedb.GetOriginStateAccount(addr)
	//data := &types.StateAccount{
	//	Nonce:    originObj.Nonce,
	//	Balance:  originObj.Balance,
	//	CodeHash: originObj.CodeHash,
	//	Root:     originObj.Root,
	//}

	data := &types.StateAccount{
		Nonce:    0,
		Balance:  new(big.Int).SetInt64(0),
		CodeHash: types.EmptyCodeHash.Bytes(),
		Root:     types.EmptyRootHash,
	}
	// Insert into the live set
	obj := NewChuKoNuTxObject(s, s.statedb, addr, *data, true, nil, false)
	s.setStateObject(obj)
	return obj
}

func (s *ChuKoNuTxStateDB) setStateObject(object *ChuKoNuTxStateObject) {
	s.stateObjects[object.Address()] = object
}

// GetOrNewStateObject retrieves a state object or create a new state object if nil.
func (s *ChuKoNuTxStateDB) GetOrNewStateObject(addr common.Address) *ChuKoNuTxStateObject {
	stateObject := s.getStateObject(addr)
	if stateObject == nil {
		stateObject, _ = s.createObject(addr)
	}
	return stateObject
}

// createObject creates a new state object. If there is an existing account with
// the given address, it is overwritten and returned as the second return value.
func (s *ChuKoNuTxStateDB) createObject(addr common.Address) (newobj, prev *ChuKoNuTxStateObject) {
	prev = s.getDeletedStateObject(addr) // Note, prev might have been deleted, we need that!

	var prevdestruct bool
	if prev != nil {
		_, prevdestruct = s.stateObjectsDestruct[prev.address]
		if !prevdestruct {
			s.stateObjectsDestruct[prev.address] = struct{}{}
		}
	}
	newobj = NewChuKoNuTxObject(s, s.statedb, addr, types.StateAccount{}, false, nil, false)
	if prev == nil {
		s.journal.append(chuKoNuCreateObjectChange{account: &addr})
	} else {
		s.journal.append(chuKoNuResetObjectChange{prev: prev, prevdestruct: prevdestruct})
	}
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
func (s *ChuKoNuTxStateDB) CreateAccount(addr common.Address) {
	addAccessAddr(s.accessAddress, addr, true, false)
	newObj, prev := s.createObject(addr)
	if prev != nil {
		newObj.setBalance(prev.data.Balance)
	}
}

// Copy creates a deep, independent copy of the state.
// Snapshots of the copied state cannot be applied to the copy.
func (s *ChuKoNuTxStateDB) Copy() *ChuKoNuTxStateDB {
	// Copy all the basic fields, initialize the memory ones
	state := &ChuKoNuTxStateDB{
		statedb:              s.statedb.Copy(),
		stateObjects:         make(map[common.Address]*ChuKoNuTxStateObject, len(s.journal.dirties)),
		stateObjectsDirty:    make(map[common.Address]struct{}, len(s.journal.dirties)),
		stateObjectsDestruct: make(map[common.Address]struct{}, len(s.stateObjectsDestruct)),
		tokens:               make(map[common.Address]*StateTokenToAccountState, len(s.tokens)),
		refund:               s.refund,
		logs:                 make(map[common.Hash][]*types.Log, len(s.logs)),
		logSize:              s.logSize,
		preimages:            make(map[common.Hash][]byte, len(s.preimages)),
		journal:              newChuKoNuJournal(),
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
			state.stateObjects[addr] = object.deepCopy(s, s.statedb)

			state.stateObjectsDirty[addr] = struct{}{} // Mark the copy dirty to force internal (code/state) commits
		}
	}

	for addr := range s.stateObjectsDirty {
		if _, exist := state.stateObjects[addr]; !exist {
			state.stateObjects[addr] = s.stateObjects[addr].deepCopy(s, s.statedb)
		}
		state.stateObjectsDirty[addr] = struct{}{}
	}
	for addr := range s.stateObjects {
		if _, exist := state.stateObjects[addr]; !exist {
			state.stateObjects[addr] = s.stateObjects[addr].deepCopy(s, s.statedb)
		}
	}
	// Deep copy the destruction flag.
	for addr := range s.stateObjectsDestruct {
		state.stateObjectsDestruct[addr] = struct{}{}
	}

	for addr, token := range s.tokens {
		state.tokens[addr] = token.deepCopy()
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
	return state
}

// Snapshot returns an identifier for the current revision of the state.
func (s *ChuKoNuTxStateDB) Snapshot() int {
	id := s.nextRevisionId
	s.nextRevisionId++
	s.validRevisions = append(s.validRevisions, revision{id, s.journal.length()})
	return id
}

// RevertToSnapshot reverts all state changes made since the given revision.
func (s *ChuKoNuTxStateDB) RevertToSnapshot(revid int) {
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
func (s *ChuKoNuTxStateDB) GetRefund() uint64 {
	return s.refund
}

// SetTxContext sets the current transaction hash and index which are
// used when the EVM emits new state logs. It should be invoked before
// transaction execution.
func (s *ChuKoNuTxStateDB) SetTxContext(thash common.Hash, ti int, simulation bool) {
	//fmt.Println(s)
	s.thash = thash
	s.txIndex = ti
	s.accessAddress = nil
	if simulation {
		s.accessAddress = types.NewAccessAddressMap()
	}
}

func (s *ChuKoNuTxStateDB) clearJournalAndRefund() {
	if len(s.journal.entries) > 0 {
		s.journal = newChuKoNuJournal()
		s.refund = 0
	}
	s.validRevisions = s.validRevisions[:0] // Snapshots can be created without journal entries
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
func (s *ChuKoNuTxStateDB) Prepare(rules params.Rules, sender, coinbase common.Address, dst *common.Address, precompiles []common.Address, list types.AccessList) {
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
func (s *ChuKoNuTxStateDB) AddAddressToAccessList(addr common.Address) {
	if s.accessList.AddAddress(addr) {
		s.journal.append(chuKoNuAccessListAddAccountChange{&addr})
	}
}

// AddSlotToAccessList adds the given (address, slot)-tuple to the access list
func (s *ChuKoNuTxStateDB) AddSlotToAccessList(addr common.Address, slot common.Hash) {
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
func (s *ChuKoNuTxStateDB) AddressInAccessList(addr common.Address) bool {
	return s.accessList.ContainsAddress(addr)
}

// SlotInAccessList returns true if the given (address, slot)-tuple is in the access list.
func (s *ChuKoNuTxStateDB) SlotInAccessList(addr common.Address, slot common.Hash) (addressPresent bool, slotPresent bool) {
	return s.accessList.Contains(addr, slot)
}

func (s *ChuKoNuTxStateDB) AccessAddress() *types.AccessAddressMap {
	return s.accessAddress
}

func (s *ChuKoNuTxStateDB) StateObjects() {
}

func (s *ChuKoNuTxStateDB) appendJournal(journal *chuKoNuJournal, entry chuKoNuJournalEntry) {

}

// GetOriginBalance retrieves the balance from the given address or 0 if object not found
func (s *ChuKoNuTxStateDB) GetOriginBalance(addr common.Address) *big.Int {
	obj := s.getDeletedStateObject(addr)
	return obj.OriginBalance()
}

func (s *ChuKoNuTxStateDB) GetOriginNonce(addr common.Address) uint64 {
	obj := s.getDeletedStateObject(addr)
	return obj.OriginNonce()
}

func (s *ChuKoNuTxStateDB) GetOriginCodeHash(addr common.Address) common.Hash {
	obj := s.getDeletedStateObject(addr)
	return common.BytesToHash(obj.OriginCodeHash())
}

// GetOriginState retrieves a value from the given account's storage trie.
func (s *ChuKoNuTxStateDB) GetOriginState(addr common.Address, hash common.Hash) common.Hash {
	obj := s.getDeletedStateObject(addr)
	return obj.OriginState(hash)
}

func (s *ChuKoNuTxStateDB) GetOriginCode(addr common.Address) []byte {
	obj := s.getDeletedStateObject(addr)
	return obj.OriginCode()
}

func (s *ChuKoNuTxStateDB) GetOriginCodeSize(addr common.Address) int {
	obj := s.getDeletedStateObject(addr)
	code := obj.OriginCode()
	if code != nil {
		return len(code)
	}
	return 0
}

func (s *ChuKoNuTxStateDB) GenerateStateTokenToAccountState() {
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
		if obj.suicided || obj.empty() {
			obj.deleted = true
		}

		var (
			data *types.StateAccount = nil
			code []byte              = nil
		)
		if obj.IsDataChange() {
			data = obj.data.Copy()
		}
		if !bytes.Equal(obj.code, obj.originCode) {
			code = obj.code
		}

		s.tokens[addr] = NewStateTokenToAccountState(addr, data, obj.dirtyStorage, code, obj.suicided)
	}
}

func (s *ChuKoNuTxStateDB) Token(addr common.Address) *StateTokenToAccountState {
	return s.tokens[addr]
}
