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
	"sync"
)

// StmTransaction is an Ethereum transaction.
type StmTransaction struct {
	Tx          *types.Transaction
	Index       int
	Incarnation int
	TxDB        *stmTxStateDB

	dbMutex sync.Mutex // 获取初始状态时需要

	accessAddress *types.AccessAddressMap
}

type stmTxStateDB struct {
	statedb *StmStateDB
	// This map holds 'live' objects, which will get modified while processing a state transition.
	stateObjects         map[common.Address]*stmTxStateObject
	stateObjectsDirty    map[common.Address]struct{} // State objects modified in the current execution
	stateObjectsDestruct map[common.Address]struct{} // State objects destructed in the block

	// The refund counter, also used by state transitioning.
	refund uint64

	thash   common.Hash
	txIndex int
	logs    map[common.Hash][]*types.Log
	logSize uint

	preimages map[common.Hash][]byte

	// Per-transaction access list
	accessList *accessList

	// Transient storage
	transientStorage transientStorage

	// Journal of state modifications. This is the backbone of
	// Snapshot and RevertToSnapshot.
	journal        *stmJournal
	validRevisions []revision
	nextRevisionId int
}

// NewStmTransaction creates a new state from a given trie.
func NewStmTransaction(tx *types.Transaction, index int, statedb *StmStateDB) *StmTransaction {
	stmTx := &StmTransaction{
		Tx:    tx,
		Index: index,
		TxDB: &stmTxStateDB{
			statedb:              statedb,
			stateObjects:         make(map[common.Address]*stmTxStateObject),
			stateObjectsDirty:    make(map[common.Address]struct{}),
			stateObjectsDestruct: make(map[common.Address]struct{}),
			logs:                 make(map[common.Hash][]*types.Log),
			preimages:            make(map[common.Hash][]byte),
			journal:              newStmJournal(),
			accessList:           newAccessList(),
			transientStorage:     newTransientStorage(),
		},
		accessAddress: types.NewAccessAddressMap(),
	}
	return stmTx
}

func (s *StmTransaction) AddLog(log *types.Log) {
	s.TxDB.journal.append(stmAddLogChange{txhash: s.Tx.Hash()})

	log.TxHash = s.Tx.Hash()
	log.TxIndex = uint(s.Index)
	log.Index = s.TxDB.logSize
	s.TxDB.logs[s.Tx.Hash()] = append(s.TxDB.logs[s.Tx.Hash()], log)
	s.TxDB.logSize++
}

// GetLogs returns the logs matching the specified transaction hash, and annotates
// them with the given blockNumber and blockHash.
func (s *StmTransaction) GetLogs(hash common.Hash, blockNumber uint64, blockHash common.Hash) []*types.Log {
	logs := s.TxDB.logs[hash]
	for _, l := range logs {
		l.BlockNumber = blockNumber
		l.BlockHash = blockHash
	}
	return logs
}

func (s *StmTransaction) Logs() []*types.Log {
	var logs []*types.Log
	for _, lgs := range s.TxDB.logs {
		logs = append(logs, lgs...)
	}
	return logs
}

// AddPreimage records a SHA3 preimage seen by the VM.
func (s *StmTransaction) AddPreimage(hash common.Hash, preimage []byte) {
	if _, ok := s.TxDB.preimages[hash]; !ok {
		s.TxDB.journal.append(stmAddPreimageChange{hash: hash})
		pi := make([]byte, len(preimage))
		copy(pi, preimage)
		s.TxDB.preimages[hash] = pi
	}
}

// Preimages returns a list of SHA3 preimages that have been submitted.
func (s *StmTransaction) Preimages() map[common.Hash][]byte {
	return s.TxDB.preimages
}

// AddRefund adds gas to the refund counter
func (s *StmTransaction) AddRefund(gas uint64) {
	s.TxDB.journal.append(stmRefundChange{prev: s.TxDB.refund})
	s.TxDB.refund += gas
}

// SubRefund removes gas from the refund counter.
// This method will panic if the refund counter goes below zero
func (s *StmTransaction) SubRefund(gas uint64) {
	s.TxDB.journal.append(stmRefundChange{prev: s.TxDB.refund})
	if gas > s.TxDB.refund {
		panic(fmt.Sprintf("Refund counter below zero (gas: %d > refund: %d)", gas, s.TxDB.refund))
	}
	s.TxDB.refund -= gas
}

// Exist reports whether the given account address exists in the state.
// Notably this also returns true for suicided accounts.
func (s *StmTransaction) Exist(addr common.Address) bool {
	addAccessAddr(s.accessAddress, addr, true, false)
	return s.getStateObject(addr) != nil
}

// Empty returns whether the state object is either non-existent
// or empty according to the EIP161 specification (balance = nonce = code = 0)
func (s *StmTransaction) Empty(addr common.Address) bool {
	addAccessAddr(s.accessAddress, addr, true, false)
	so := s.getStateObject(addr)
	return so == nil || so.empty()
}

// GetBalance retrieves the balance from the given address or 0 if object not found
func (s *StmTransaction) GetBalance(addr common.Address) *big.Int {
	addAccessAddr(s.accessAddress, addr, true, false)
	stmTxStateObject := s.getStateObject(addr)
	if stmTxStateObject != nil {
		return stmTxStateObject.Balance()
	}
	return common.Big0
}

func (s *StmTransaction) GetNonce(addr common.Address) uint64 {
	addAccessAddr(s.accessAddress, addr, true, false)
	stmTxStateObject := s.getStateObject(addr)
	if stmTxStateObject != nil {
		return stmTxStateObject.Nonce()
	}

	return 0
}

// TxIndex returns the current transaction index set by Prepare.
func (s *StmTransaction) TxIndex() int {
	return s.Index
}

func (s *StmTransaction) GetCode(addr common.Address) []byte {
	addAccessAddr(s.accessAddress, addr, true, false)
	stmTxStateObject := s.getStateObject(addr)
	if stmTxStateObject != nil {
		return stmTxStateObject.Code()
	}
	return nil
}

func (s *StmTransaction) GetCodeSize(addr common.Address) int {
	addAccessAddr(s.accessAddress, addr, true, false)
	stmTxStateObject := s.getStateObject(addr)
	if stmTxStateObject != nil {
		return stmTxStateObject.CodeSize()
	}
	return 0
}

func (s *StmTransaction) GetCodeHash(addr common.Address) common.Hash {
	addAccessAddr(s.accessAddress, addr, true, false)
	stmTxStateObject := s.getStateObject(addr)
	if stmTxStateObject == nil {
		return common.Hash{}
	}
	return common.BytesToHash(stmTxStateObject.CodeHash())
}

// GetState retrieves a value from the given account's storage trie.
func (s *StmTransaction) GetState(addr common.Address, hash common.Hash) common.Hash {
	addAccessSlot(s.accessAddress, addr, hash, true, s.Index)
	var stateHash common.Hash = common.Hash{}
	stmTxStateObject := s.getStateObject(addr)
	if stmTxStateObject != nil {
		//return stmTxStateObject.GetState(hash)
		stateHash = stmTxStateObject.GetState(hash)
	}
	return stateHash
}

// GetCommittedState retrieves a value from the given account's committed storage trie.
func (s *StmTransaction) GetCommittedState(addr common.Address, hash common.Hash) common.Hash {
	addAccessSlot(s.accessAddress, addr, hash, true, s.Index)
	stmTxStateObject := s.getStateObject(addr)
	if stmTxStateObject != nil {
		// 这里可能会有点问题
		return stmTxStateObject.GetCommittedState(hash)
	}
	return common.Hash{}
}

func (s *StmTransaction) HasSuicided(addr common.Address) bool {
	addAccessAddr(s.accessAddress, addr, true, false)
	stmTxStateObject := s.getStateObject(addr)
	if stmTxStateObject != nil {
		return stmTxStateObject.suicided
	}
	return false
}

/*
 * SETTERS
 */

// AddBalance adds amount to the account associated with addr.
func (s *StmTransaction) AddBalance(addr common.Address, amount *big.Int) {
	addAccessAddr(s.accessAddress, addr, false, false)
	stmTxStateObject := s.GetOrNewStateObject(addr)
	if stmTxStateObject != nil {
		stmTxStateObject.AddBalance(amount)
	}
}

// SubBalance subtracts amount from the account associated with addr.
func (s *StmTransaction) SubBalance(addr common.Address, amount *big.Int) {
	addAccessAddr(s.accessAddress, addr, false, false)
	stmTxStateObject := s.GetOrNewStateObject(addr)
	if stmTxStateObject != nil {
		stmTxStateObject.SubBalance(amount)
	}
}

func (s *StmTransaction) SetBalance(addr common.Address, amount *big.Int) {
	stmTxStateObject := s.GetOrNewStateObject(addr)
	if stmTxStateObject != nil {
		stmTxStateObject.SetBalance(amount)
	}
}

func (s *StmTransaction) SetNonce(addr common.Address, nonce uint64) {
	addAccessAddr(s.accessAddress, addr, false, false)
	stmTxStateObject := s.GetOrNewStateObject(addr)
	if stmTxStateObject != nil {
		stmTxStateObject.SetNonce(nonce)
	}
}

func (s *StmTransaction) SetCode(addr common.Address, code []byte) {
	addAccessAddr(s.accessAddress, addr, false, false)
	stmTxStateObject := s.GetOrNewStateObject(addr)
	if stmTxStateObject != nil {
		stmTxStateObject.SetCode(crypto.Keccak256Hash(code), code)
	}
}

func (s *StmTransaction) SetState(addr common.Address, key, value common.Hash) {
	addAccessSlot(s.accessAddress, addr, key, false, s.Index)
	stmTxStateObject := s.GetOrNewStateObject(addr)
	if stmTxStateObject != nil {
		stmTxStateObject.SetState(key, value)
	}
}

// SetStorage replaces the entire storage for the specified account with given
// storage. This function should only be used for debugging.
func (s *StmTransaction) SetStorage(addr common.Address, storage map[common.Hash]common.Hash) {
	// SetStorage needs to wipe existing storage. We achieve this by pretending
	// that the account self-destructed earlier in this block, by flagging
	// it in stateObjectsDestruct. The effect of doing so is that storage lookups
	// will not hit disk, since it is assumed that the disk-data is belonging
	// to a previous incarnation of the object.
	stmTxStateObject := s.GetOrNewStateObject(addr)
	for k, v := range storage {
		stmTxStateObject.SetState(k, v)
	}
}

// Suicide marks the given account as suicided.
// This clears the account balance.
//
// The account's state object is still available until the state is committed,
// getStateObject will return a non-nil account after Suicide.
func (s *StmTransaction) Suicide(addr common.Address) bool {
	addAccessAddr(s.accessAddress, addr, false, false)
	stmTxStateObject := s.getStateObject(addr)
	if stmTxStateObject == nil {
		return false
	}
	s.TxDB.journal.append(stmSuicideChange{
		account:     &addr,
		prev:        stmTxStateObject.suicided,
		prevbalance: new(big.Int).Set(stmTxStateObject.Balance()),
	})
	stmTxStateObject.markSuicided()
	stmTxStateObject.data.Balance = new(big.Int)

	return true
}

// SetTransientState sets transient storage for a given account. It
// adds the change to the journal so that it can be rolled back
// to its previous value if there is a revert.
func (s *StmTransaction) SetTransientState(addr common.Address, key, value common.Hash) {
	addAccessAddr(s.accessAddress, addr, true, false)
	prev := s.GetTransientState(addr, key)
	if prev == value {
		return
	}
	s.TxDB.journal.append(stmTransientStorageChange{
		account:  &addr,
		key:      key,
		prevalue: prev,
	})
	s.setTransientState(addr, key, value)
}

// setTransientState is a lower level setter for transient storage. It
// is called during a revert to prevent modifications to the journal.
func (s *StmTransaction) setTransientState(addr common.Address, key, value common.Hash) {
	s.TxDB.transientStorage.Set(addr, key, value)
}
func (sts *stmTxStateDB) setTransientState(addr common.Address, key, value common.Hash) {
	sts.transientStorage.Set(addr, key, value)
}

// GetTransientState gets transient storage for a given account.
func (s *StmTransaction) GetTransientState(addr common.Address, key common.Hash) common.Hash {
	addAccessAddr(s.accessAddress, addr, true, false)
	return s.TxDB.transientStorage.Get(addr, key)
}

//
// Setting, updating & deleting state object methods.
//

// 为了回滚
func (sts *stmTxStateDB) getStateObject(addr common.Address) *stmTxStateObject {
	obj := sts.stateObjects[addr]
	return obj
}

// getStateObject retrieves a state object given by the address, returning nil if
// the object is not found or was deleted in this execution context. If you need
// to differentiate between non-existent/just-deleted, use getDeletedStateObject.
func (s *StmTransaction) getStateObject(addr common.Address) *stmTxStateObject {
	if obj := s.getDeletedStateObject(addr); obj != nil && !obj.deleted {
		return obj
	}
	return nil
}

// getDeletedStateObject is similar to getStateObject, but instead of returning
// nil for a deleted state object, it returns the actual object with the deleted
// flag set. This is needed by the state journal to revert to the correct s-
// destructed object instead of wiping all knowledge about the state object.
func (s *StmTransaction) getDeletedStateObject(addr common.Address) *stmTxStateObject {
	// Prefer live objects if any is available
	if obj := s.TxDB.stateObjects[addr]; obj != nil {
		return obj
	}
	// 如果开始没取过来，那么就无了
	return nil
}

func (s *StmTransaction) setStateObject(object *stmTxStateObject) {
	s.TxDB.stateObjects[object.Address()] = object
}

// 为了回滚
func (sts *stmTxStateDB) setStateObject(object *stmTxStateObject) {
	sts.stateObjects[object.Address()] = object
}

// GetOrNewStateObject retrieves a state object or create a new state object if nil.
func (s *StmTransaction) GetOrNewStateObject(addr common.Address) *stmTxStateObject {
	stmTxStateObject := s.getStateObject(addr)
	if stmTxStateObject == nil {
		stmTxStateObject, _ = s.createObject(addr)
	}
	return stmTxStateObject
}

// createObject creates a new state object. If there is an existing account with
// the given address, it is overwritten and returned as the second return value.
func (s *StmTransaction) createObject(addr common.Address) (newobj, prev *stmTxStateObject) {
	prev = s.getDeletedStateObject(addr) // Note, prev might have been deleted, we need that!

	var prevdestruct bool
	if prev != nil {
		_, prevdestruct = s.TxDB.stateObjectsDestruct[prev.address]
		if !prevdestruct {
			s.TxDB.stateObjectsDestruct[prev.address] = struct{}{}
		}
	}

	newobj = newStmTxStateObject(s.TxDB, s.TxDB.statedb, addr, types.StateAccount{}, s.Index, s.Incarnation)
	if prev == nil {
		s.TxDB.journal.append(stmCreateObjectChange{account: &addr})
	} else {
		s.TxDB.journal.append(stmResetObjectChange{prev: prev, prevdestruct: prevdestruct})
	}
	s.setStateObject(newobj)
	if prev != nil && !prev.deleted {
		return newobj, prev
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
func (s *StmTransaction) CreateAccount(addr common.Address) {
	addAccessAddr(s.accessAddress, addr, false, false)
	newObj, prev := s.createObject(addr)
	if prev != nil {
		newObj.setBalance(prev.data.Balance)
	}
}

// Snapshot returns an identifier for the current revision of the state.
func (s *StmTransaction) Snapshot() int {
	id := s.TxDB.nextRevisionId
	s.TxDB.nextRevisionId++
	s.TxDB.validRevisions = append(s.TxDB.validRevisions, revision{id, s.TxDB.journal.length()})
	return id
}

// RevertToSnapshot reverts all state changes made since the given revision.
func (s *StmTransaction) RevertToSnapshot(revid int) {
	// Find the snapshot in the stack of valid snapshots.
	idx := sort.Search(len(s.TxDB.validRevisions), func(i int) bool {
		return s.TxDB.validRevisions[i].id >= revid
	})
	if idx == len(s.TxDB.validRevisions) || s.TxDB.validRevisions[idx].id != revid {
		panic(fmt.Errorf("revision id %v cannot be reverted", revid))
	}
	snapshot := s.TxDB.validRevisions[idx].journalIndex

	// Replay the journal to undo changes and remove invalidated snapshots
	s.TxDB.journal.revert(s.TxDB, snapshot)
	s.TxDB.validRevisions = s.TxDB.validRevisions[:idx]
}

// GetRefund returns the current value of the refund counter.
func (s *StmTransaction) GetRefund() uint64 {
	return s.TxDB.refund
}

// SetTxContext sets the current transaction hash and index which are
// used when the EVM emits new state logs. It should be invoked before
// transaction execution.
func (s *StmTransaction) SetTxContext(thash common.Hash, ti int) {
	s.TxDB.thash = thash
	s.TxDB.txIndex = ti
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
func (s *StmTransaction) Prepare(rules params.Rules, sender, coinbase common.Address, dst *common.Address, precompiles []common.Address, list types.AccessList) {
	if rules.IsBerlin {
		// Clear out any leftover from previous executions
		al := newAccessList()
		s.TxDB.accessList = al

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
	s.TxDB.transientStorage = newTransientStorage()
}

// AddAddressToAccessList adds the given address to the access list
func (s *StmTransaction) AddAddressToAccessList(addr common.Address) {
	if s.TxDB.accessList.AddAddress(addr) {
		s.TxDB.journal.append(stmAccessListAddAccountChange{&addr})
	}
}

// AddSlotToAccessList adds the given (address, slot)-tuple to the access list
func (s *StmTransaction) AddSlotToAccessList(addr common.Address, slot common.Hash) {
	addrMod, slotMod := s.TxDB.accessList.AddSlot(addr, slot)
	if addrMod {
		// In practice, this should not happen, since there is no way to enter the
		// scope of 'address' without having the 'address' become already added
		// to the access list (via call-variant, create, etc).
		// Better safe than sorry, though
		s.TxDB.journal.append(stmAccessListAddAccountChange{&addr})
	}
	if slotMod {
		s.TxDB.journal.append(stmAccessListAddSlotChange{
			address: &addr,
			slot:    &slot,
		})
	}
}

// AddressInAccessList returns true if the given address is in the access list.
func (s *StmTransaction) AddressInAccessList(addr common.Address) bool {
	return s.TxDB.accessList.ContainsAddress(addr)
}

// SlotInAccessList returns true if the given (address, slot)-tuple is in the access list.
func (s *StmTransaction) SlotInAccessList(addr common.Address, slot common.Hash) (addressPresent bool, slotPresent bool) {
	return s.TxDB.accessList.Contains(addr, slot)
}

func (s *StmTransaction) AccessAddress() *types.AccessAddressMap {
	return s.accessAddress
}

func (s *StmTransaction) GetAllState(tx *types.Transaction) {
	for addr, accessAddr := range *tx.AccessPre {
		stmObj := s.TxDB.statedb.getDeletedStateObject(addr)
		if stmObj != nil {
			txObj := newStmTxStateObject(s.TxDB, s.TxDB.statedb, addr, *(stmObj.data.Copy()), s.Index, s.Incarnation)
			txObj.dirtyCode = stmObj.dirtyCode
			txObj.suicided = stmObj.suicided
			txObj.deleted = stmObj.deleted
			s.setStateObject(txObj)

			// 该地址为合约地址，则需提取Code
			if !bytes.Equal(stmObj.data.CodeHash, types.EmptyCodeHash.Bytes()) {
				code, err := s.TxDB.statedb.db.ContractCode(stmObj.addrHash, common.BytesToHash(stmObj.data.CodeHash))
				if err != nil {
					s.TxDB.statedb.setError(fmt.Errorf("can't load code hash %x: %v", stmObj.data.CodeHash, err))
				}
				txObj.code = common.CopyBytes(code)
			}

			for key := range *accessAddr.Slots {
				value := stmObj.GetState(s.TxDB.statedb.db, key, tx.Index, -1)
				txObj.originStorage[key] = value
			}
			s.TxDB.stateObjects[addr] = txObj
		}

	}
}

func (s *StmTransaction) ReallyGetState(tx *types.Transaction, from common.Address) {
	for addr, accessAddr := range *tx.AccessPre {
		obj := s.TxDB.stateObjects[addr]
		if obj != nil {
			fmt.Println(obj.Nonce(), obj.Balance())

			for key := range *accessAddr.Slots {
				if value, ok := obj.originStorage[key]; ok {
					fmt.Println(key, value)
				} else {

					if addr == from {
						fmt.Println("no match address")
					}
				}
			}
		} else {
			if addr == from {
				fmt.Println("no match address")
			}

		}

	}
}

func (s *StmTransaction) GetAccountState(addr common.Address, accessAddr *types.AccessAddress) {
	s.dbMutex.Lock()
	defer s.dbMutex.Unlock()

	stmObj := s.TxDB.statedb.getDeletedStateObject(addr)
	if stmObj != nil {
		txObj := newStmTxStateObject(s.TxDB, s.TxDB.statedb, addr, *(stmObj.data.Copy()), s.Index, s.Incarnation)
		txObj.dirtyCode = stmObj.dirtyCode
		txObj.suicided = stmObj.suicided
		txObj.deleted = stmObj.deleted

		// 该地址为合约地址，则需提取Code
		if !bytes.Equal(stmObj.data.CodeHash, types.EmptyCodeHash.Bytes()) {
			code, err := s.TxDB.statedb.db.ContractCode(stmObj.addrHash, common.BytesToHash(stmObj.data.CodeHash))
			if err != nil {
				s.TxDB.statedb.setError(fmt.Errorf("can't load code hash %x: %v", stmObj.data.CodeHash, err))
			}
			txObj.code = common.CopyBytes(code)
		}

		for key := range *accessAddr.Slots {
			value := stmObj.GetState(s.TxDB.statedb.db, key, s.Index, -1)
			txObj.originStorage[key] = value
		}
		s.setStateObject(txObj)
	}
}

func (s *StmTransaction) Validation(deleteEmptyObjects bool) {
	valObjects := make(map[common.Address]*stmTxStateObject)
	for addr := range s.TxDB.journal.dirties {
		obj, exist := s.TxDB.stateObjects[addr]
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
		}
		valObjects[addr] = obj
	}
	s.TxDB.statedb.Validation(valObjects, s.Index, s.Incarnation)
}
