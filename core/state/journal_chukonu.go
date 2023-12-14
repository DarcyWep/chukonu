package state

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

type ChuKoNuStateObjectForJournal interface {
	OriginNonce() uint64
}

type ChuKoNuStateDBForJournal interface {
	StateObjects()
	appendJournal(journal *chuKoNuJournal, entry chuKoNuJournalEntry)
}

// chuKoNuJournalEntry is a modification entry in the state change chuKoNuJournal that can be
// reverted on demand.
type chuKoNuJournalEntry interface {
	// revert undoes the changes introduced by this chuKoNuJournal entry.
	revert(db ChuKoNuStateDBForJournal)

	// dirtied returns the Ethereum address modified by this chuKoNuJournal entry.
	dirtied() *common.Address
}

// chuKoNuJournal contains the list of state modifications applied since the last state
// commit. These are tracked to be able to be reverted in the case of an execution
// exception or request for reversal.
type chuKoNuJournal struct {
	entries []chuKoNuJournalEntry  // Current changes tracked by the chuKoNuJournal
	dirties map[common.Address]int // Dirty accounts and the number of changes
}

// newChuKoNuJournal creates a new initialized chuKoNuJournal.
func newChuKoNuJournal() *chuKoNuJournal {
	return &chuKoNuJournal{
		dirties: make(map[common.Address]int),
	}
}

// append inserts a new modification entry to the end of the change chuKoNuJournal.
func (j *chuKoNuJournal) append(entry chuKoNuJournalEntry) {
	j.entries = append(j.entries, entry)
	if addr := entry.dirtied(); addr != nil {
		j.dirties[*addr]++
	}
}

// revert undoes a batch of journalled modifications along with any reverted
// dirty handling too.
func (j *chuKoNuJournal) revert(statedb ChuKoNuStateDBForJournal, snapshot int) {
	for i := len(j.entries) - 1; i >= snapshot; i-- {
		// Undo the changes made by the operation
		j.entries[i].revert(statedb)

		// Drop any dirty tracking induced by the change
		if addr := j.entries[i].dirtied(); addr != nil {
			if j.dirties[*addr]--; j.dirties[*addr] == 0 {
				delete(j.dirties, *addr)
			}
		}
	}
	j.entries = j.entries[:snapshot]
}

// dirty explicitly sets an address to dirty, even if the change entries would
// otherwise suggest it as clean. This method is an ugly hack to handle the RIPEMD
// precompile consensus exception.
func (j *chuKoNuJournal) dirty(addr common.Address) {
	j.dirties[addr]++
}

// length returns the current number of entries in the chuKoNuJournal.
func (j *chuKoNuJournal) length() int {
	return len(j.entries)
}

type (
	// Changes to the account trie.
	chuKoNuCreateObjectChange struct {
		account *common.Address
	}
	chuKoNuResetObjectChange struct {
		prev         ChuKoNuStateObjectForJournal
		prevdestruct bool
	}
	chuKoNuSuicideChange struct {
		account     *common.Address
		prev        bool // whether account had already suicided
		prevbalance *big.Int
	}

	// Changes to individual accounts.
	chuKoNuBalanceChange struct {
		account *common.Address
		prev    *big.Int
	}
	chuKoNuNonceChange struct {
		account *common.Address
		prev    uint64
	}
	chuKoNuStorageChange struct {
		account       *common.Address
		key, prevalue common.Hash
	}
	chuKoNuCodeChange struct {
		account            *common.Address
		prevcode, prevhash []byte
	}

	// Changes to other state values.
	chuKoNuRefundChange struct {
		prev uint64
	}
	chuKoNuAddLogChange struct {
		txhash common.Hash
	}
	chuKoNuAddPreimageChange struct {
		hash common.Hash
	}
	chuKoNuTouchChange struct {
		account *common.Address
	}
	// Changes to the access list
	chuKoNuAccessListAddAccountChange struct {
		address *common.Address
	}
	chuKoNuAccessListAddSlotChange struct {
		address *common.Address
		slot    *common.Hash
	}

	chuKoNuTransientStorageChange struct {
		account       *common.Address
		key, prevalue common.Hash
	}
)

func (ch chuKoNuCreateObjectChange) revert(s ChuKoNuStateDBForJournal) {
	statedb, ok := s.(*ChuKoNuStateDB)
	if ok {
		delete(statedb.stateObjects, *ch.account)
		delete(statedb.stateObjectsDirty, *ch.account)
	} else {
		statedb1, _ := s.(*ChuKoNuTxStateDB)
		delete(statedb1.stateObjects, *ch.account)
		delete(statedb1.stateObjectsDirty, *ch.account)
	}
}

func (ch chuKoNuCreateObjectChange) dirtied() *common.Address {
	return ch.account
}

func (ch chuKoNuResetObjectChange) revert(s ChuKoNuStateDBForJournal) {
	statedb, ok := s.(*ChuKoNuStateDB)
	if ok {
		obj := ch.prev.(*ChuKoNuStateObject)
		statedb.setStateObject(obj)
		if !ch.prevdestruct {
			delete(statedb.stateObjectsDestruct, obj.address)
		}
	} else {
		statedb1, _ := s.(*ChuKoNuTxStateDB)
		obj := ch.prev.(*ChuKoNuTxStateObject)
		statedb1.setStateObject(obj)
		if !ch.prevdestruct {
			delete(statedb1.stateObjectsDestruct, obj.address)
		}
	}

}

func (ch chuKoNuResetObjectChange) dirtied() *common.Address {
	return nil
}

func (ch chuKoNuSuicideChange) revert(s ChuKoNuStateDBForJournal) {
	statedb, ok := s.(*ChuKoNuStateDB)
	if ok {
		obj := statedb.getStateObject(*ch.account)
		if obj != nil {
			obj.suicided = ch.prev
			obj.setBalance(ch.prevbalance)
		}
	} else {
		statedb1, _ := s.(*ChuKoNuTxStateDB)
		obj := statedb1.getStateObject(*ch.account)
		if obj != nil {
			obj.suicided = ch.prev
			obj.setBalance(ch.prevbalance)
		}
	}
}

func (ch chuKoNuSuicideChange) dirtied() *common.Address {
	return ch.account
}

func (ch chuKoNuTouchChange) revert(s ChuKoNuStateDBForJournal) {
}

func (ch chuKoNuTouchChange) dirtied() *common.Address {
	return ch.account
}

func (ch chuKoNuBalanceChange) revert(s ChuKoNuStateDBForJournal) {
	statedb, ok := s.(*ChuKoNuStateDB)
	if ok {
		statedb.getStateObject(*ch.account).setBalance(ch.prev)
	} else {
		statedb1, _ := s.(*ChuKoNuTxStateDB)
		statedb1.getStateObject(*ch.account).setBalance(ch.prev)
	}
}

func (ch chuKoNuBalanceChange) dirtied() *common.Address {
	return ch.account
}

func (ch chuKoNuNonceChange) revert(s ChuKoNuStateDBForJournal) {
	statedb, ok := s.(*ChuKoNuStateDB)
	if ok {
		statedb.getStateObject(*ch.account).setNonce(ch.prev)
	} else {
		statedb1, _ := s.(*ChuKoNuTxStateDB)
		statedb1.getStateObject(*ch.account).setNonce(ch.prev)
	}
}

func (ch chuKoNuNonceChange) dirtied() *common.Address {
	return ch.account
}

func (ch chuKoNuCodeChange) revert(s ChuKoNuStateDBForJournal) {
	statedb, ok := s.(*ChuKoNuStateDB)
	if ok {
		statedb.getStateObject(*ch.account).setCode(common.BytesToHash(ch.prevhash), ch.prevcode)
	} else {
		statedb1, _ := s.(*ChuKoNuTxStateDB)
		statedb1.getStateObject(*ch.account).setCode(common.BytesToHash(ch.prevhash), ch.prevcode)
	}
}

func (ch chuKoNuCodeChange) dirtied() *common.Address {
	return ch.account
}

func (ch chuKoNuStorageChange) revert(s ChuKoNuStateDBForJournal) {
	statedb, ok := s.(*ChuKoNuStateDB)
	if ok {
		statedb.getStateObject(*ch.account).setState(ch.key, ch.prevalue)
	} else {
		statedb1, _ := s.(*ChuKoNuTxStateDB)
		statedb1.getStateObject(*ch.account).setState(ch.key, ch.prevalue)
	}
}

func (ch chuKoNuStorageChange) dirtied() *common.Address {
	return ch.account
}

func (ch chuKoNuTransientStorageChange) revert(s ChuKoNuStateDBForJournal) {
	statedb, ok := s.(*ChuKoNuStateDB)
	if ok {
		statedb.setTransientState(*ch.account, ch.key, ch.prevalue)
	} else {
		statedb1, _ := s.(*ChuKoNuTxStateDB)
		statedb1.setTransientState(*ch.account, ch.key, ch.prevalue)
	}
}

func (ch chuKoNuTransientStorageChange) dirtied() *common.Address {
	return nil
}

func (ch chuKoNuRefundChange) revert(s ChuKoNuStateDBForJournal) {
	statedb, ok := s.(*ChuKoNuStateDB)
	if ok {
		statedb.refund = ch.prev
	} else {
		statedb1, _ := s.(*ChuKoNuTxStateDB)
		statedb1.refund = ch.prev
	}
}

func (ch chuKoNuRefundChange) dirtied() *common.Address {
	return nil
}

func (ch chuKoNuAddLogChange) revert(s ChuKoNuStateDBForJournal) {
	statedb, ok := s.(*ChuKoNuStateDB)
	if ok {
		logs := statedb.logs[ch.txhash]
		if len(logs) == 1 {
			delete(statedb.logs, ch.txhash)
		} else {
			statedb.logs[ch.txhash] = logs[:len(logs)-1]
		}
		statedb.logSize--
	} else {
		statedb1, _ := s.(*ChuKoNuTxStateDB)
		logs := statedb1.logs[ch.txhash]
		if len(logs) == 1 {
			delete(statedb1.logs, ch.txhash)
		} else {
			statedb1.logs[ch.txhash] = logs[:len(logs)-1]
		}
		statedb1.logSize--
	}
}

func (ch chuKoNuAddLogChange) dirtied() *common.Address {
	return nil
}

func (ch chuKoNuAddPreimageChange) revert(s ChuKoNuStateDBForJournal) {
	statedb, ok := s.(*ChuKoNuStateDB)
	if ok {
		delete(statedb.preimages, ch.hash)
	} else {
		statedb1, _ := s.(*ChuKoNuTxStateDB)
		delete(statedb1.preimages, ch.hash)
	}

}

func (ch chuKoNuAddPreimageChange) dirtied() *common.Address {
	return nil
}

func (ch chuKoNuAccessListAddAccountChange) revert(s ChuKoNuStateDBForJournal) {
	/*
		One important invariant here, is that whenever a (addr, slot) is added, if the
		addr is not already present, the add causes two chuKoNuJournal entries:
		- one for the address,
		- one for the (address,slot)
		Therefore, when unrolling the change, we can always blindly delete the
		(addr) at this point, since no storage adds can remain when come upon
		a single (addr) change.
	*/
	statedb, ok := s.(*ChuKoNuStateDB)
	if ok {
		statedb.accessList.DeleteAddress(*ch.address)
	} else {
		statedb1, _ := s.(*ChuKoNuTxStateDB)
		statedb1.accessList.DeleteAddress(*ch.address)
	}
}

func (ch chuKoNuAccessListAddAccountChange) dirtied() *common.Address {
	return nil
}

func (ch chuKoNuAccessListAddSlotChange) revert(s ChuKoNuStateDBForJournal) {
	statedb, ok := s.(*ChuKoNuStateDB)
	if ok {
		statedb.accessList.DeleteSlot(*ch.address, *ch.slot)
	} else {
		statedb1, _ := s.(*ChuKoNuTxStateDB)
		statedb1.accessList.DeleteSlot(*ch.address, *ch.slot)
	}
}

func (ch chuKoNuAccessListAddSlotChange) dirtied() *common.Address {
	return nil
}
