package state

import (
	"chukonu/core/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"math/big"
)

type StateTokenToTxDB struct {
	address  common.Address
	data     *types.StateAccount
	code     []byte
	storages Storage
	deleted  bool
}

func NewStateTokenToTxDB(addr common.Address, accountObj *ChuKoNuStateObject, slots []common.Hash, deleted bool) *StateTokenToTxDB {
	st := &StateTokenToTxDB{
		address:  addr,
		data:     accountObj.data.Copy(),
		code:     common.CopyBytes(accountObj.code),
		storages: make(Storage),
		deleted:  deleted,
	}
	for _, slot := range slots { // 空的也记录，执行时可以不访问StateDB
		st.storages[slot] = accountObj.GetState(accountObj.db.db, slot)
	}
	return st
}

func (st *StateTokenToTxDB) UpdateTxState(txdb *ChuKoNuTxStateDB) {
	obj := NewChuKoNuTxObject(txdb, txdb.statedb, st.address, *st.data, true, st.code, st.deleted)
	for key, val := range st.storages {
		obj.originStorage[key] = val
		obj.originStableStorage[key] = val
	}
	txdb.setStateObject(obj)
}

type StateTokenToStateDB struct {
	address  common.Address
	data     *types.StateAccount
	code     []byte
	storages Storage
	suicided bool
}

func NewStateTokenToAccountState(addr common.Address, data *types.StateAccount, storages Storage, code []byte, suicided bool) *StateTokenToStateDB {
	return &StateTokenToStateDB{
		address:  addr,
		data:     data,
		code:     code,
		storages: storages.Copy(),
		suicided: suicided,
	}
}

func (st *StateTokenToStateDB) UpdateAccountState(accountObject *ChuKoNuStateObject) {
	if st.data != nil {
		accountObject.dirty = true
		accountObject.data.Nonce = st.data.Nonce
		accountObject.data.Balance = new(big.Int).Set(st.data.Balance)
	}
	if st.code != nil {
		accountObject.dirty = true
		accountObject.setCode(crypto.Keccak256Hash(st.code), st.code)
	}
	if st.suicided {
		accountObject.dirty = true
		accountObject.markSuicided()
		accountObject.deleted = true
	}
	for key, val := range st.storages {
		accountObject.dirtyStorage[key] = val
	}
}

func (st *StateTokenToStateDB) deepCopy() *StateTokenToStateDB {
	stoken := &StateTokenToStateDB{
		address:  st.address,
		data:     nil,
		code:     nil,
		storages: st.storages.Copy(),
		suicided: st.suicided,
	}
	if st.data != nil {
		stoken.data = st.data.Copy()
	}
	if st.code != nil {
		stoken.code = common.CopyBytes(st.code)
	}
	return stoken
}
