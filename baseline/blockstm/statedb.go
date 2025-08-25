package blockstm

import (
	"github.com/ethereum/go-ethereum/common"
	"sync"
)

type stateObject struct {
	address  common.Address
	versions []int // tx index is state version
}

func newStateObject(addr common.Address) *stateObject {
	return &stateObject{
		address:  addr,
		versions: make([]int, 0),
	}
}

func (so *stateObject) getState() int {
	if len(so.versions) == 0 {
		return -1
	}
	return so.versions[len(so.versions)-1]
}

// setState set tx index as version
func (so *stateObject) setState(version int) {
	so.versions = append(so.versions, version)
}

type stateDB struct {
	objectMutex  sync.RWMutex
	stateObjects map[common.Address]*stateObject
}

func newStateDB() *stateDB {
	return &stateDB{
		stateObjects: make(map[common.Address]*stateObject),
	}
}

func (s *stateDB) getState(addr common.Address) int {
	var version int = -1
	s.objectMutex.RLock()
	obj, ok := s.stateObjects[addr]
	if ok {
		version = obj.getState()
	}
	s.objectMutex.RUnlock()

	if !ok {
		obj = newStateObject(addr)
		s.setObject(addr, obj)
	}
	return version
}

func (s *stateDB) setObject(addr common.Address, obj *stateObject) {
	s.objectMutex.Lock()
	s.stateObjects[addr] = obj
	s.objectMutex.Unlock()
}

func (s *stateDB) validate(addr common.Address, preValue int) bool {
	var abort bool = false
	s.objectMutex.Lock()
	obj, _ := s.stateObjects[addr]
	if obj.getState() != preValue {
		abort = true
	}
	s.objectMutex.Unlock()
	return abort
}

func (s *stateDB) setState(addr common.Address, newValue int) {
	s.objectMutex.Lock()
	s.stateObjects[addr].setState(newValue)
	s.objectMutex.Unlock()
}

func (s *stateDB) abortCommitTx(addr common.Address, txIndex int) ([]int, int) {
	s.objectMutex.Lock()
	obj, _ := s.stateObjects[addr]
	i := len(obj.versions) - 1
	for ; i >= 0; i-- {
		if obj.versions[i] < txIndex {
			break
		}
	}
	obj.versions = obj.versions[:i+1]
	newVersion := obj.versions[i]
	abortTxs := obj.versions[i+1:]
	s.objectMutex.Unlock()
	return abortTxs, newVersion
}
