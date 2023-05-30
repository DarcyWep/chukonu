package optimistic

import (
	"github.com/ethereum/go-ethereum/common"
	"sync"
)

type stateObject struct {
	address  common.Address
	versions []int
}

func newStateObject(addr common.Address) *stateObject {
	return &stateObject{
		address:  addr,
		versions: make([]int, 0),
	}
}

func (so *stateObject) getState() int {
	return len(so.versions)
}

func (so *stateObject) setState() {
	so.versions = append(so.versions, len(so.versions))
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
	var version int = 0
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

func (s *stateDB) setState(addr common.Address, preValue int) bool {
	var abort bool = false
	s.objectMutex.Lock()
	obj, _ := s.stateObjects[addr]
	if obj.getState() != preValue {
		abort = true
	} else {
		obj.setState()
	}
	s.objectMutex.Unlock()
	return abort
}
