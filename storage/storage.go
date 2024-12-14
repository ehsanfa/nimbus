package storage

import (
	"errors"
	"fmt"
	"sync"
)

type storage struct {
	store      map[string][]byte
	storeMutex sync.RWMutex
}

func (s *storage) set(k string, v []byte) error {
	s.storeMutex.Lock()
	defer s.storeMutex.Unlock()
	s.store[k] = v
	return nil
}

func (s *storage) get(k string) ([]byte, error) {
	s.storeMutex.RLock()
	val, ok := s.store[k]
	s.storeMutex.RUnlock()
	if !ok {
		return make([]byte, 0), errors.New("key not found")
	}
	fmt.Println(k, string(val))
	return val, nil
}

func newStorage() *storage {
	return &storage{store: make(map[string][]byte)}
}
