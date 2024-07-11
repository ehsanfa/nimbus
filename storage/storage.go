package storage

import (
	"errors"
	"sync"
)

type Storage struct {
	store map[string]string
	mu    sync.RWMutex
}

func (s *Storage) Set(k, v string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store[k] = v
	return nil
}

func (s *Storage) Get(k string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	val, ok := s.store[k]
	if !ok {
		return "", errors.New("Key not found")
	}
	return val, nil
}

func NewStorage() Storage {
	return Storage{store: make(map[string]string)}
}
