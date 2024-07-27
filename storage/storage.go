package storage

import (
	"errors"
	"sync"
)

type storage struct {
	store sync.Map
}

func (s *storage) set(k, v string) error {
	s.store.Store(k, v)
	return nil
}

func (s *storage) get(k string) (string, error) {
	val, ok := s.store.Load(k)
	if !ok {
		return "", errors.New("key not found")
	}
	return val.(string), nil
}

func newStorage() *storage {
	return &storage{}
}
