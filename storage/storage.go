package storage

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

type storage struct {
	store sync.Map
	hits  int64
}

func (s *storage) set(k, v string) error {
	s.store.Store(k, v)
	s.hits += 1
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
	s := &storage{}
	go func() {
		for {
			select {
			case <-time.NewTicker(5 * time.Second).C:
				fmt.Println(s.hits)
			}
		}
	}()
	return s
}
