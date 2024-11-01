package storage

import (
	"errors"
	"sync"
)

type Promise int64

type backlog struct {
	promises     map[string]Promise
	accepts      map[Promise][]byte
	promiseMutex sync.RWMutex
	acceptMutex  sync.RWMutex
}

func (b *backlog) promise(proposal Promise, key string) error {
	lp := b.lastPromise(key)
	if lp > proposal {
		return errors.New("already promised a higher proposal")
	}
	b.promiseMutex.Lock()
	b.promises[key] = proposal
	b.promiseMutex.Unlock()
	return nil
}

func (b *backlog) lastPromise(key string) Promise {
	b.promiseMutex.RLock()
	p, ok := b.promises[key]
	b.promiseMutex.RUnlock()
	if !ok {
		return 0
	}
	return p
}

func (b *backlog) accept(proposal Promise, key string, value []byte) error {
	lp := b.lastPromise(key)
	if lp > proposal {
		return errors.New("already promised a higher proposal")
	}
	if lp == 0 {
		return errors.New("no promise found for this key")
	}
	b.acceptMutex.Lock()
	b.accepts[proposal] = value
	b.acceptMutex.Unlock()
	return nil
}

func (b *backlog) getValue(proposal Promise) ([]byte, bool) {
	b.acceptMutex.RLock()
	defer b.acceptMutex.RUnlock()
	v, ok := b.accepts[proposal]
	return v, ok
}

func newBacklog() *backlog {
	return &backlog{
		promises: make(map[string]Promise),
		accepts:  make(map[Promise][]byte),
	}
}
