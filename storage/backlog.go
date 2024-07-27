package storage

import (
	"errors"
	"fmt"
	"sync"
)

type Promise int64

type backlog struct {
	promises sync.Map
	accepts  sync.Map
}

func (b *backlog) promise(proposal Promise, key string) error {
	lp := b.lastPromise(key)
	if lp > proposal {
		fmt.Println(proposal, key, lp)
		return errors.New("already promised a higher proposal")
	}
	b.promises.Store(key, proposal)
	return nil
}

func (b *backlog) lastPromise(key string) Promise {
	k, ok := b.promises.Load(key)
	if !ok {
		return 0
	}
	return k.(Promise)
}

func (b *backlog) accept(proposal Promise, key, value string) error {
	lp := b.lastPromise(key)
	if lp > proposal {
		fmt.Println(proposal, key, value, lp)
		return errors.New("already promised a higher proposal")
	}
	b.accepts.Store(proposal, value)
	return nil
}

func newBacklog() *backlog {
	return &backlog{}
}
