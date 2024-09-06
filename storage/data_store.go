package storage

import (
	"context"
	"errors"
	"time"
)

type persist interface {
	append(entryLog entryLog) error
	read(logs chan entryLog)
}

type DataStore struct {
	storage *storage
	backlog *backlog
	persist persist
}

func (d DataStore) Get(k string) (string, error) {
	return d.storage.get(k)
}

func (d DataStore) Promise(p Promise, k string) error {
	return d.backlog.promise(p, k)
}

func (d DataStore) Accept(p Promise, k, v string) error {
	return d.backlog.accept(p, k, v)
}

func (d DataStore) Commit(k string, p Promise) error {
	v, ok := d.backlog.accepts.Load(p)
	if !ok {
		return errors.New("error committing")
	}
	strVal, ok := v.(string)
	if !ok {
		return errors.New("error converting to string")
	}
	d.storage.set(k, strVal)
	d.persist.append(entryLog{SET_COMMAND, k, strVal})
	return nil
}

func (d DataStore) Rehydrate() {
	ch := make(chan entryLog)
	go d.persist.read(ch)
	for l := range ch {
		d.storage.set(l.key, l.value)
	}
}

func NewDataStore(ctx context.Context) DataStore {
	storage := newStorage()
	backlog := newBacklog()
	writeAheadLog := newWriteAheadLog(ctx, "/tmp/nimbus/data", 5*time.Second)
	return DataStore{storage, backlog, writeAheadLog}
}
