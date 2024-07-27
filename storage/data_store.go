package storage

import "errors"

type DataStore struct {
	storage *storage
	backlog *backlog
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
	d.storage.set(k, v.(string))
	if !ok {
		return errors.New("error committing")
	}
	return nil
}

func NewDataStore() DataStore {
	storage := newStorage()
	backlog := newBacklog()
	return DataStore{storage, backlog}
}
