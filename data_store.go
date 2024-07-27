package main

import (
	"context"
	"fmt"
	"net"

	"github.com/ehsanfa/nimbus/storage"
)

type DataStore struct {
	context   context.Context
	dataStore storage.DataStore
	listener  net.Listener
}

type GetRequest struct {
	Key string
}

type GetResponse struct {
	Value string
	Ok    bool
	Error string
}

func (d DataStore) Get(req *GetRequest, resp *GetResponse) error {
	val, err := d.dataStore.Get(req.Key)
	if err != nil {
		resp.Ok = false
		resp.Error = err.Error()
	} else {
		resp.Ok = true
	}
	resp.Value = val
	fmt.Println("get", req.Key, val)
	return nil
}

type PrepareRequest struct {
	Key      string
	Proposal int64
}

type PrepareResponse struct {
	Promised bool
	Error    string
}

func (d DataStore) Prepare(req *PrepareRequest, resp *PrepareResponse) error {
	err := d.dataStore.Promise(storage.Promise(req.Proposal), req.Key)
	if err != nil {
		fmt.Println("error prepare", err)
		resp.Promised = false
		resp.Error = err.Error()
		return err
	}
	resp.Promised = true
	return nil
}

type AcceptRequest struct {
	Key      string
	Proposal int64
	Value    string
}

type AcceptResponse struct {
	Accepted bool
	Error    string
}

func (d DataStore) Accept(req *AcceptRequest, resp *AcceptResponse) error {
	err := d.dataStore.Accept(storage.Promise(req.Proposal), req.Key, req.Value)
	if err != nil {
		fmt.Println("error accept", err)
		resp.Error = err.Error()
		resp.Accepted = false
		return err
	}
	resp.Accepted = true
	return nil
}

type CommitRequest struct {
	Key      string
	Proposal int64
}

type CommitResponse struct {
	Committed bool
}

func (d DataStore) Commit(req *CommitRequest, resp *CommitResponse) error {
	err := d.dataStore.Commit(req.Key, storage.Promise(req.Proposal))
	if err != nil {
		resp.Committed = false
		return err
	}
	resp.Committed = true
	return nil
}

func NewDataStore(
	ctx context.Context,
	listener net.Listener,
) DataStore {
	return DataStore{
		context:   ctx,
		dataStore: storage.NewDataStore(),
		listener:  listener,
	}
}
