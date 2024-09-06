package main

import (
	"context"
	"fmt"
	"net"

	pbds "github.com/ehsanfa/nimbus/datastore"
	"github.com/ehsanfa/nimbus/storage"
)

type dataStore struct {
	context   context.Context
	dataStore storage.DataStore
	listener  net.Listener
}

type dataStoreServer struct {
	pbds.UnimplementedDataStoreServiceServer
	d dataStore
}

func (ds *dataStoreServer) Get(ctx context.Context, in *pbds.GetRequest) (*pbds.GetResponse, error) {
	resp := &pbds.GetResponse{}
	val, err := ds.d.dataStore.Get(in.Key)
	if err != nil {
		resp.Ok = false
		resp.Error = err.Error()
	} else {
		resp.Ok = true
	}
	resp.Value = val
	fmt.Println("get", in.Key, val)
	return resp, nil
}

func (ds *dataStoreServer) Prepare(ctx context.Context, in *pbds.PrepareRequest) (*pbds.PrepareResponse, error) {
	resp := &pbds.PrepareResponse{}
	err := ds.d.dataStore.Promise(storage.Promise(in.Proposal), in.Key)
	if err != nil {
		fmt.Println("error prepare", err)
		resp.Promised = false
		resp.Error = err.Error()
		return resp, err
	}
	resp.Promised = true
	return resp, nil
}

func (ds *dataStoreServer) Accept(ctx context.Context, in *pbds.AcceptRequest) (*pbds.AcceptResponse, error) {
	resp := &pbds.AcceptResponse{}
	err := ds.d.dataStore.Accept(storage.Promise(in.Proposal), in.Key, in.Value)
	if err != nil {
		fmt.Println("error accept", err)
		resp.Error = err.Error()
		resp.Accepted = false
		return resp, err
	}
	resp.Accepted = true
	return resp, nil
}

func (ds *dataStoreServer) Commit(ctx context.Context, in *pbds.CommitRequest) (*pbds.CommitResponse, error) {
	resp := &pbds.CommitResponse{}
	err := ds.d.dataStore.Commit(in.Key, storage.Promise(in.Proposal))
	if err != nil {
		resp.Committed = false
		return resp, err
	}
	resp.Committed = true
	return resp, nil
}

func NewDataStoreServer(
	ctx context.Context,
	listener net.Listener,
) *dataStoreServer {
	d := dataStore{
		context:   ctx,
		dataStore: storage.NewDataStore(ctx),
		listener:  listener,
	}
	return &dataStoreServer{d: d}
}
