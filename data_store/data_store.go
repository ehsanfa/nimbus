package datastore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/ehsanfa/nimbus/cluster"
	connectionpool "github.com/ehsanfa/nimbus/connection_pool"
	"github.com/ehsanfa/nimbus/storage"
)

type DataStore struct {
	stg     *storage.DataStore
	cp      *connectionpool.ConnectionPool
	cluster *cluster.Cluster
}

const (
	IDENTIFIER_DATA_STORE_PREPARE_REQUEST byte = iota + 11
	IDENTIFIER_DATA_STORE_PREPARE_RESPONSE
	IDENTIFIER_DATA_STORE_ACCEPT_REQUEST
	IDENTIFIER_DATA_STORE_ACCEPT_RESPONSE
	IDENTIFIER_DATA_STORE_COMMIT_REQUEST
	IDENTIFIER_DATA_STORE_COMMIT_RESPONSE
	IDENTIFIER_DATA_STORE_GET_REQUEST
	IDENTIFIER_DATA_STORE_GET_RESPONSE
)

func (ds *DataStore) Prepare(ctx context.Context, node *cluster.Node, key []byte, proposal uint64) error {
	if ds.cluster.CurrentNode() == node {
		_, err := ds.prepare(ctx, key, proposal)
		return err
	}
	client, err := ds.cp.GetClient(node.DataStoreAddress)
	if err != nil {
		return err
	}
	pr := prepareRequest{identifier: IDENTIFIER_DATA_STORE_PREPARE_REQUEST, key: key, proposal: proposal}
	err = pr.encode(ctx, client)
	if err != nil {
		return err
	}
	resp, err := decodePrepareResponse(client)
	if resp == nil {
		return err
	}
	if !resp.promised {
		return errors.New(string(resp.err))
	}
	return nil
}

func (ds *DataStore) Accept(ctx context.Context, node *cluster.Node, key []byte, proposal uint64, value []byte) error {
	if ds.cluster.CurrentNode() == node {
		_, err := ds.accept(ctx, key, proposal, value)
		return err
	}
	client, err := ds.cp.GetClient(node.DataStoreAddress)
	if err != nil {
		return err
	}
	ar := acceptRequest{identifier: IDENTIFIER_DATA_STORE_ACCEPT_REQUEST, key: key, proposal: proposal, value: value}
	err = ar.encode(ctx, client)
	if err != nil {
		return err
	}
	resp, err := decodeAcceptResponse(client)
	if resp == nil {
		return err
	}
	if !resp.accepted {
		return errors.New(string(resp.err))
	}
	return nil
}

func (ds *DataStore) Commit(ctx context.Context, node *cluster.Node, key []byte, proposal uint64) error {
	if ds.cluster.CurrentNode() == node {
		_, err := ds.commit(ctx, key, proposal)
		return err
	}
	client, err := ds.cp.GetClient(node.DataStoreAddress)
	if err != nil {
		return err
	}
	cr := commitRequest{identifier: IDENTIFIER_DATA_STORE_COMMIT_REQUEST, key: key, proposal: proposal}
	err = cr.encode(ctx, client)
	if err != nil {
		return err
	}
	resp, err := decodeCommitResponse(client)
	if resp == nil {
		return err
	}
	if !resp.committed {
		return errors.New(string(resp.err))
	}
	return nil
}

func (ds *DataStore) Get(ctx context.Context, node *cluster.Node, key []byte) ([]byte, error) {
	if ds.cluster.CurrentNode() == node {
		return ds.stg.Get(string(key))
	}
	client, err := ds.cp.GetClient(node.DataStoreAddress)
	if err != nil {
		return make([]byte, 0), err
	}
	gr := getRequest{identifier: IDENTIFIER_DATA_STORE_GET_REQUEST, key: key}
	err = gr.encode(ctx, client)
	if err != nil {
		return make([]byte, 0), err
	}
	fmt.Println("wrote to the node", node.GossipAddress)
	var l uint32
	err = binary.Read(client, binary.BigEndian, &l)
	if err != nil {
		return make([]byte, 0), err
	}
	b := make([]byte, l)
	_, err = client.Read(b)
	if err != nil {
		return make([]byte, 0), err
	}
	buf := bytes.NewBuffer(b)
	resp, err := decodeGetResponse(buf)
	if err != nil {
		return make([]byte, 0), err
	}
	fmt.Println("got resp from node", resp, node.GossipAddress)
	if resp == nil {
		return make([]byte, 0), errors.New("empty response")
	}
	fmt.Println("datastore", key, resp.value)
	return resp.value, nil
}

func (ds *DataStore) prepare(ctx context.Context, key []byte, proposal uint64) (bool, error) {
	err := ds.stg.Promise(storage.Promise(proposal), string(key))
	if err != nil {
		return false, err
	}
	return true, nil
}

func (ds *DataStore) accept(ctx context.Context, key []byte, proposal uint64, value []byte) (bool, error) {
	err := ds.stg.Accept(storage.Promise(proposal), string(key), value)
	if err != nil {
		return false, err
	}
	return true, nil
}

func (ds *DataStore) commit(ctx context.Context, key []byte, proposal uint64) (bool, error) {
	err := ds.stg.Commit(string(key), storage.Promise(proposal))
	if err != nil {
		return false, err
	}
	return true, nil
}

func (ds *DataStore) get(ctx context.Context, key []byte) ([]byte, error) {
	val, err := ds.stg.Get(string(key))
	if err != nil {
		return make([]byte, 0), err
	}
	return val, nil
}

func (ds *DataStore) handleIncoming(ctx context.Context, r io.Reader, w io.Writer, identifier byte) error {
	switch identifier {
	case IDENTIFIER_DATA_STORE_PREPARE_REQUEST:
		pr, err := decodePrepareRequest(r)
		if err != nil {
			return err
		}
		promised, err := ds.prepare(ctx, pr.key, pr.proposal)
		errString := ""
		if err != nil {
			errString = err.Error()
		}
		resp := prepareResponse{identifier: IDENTIFIER_DATA_STORE_PREPARE_RESPONSE, promised: promised, err: []byte(errString)}
		return resp.encode(ctx, w)
	case IDENTIFIER_DATA_STORE_ACCEPT_REQUEST:
		ar, err := decodeAcceptRequest(r)
		if err != nil {
			return err
		}
		accepted, err := ds.accept(ctx, ar.key, ar.proposal, ar.value)
		errString := make([]byte, 0)
		if err != nil {
			errString = []byte(err.Error())
		}
		resp := acceptResponse{identifier: IDENTIFIER_DATA_STORE_ACCEPT_RESPONSE, accepted: accepted, err: errString}
		return resp.encode(ctx, w)
	case IDENTIFIER_DATA_STORE_COMMIT_REQUEST:
		cr, err := decodeCommitRequest(r)
		if err != nil {
			return err
		}

		committed, err := ds.commit(ctx, cr.key, cr.proposal)
		errString := make([]byte, 0)
		if err != nil {
			errString = []byte(err.Error())
		}
		resp := commitResponse{identifier: IDENTIFIER_DATA_STORE_COMMIT_RESPONSE, committed: committed, err: errString}
		return resp.encode(ctx, w)
	case IDENTIFIER_DATA_STORE_GET_REQUEST:
		gr, err := decodeGetRequest(r)
		if err != nil {
			return err
		}
		val, err := ds.get(ctx, gr.key)
		errString := make([]byte, 0)
		if err != nil {
			errString = []byte(err.Error())
		}
		resp := getResponse{value: val, err: errString}
		return resp.encode(ctx, w)
	}

	return nil
}

func NewDataStore(
	ctx context.Context,
	stg *storage.DataStore,
	clstr *cluster.Cluster,
	cp *connectionpool.ConnectionPool,
) *DataStore {
	ds := &DataStore{
		stg:     stg,
		cp:      cp,
		cluster: clstr,
	}

	go ds.serve(ctx, clstr.CurrentNode().DataStoreAddress)

	return ds
}
