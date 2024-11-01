package datastore

import (
	"context"
	"errors"
	"io"

	"github.com/ehsanfa/nimbus/cluster"
	connectionpool "github.com/ehsanfa/nimbus/connection_pool"
	"github.com/ehsanfa/nimbus/storage"
)

type DataStore struct {
	stg            *storage.DataStore
	cp             *connectionpool.ConnectionPool
	cluster        *cluster.Cluster
	prepareReqChan chan prepareRequest
	acceptReqChan  chan acceptRequest
	commitReqChan  chan commitRequest
}

const (
	IDENTIFIER_DATA_STORE_PREPARE_REQUEST byte = iota + 11
	IDENTIFIER_DATA_STORE_PREPARE_RESPONSE
	IDENTIFIER_DATA_STORE_ACCEPT_REQUEST
	IDENTIFIER_DATA_STORE_ACCEPT_RESPONSE
	IDENTIFIER_DATA_STORE_COMMIT_REQUEST
	IDENTIFIER_DATA_STORE_COMMIT_RESPONSE
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
	pr := prepareRequest{identifier: 11, key: key, proposal: proposal}
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
	ar := acceptRequest{identifier: 13, key: key, proposal: proposal, value: value}
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
	cr := commitRequest{identifier: 15, key: key, proposal: proposal}
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

// func (ds *dataStoreServer) Get(ctx context.Context, in *pbds.GetRequest) (*pbds.GetResponse, error) {
// 	resp := &pbds.GetResponse{}
// 	val, err := ds.d.dataStore.Get(in.Key)
// 	if err != nil {
// 		resp.Ok = false
// 		resp.Error = err.Error()
// 	} else {
// 		resp.Ok = true
// 	}
// 	resp.Value = val
// 	fmt.Println("get", in.Key, val)
// 	return resp, nil
// }

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

func (ds *DataStore) handlePrepares(ctx context.Context) {
	defer close(ds.prepareReqChan)

	for {
		select {
		case pr := <-ds.prepareReqChan:
			promised, err := ds.prepare(ctx, pr.key, pr.proposal)
			errString := ""
			if err != nil {
				errString = err.Error()
			}
			pr.replyTo <- prepareResponse{identifier: IDENTIFIER_DATA_STORE_PREPARE_RESPONSE, promised: promised, err: []byte(errString)}
		case <-ctx.Done():
			return
		}
	}
}

func (ds *DataStore) handleAccepts(ctx context.Context) {
	defer close(ds.acceptReqChan)

	for {
		select {
		case ar := <-ds.acceptReqChan:
			accepted, err := ds.accept(ctx, ar.key, ar.proposal, ar.value)
			errString := make([]byte, 0)
			if err != nil {
				errString = []byte(err.Error())
			}
			ar.replyTo <- acceptResponse{identifier: IDENTIFIER_DATA_STORE_ACCEPT_RESPONSE, accepted: accepted, err: errString}
		case <-ctx.Done():
			return
		}
	}
}

func (ds *DataStore) handleCommits(ctx context.Context) {
	defer close(ds.commitReqChan)

	for {
		select {
		case cr := <-ds.commitReqChan:
			committed, err := ds.commit(ctx, cr.key, cr.proposal)
			errString := make([]byte, 0)
			if err != nil {
				errString = []byte(err.Error())
			}
			cr.replyTo <- commitResponse{identifier: IDENTIFIER_DATA_STORE_COMMIT_RESPONSE, committed: committed, err: errString}
		case <-ctx.Done():
			return
		}
	}
}

func (ds *DataStore) handleIncoming(ctx context.Context, r io.Reader, w io.Writer, identifier byte) error {
	switch identifier {
	case 11:
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
		// pr.replyTo = make(chan prepareResponse)
		// defer close(pr.replyTo)
		// ds.prepareReqChan <- *pr
		// resp := <-pr.replyTo
		return resp.encode(ctx, w)
	case 12:
		// prepare response
	case 13:
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
		// ar.replyTo = make(chan acceptResponse)
		// defer close(ar.replyTo)
		// ds.acceptReqChan <- *ar
		// resp := <-ar.replyTo
		return resp.encode(ctx, w)
	case 14:
		// accept response
	case 15:
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
		// cr.replyTo = make(chan commitResponse)
		// defer close(cr.replyTo)
		// ds.commitReqChan <- *cr
		// resp := <-cr.replyTo
		return resp.encode(ctx, w)
	case 16:
		// commit response
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
		// dataStore:      storage.NewDataStore(ctx),
		stg:            stg,
		cp:             cp,
		cluster:        clstr,
		prepareReqChan: make(chan prepareRequest),
		acceptReqChan:  make(chan acceptRequest),
		commitReqChan:  make(chan commitRequest),
	}

	go ds.serve(ctx, clstr.CurrentNode().DataStoreAddress)
	// go ds.handlePrepares(ctx)
	// go ds.handleAccepts(ctx)
	// go ds.handleCommits(ctx)

	return ds
}
