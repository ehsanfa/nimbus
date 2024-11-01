package datastore

import (
	"context"
	"encoding/binary"
	"net"
	"testing"

	"github.com/ehsanfa/nimbus/cluster"
	connectionpool "github.com/ehsanfa/nimbus/connection_pool"
	"github.com/ehsanfa/nimbus/partition"
	"github.com/ehsanfa/nimbus/storage"
)

func TestCore(t *testing.T) {
	ctx := context.Background()
	stg := storage.NewDataStore(ctx)
	node1 := cluster.NewNode("localhost:", "localhost:9060", []partition.Token{1}, cluster.NODE_STATUS_OK)
	clstr1 := cluster.NewCluster(node1, 2, cluster.CONSISTENCY_LEVEL_ALL)
	cp := connectionpool.NewConnectionPool(connectionpool.NewTcpConnector())
	ds := NewDataStore(ctx, stg, clstr1, cp)
	proposal := uint64(343234234)
	prepared, err := ds.prepare(ctx, []byte("key"), proposal)
	if !prepared {
		t.Error("expected to see prepared")
	}
	if err != nil {
		t.Error(err)
	}
	accepted, err := ds.accept(ctx, []byte("key"), proposal, []byte("value"))
	if !accepted {
		t.Error("expected to see accepted")
	}
	if err != nil {
		t.Error(err)
	}
	committed, err := ds.commit(ctx, []byte("key"), proposal)
	if !committed {
		t.Error("expected to see committed")
	}
	if err != nil {
		t.Error(err)
	}
}

func TestSuccessfulPrepareRequest(t *testing.T) {
	ctx := context.Background()
	stg := storage.NewDataStore(ctx)
	node1 := cluster.NewNode("localhost:", "localhost:9060", []partition.Token{1}, cluster.NODE_STATUS_OK)
	clstr1 := cluster.NewCluster(node1, 2, cluster.CONSISTENCY_LEVEL_ALL)
	cp := connectionpool.NewConnectionPool(connectionpool.NewTcpConnector())
	ds := NewDataStore(ctx, stg, clstr1, cp)

	client, server := net.Pipe()

	go func() {
		pr := prepareRequest{identifier: IDENTIFIER_DATA_STORE_PREPARE_REQUEST, key: []byte("key"), proposal: 34235}
		err := pr.encode(ctx, server)
		if err != nil {
			panic(err)
		}
	}()

	go func() {
		var size uint32
		binary.Read(client, binary.BigEndian, &size)
		var identifier byte
		binary.Read(client, binary.BigEndian, &identifier)
		err := ds.handleIncoming(ctx, client, client, identifier)
		if err != nil {
			panic(err)
		}
	}()

	respond := make(chan *prepareResponse)
	errs := make(chan error)
	go func() {
		defer close(errs)
		defer close(respond)
		var size uint32
		binary.Read(server, binary.BigEndian, &size)
		var identifier byte
		binary.Read(server, binary.BigEndian, &identifier)
		res, err := decodePrepareResponse(server)
		if err != nil {
			errs <- err
			return
		}
		errs <- nil
		respond <- res
	}()
	if err := <-errs; err != nil {
		t.Error(err)
	}
	if res := <-respond; res != nil && res.promised != true {
		t.Error("expected to receive promise")
	}
}

func TestOutdatedPrepareRequest(t *testing.T) {
	ctx := context.Background()
	stg := storage.NewDataStore(ctx)
	node1 := cluster.NewNode("localhost:", "localhost:9060", []partition.Token{1}, cluster.NODE_STATUS_OK)
	clstr1 := cluster.NewCluster(node1, 2, cluster.CONSISTENCY_LEVEL_ALL)
	cp := connectionpool.NewConnectionPool(connectionpool.NewTcpConnector())
	ds := NewDataStore(ctx, stg, clstr1, cp)
	key := []byte("key")
	proposal := uint64(34235)
	ds.prepare(ctx, key, proposal)

	client, server := net.Pipe()

	go func() {
		pr := prepareRequest{identifier: IDENTIFIER_DATA_STORE_PREPARE_REQUEST, key: key, proposal: proposal - 1}
		err := pr.encode(ctx, server)
		if err != nil {
			panic(err)
		}
	}()

	go func() {
		var size uint32
		binary.Read(client, binary.BigEndian, &size)
		var identifier byte
		binary.Read(client, binary.BigEndian, &identifier)
		err := ds.handleIncoming(ctx, client, client, identifier)
		if err != nil {
			panic(err)
		}
	}()

	respond := make(chan *prepareResponse)
	errs := make(chan error)
	go func() {
		defer close(errs)
		defer close(respond)
		var size uint32
		binary.Read(server, binary.BigEndian, &size)
		var identifier byte
		binary.Read(server, binary.BigEndian, &identifier)
		res, err := decodePrepareResponse(server)
		if err != nil {
			errs <- err
			return
		}
		errs <- nil
		respond <- res
	}()
	if err := <-errs; err != nil {
		t.Error("expected not to see errors related to decoding the response")
	}
	res := <-respond
	if res != nil && res.promised {
		t.Error("expected not to receive a promise")
	}
	if res != nil && res.err == nil {
		t.Error("expected to see errors")
	}
}

func TestSuccessfulAcceptRequest(t *testing.T) {
	ctx := context.Background()
	stg := storage.NewDataStore(ctx)
	node1 := cluster.NewNode("localhost:", "localhost:9060", []partition.Token{1}, cluster.NODE_STATUS_OK)
	clstr1 := cluster.NewCluster(node1, 2, cluster.CONSISTENCY_LEVEL_ALL)
	cp := connectionpool.NewConnectionPool(connectionpool.NewTcpConnector())
	ds := NewDataStore(ctx, stg, clstr1, cp)
	key := []byte("key")
	proposal := uint64(34235)
	ds.prepare(ctx, key, proposal)

	client, server := net.Pipe()

	go func() {
		ar := acceptRequest{identifier: IDENTIFIER_DATA_STORE_ACCEPT_REQUEST, key: key, proposal: proposal, value: []byte("value")}
		err := ar.encode(ctx, server)
		if err != nil {
			panic(err)
		}
	}()

	go func() {
		var size uint32
		binary.Read(client, binary.BigEndian, &size)
		var identifier byte
		binary.Read(client, binary.BigEndian, &identifier)
		err := ds.handleIncoming(ctx, client, client, identifier)
		if err != nil {
			panic(err)
		}
	}()

	respond := make(chan *acceptResponse)
	errs := make(chan error)
	go func() {
		defer close(errs)
		defer close(respond)
		var size uint32
		binary.Read(server, binary.BigEndian, &size)
		var identifier byte
		binary.Read(server, binary.BigEndian, &identifier)
		res, err := decodeAcceptResponse(server)
		if err != nil {
			errs <- err
			return
		}
		errs <- nil
		respond <- res
	}()
	if err := <-errs; err != nil {
		t.Error(err)
	}
	if res := <-respond; res != nil && res.accepted != true {
		t.Error("expected to receive accepted")
	}
}

func TestAcceptRequestForNonExistingPromise(t *testing.T) {
	ctx := context.Background()
	stg := storage.NewDataStore(ctx)
	node1 := cluster.NewNode("localhost:", "localhost:9060", []partition.Token{1}, cluster.NODE_STATUS_OK)
	clstr1 := cluster.NewCluster(node1, 2, cluster.CONSISTENCY_LEVEL_ALL)
	cp := connectionpool.NewConnectionPool(connectionpool.NewTcpConnector())
	ds := NewDataStore(ctx, stg, clstr1, cp)
	key := []byte("key")
	proposal := uint64(34235)

	client, server := net.Pipe()

	go func() {
		ar := acceptRequest{identifier: IDENTIFIER_DATA_STORE_ACCEPT_REQUEST, key: key, proposal: proposal, value: []byte("value")}
		err := ar.encode(ctx, server)
		if err != nil {
			panic(err)
		}
	}()

	go func() {
		var size uint32
		binary.Read(client, binary.BigEndian, &size)
		var identifier byte
		binary.Read(client, binary.BigEndian, &identifier)
		err := ds.handleIncoming(ctx, client, client, identifier)
		if err != nil {
			panic(err)
		}
	}()

	respond := make(chan *acceptResponse)
	errs := make(chan error)
	go func() {
		defer close(errs)
		defer close(respond)
		var size uint32
		binary.Read(server, binary.BigEndian, &size)
		var identifier byte
		binary.Read(server, binary.BigEndian, &identifier)
		res, err := decodeAcceptResponse(server)
		if err != nil {
			errs <- err
			return
		}
		errs <- nil
		respond <- res
	}()
	if err := <-errs; err != nil {
		t.Error(err)
	}
	res := <-respond
	if res != nil && res.accepted == true {
		t.Error("expected not to receive accepted")
	}
	if res != nil && string(res.err) == "" {
		t.Error("expected to receive errors")
	}
}

func TestSuccessfulCommitRequest(t *testing.T) {
	ctx := context.Background()
	stg := storage.NewDataStore(ctx)
	node1 := cluster.NewNode("localhost:", "localhost:9060", []partition.Token{1}, cluster.NODE_STATUS_OK)
	clstr1 := cluster.NewCluster(node1, 2, cluster.CONSISTENCY_LEVEL_ALL)
	cp := connectionpool.NewConnectionPool(connectionpool.NewTcpConnector())
	ds := NewDataStore(ctx, stg, clstr1, cp)
	key := []byte("key")
	proposal := uint64(34235)
	value := []byte("value")
	ds.prepare(ctx, key, proposal)
	ds.accept(ctx, key, proposal, value)

	client, server := net.Pipe()

	go func() {
		cr := commitRequest{identifier: IDENTIFIER_DATA_STORE_COMMIT_REQUEST, key: key, proposal: proposal}
		err := cr.encode(ctx, server)
		if err != nil {
			panic(err)
		}
	}()

	go func() {
		var size uint32
		binary.Read(client, binary.BigEndian, &size)
		var identifier byte
		binary.Read(client, binary.BigEndian, &identifier)
		err := ds.handleIncoming(ctx, client, client, identifier)
		if err != nil {
			panic(err)
		}
	}()

	respond := make(chan *commitResponse)
	errs := make(chan error)
	go func() {
		defer close(errs)
		defer close(respond)
		var size uint32
		binary.Read(server, binary.BigEndian, &size)
		var identifier byte
		binary.Read(server, binary.BigEndian, &identifier)
		res, err := decodeCommitResponse(server)
		if err != nil {
			errs <- err
			return
		}
		errs <- nil
		respond <- res
	}()
	if err := <-errs; err != nil {
		t.Error(err)
	}
	if res := <-respond; res != nil && res.committed != true {
		t.Error("expected to receive committed")
	}
}

func TestCommitRequestForNonExistingPromise(t *testing.T) {
	ctx := context.Background()
	stg := storage.NewDataStore(ctx)
	node1 := cluster.NewNode("localhost:", "localhost:9060", []partition.Token{1}, cluster.NODE_STATUS_OK)
	clstr1 := cluster.NewCluster(node1, 2, cluster.CONSISTENCY_LEVEL_ALL)
	cp := connectionpool.NewConnectionPool(connectionpool.NewTcpConnector())
	ds := NewDataStore(ctx, stg, clstr1, cp)
	key := []byte("key")
	proposal := uint64(34235)

	client, server := net.Pipe()

	go func() {
		cr := commitRequest{identifier: IDENTIFIER_DATA_STORE_COMMIT_REQUEST, key: key, proposal: proposal}
		err := cr.encode(ctx, server)
		if err != nil {
			panic(err)
		}
	}()

	go func() {
		var size uint32
		binary.Read(client, binary.BigEndian, &size)
		var identifier byte
		binary.Read(client, binary.BigEndian, &identifier)
		err := ds.handleIncoming(ctx, client, client, identifier)
		if err != nil {
			panic(err)
		}
	}()

	respond := make(chan *commitResponse)
	errs := make(chan error)
	go func() {
		defer close(errs)
		defer close(respond)
		var size uint32
		binary.Read(server, binary.BigEndian, &size)
		var identifier byte
		binary.Read(server, binary.BigEndian, &identifier)
		res, err := decodeCommitResponse(server)
		if err != nil {
			errs <- err
			return
		}
		errs <- nil
		respond <- res
	}()
	if err := <-errs; err != nil {
		t.Error(err)
	}
	res := <-respond
	if res != nil && res.committed == true {
		t.Error("expected not to receive accepted")
	}
	if res != nil && string(res.err) == "" {
		t.Error("expected to receive errors")
	}
}
