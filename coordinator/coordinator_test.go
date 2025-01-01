package coordinator

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"net"
	"testing"

	cluster "github.com/ehsanfa/nimbus/cluster"
	connectionpool "github.com/ehsanfa/nimbus/connection_pool"
	datastore "github.com/ehsanfa/nimbus/data_store"
	"github.com/ehsanfa/nimbus/partition"
	"github.com/ehsanfa/nimbus/storage"
)

func TestSetRequestEncode(t *testing.T) {
	ctx := context.Background()
	var b bytes.Buffer
	sr := setRequest{identifier: IDENTIFIER_COORDINATOR_SET_REQUEST, key: []byte("key"), value: []byte("value")}
	err := sr.encode(ctx, &b)
	if err != nil {
		t.Error(err)
	}

	var size uint32
	binary.Read(&b, binary.BigEndian, &size)

	var identifier byte
	err = binary.Read(&b, binary.BigEndian, &identifier)
	if err != nil {
		t.Error(err)
	}
	if identifier != IDENTIFIER_COORDINATOR_SET_REQUEST {
		t.Error("expected to get the correct identifier")
	}

	decodedSr, err := decodeSetRequest(&b)
	if err != nil {
		t.Error(err)
	}
	if decodedSr == nil {
		t.Error("unexpected nil value for decoded set request")
		return
	}
	if string(decodedSr.key) != "key" {
		t.Error("wrong key")
	}
	if string(decodedSr.value) != "value" {
		t.Error("wrong value")
	}
}

func TestSetRequestDecodeWithInvalidData(t *testing.T) {
	var b bytes.Buffer
	b.WriteByte(IDENTIFIER_COORDINATOR_SET_REQUEST)

	var identifier byte
	err := binary.Read(&b, binary.BigEndian, &identifier)
	if err != nil {
		t.Error(err)
	}
	if identifier != IDENTIFIER_COORDINATOR_SET_REQUEST {
		t.Error("expected to get the correct identifier")
	}

	_, err = decodeSetRequest(&b)
	if err != io.EOF {
		t.Error(err)
		return
	}
}

func TestCoordinatorSet(t *testing.T) {
	ctx := context.Background()

	l, err := net.Listen("tcp", "127.0.0.1:")
	if err != nil {
		panic(err)
	}
	defer l.Close()
	self := cluster.NewNode(l.Addr().String(), []partition.Token{12}, cluster.NODE_STATUS_OK)

	// for i := range 3 {
	// 	l, err := net.Listen("tcp", "127.0.0.1:")
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	defer l.Close()
	// 	n := NewNode(l.Addr().String(), []partition.Token{partition.Token(12 + i)}, NODE_STATUS_OK)
	// 	nodes = append(nodes, &n)
	// }

	conn, _ := net.Pipe()
	cp := connectionpool.NewConnectionPool(connectionpool.NewMockConnector(conn))
	stg := storage.NewDataStore(ctx)
	cluster := cluster.NewCluster(self, 1, cluster.CONSISTENCY_LEVEL_ALL)
	ds := datastore.NewDataStore(ctx, stg, cluster, cp)
	c := NewCoordinator(ctx, cluster, ds, stg, "localhost:9055")

	coord, err := cp.GetClient(self.DataStoreAddress, "test")
	if err != nil {
		panic(err)
	}

	go func() {
		sr := setRequest{identifier: IDENTIFIER_COORDINATOR_SET_REQUEST, key: []byte("key"), value: []byte("value")}
		err = sr.encode(ctx, coord)
		if err != nil {
			panic(err)
		}
		fmt.Println("wrote")
	}()

	go func() {
		b := make([]byte, 1024)
		coord.Read(b)
		fmt.Println(b)
		// var size uint32
		// binary.Read(coord, binary.BigEndian, &size)
		var identifier byte
		binary.Read(coord, binary.BigEndian, &identifier)
		err := c.handleIncoming(ctx, coord, coord, identifier)
		if err != nil {
			panic(err)
		}
	}()

	res := make(chan *setResponse)
	errs := make(chan error)
	go func() {
		defer close(res)
		defer close(errs)
		var size uint32
		binary.Read(coord, binary.BigEndian, &size)
		var identifier byte
		binary.Read(coord, binary.BigEndian, &identifier)
		sr, err := decodeSetResponse(coord)
		if err != nil {
			res <- nil
			errs <- err
			return
		}
		errs <- nil
		res <- sr
	}()
	if err := <-errs; err != nil {
		t.Error(err)
	}
	resp := <-res
	if resp.ok {
		t.Error("expected to get ok result", resp.err)
	}

}

func BenchmarkCoordinatorPrepare(b *testing.B) {
	addr2 := fmt.Sprintf("127.0.0.1:%d", rand.Intn(9850-9001)+9001)
	addr4 := fmt.Sprintf("127.0.0.1:%d", rand.Intn(9850-9001)+9001)
	ctx := context.Background()
	cp := connectionpool.NewConnectionPool(connectionpool.NewTcpConnector())
	stg := storage.NewDataStore(ctx)
	node1 := cluster.NewNode(addr2, []partition.Token{12}, cluster.NODE_STATUS_OK)
	clstr1 := cluster.NewCluster(node1, 1, cluster.CONSISTENCY_LEVEL_ALL)
	ds := datastore.NewDataStore(ctx, stg, clstr1, cp)
	c := NewCoordinator(ctx, clstr1, ds, stg, "127.0.0.1:")

	node2 := cluster.NewNode(addr4, []partition.Token{12}, cluster.NODE_STATUS_OK)
	clstr2 := cluster.NewCluster(node2, 1, cluster.CONSISTENCY_LEVEL_ALL)
	datastore.NewDataStore(ctx, stg, clstr2, cp)

	for n := 0; n < b.N; n++ {
		promised := c.prepare(ctx, []*cluster.Node{node2}, []byte("hasan"), 2123)
		if !promised {
			b.Error("expected to promise")
		}
	}
}

func getPartitions(num int) []partition.Token {
	ps := make([]partition.Token, num)
	for i := range num {
		ps[i] = partition.SuggestToken()
	}
	return ps
}

func BenchmarkCoordinatorSet(b *testing.B) {
	addr2 := fmt.Sprintf("127.0.0.1:%d", rand.Intn(9850-7001)+7001)
	addr4 := fmt.Sprintf("127.0.0.1:%d", rand.Intn(9850-7001)+7001)
	addr6 := fmt.Sprintf("127.0.0.1:%d", rand.Intn(9850-7001)+7001)
	addr8 := fmt.Sprintf("127.0.0.1:%d", rand.Intn(9850-7001)+7001)
	ctx := context.Background()
	cp := connectionpool.NewConnectionPool(connectionpool.NewTcpConnector())
	stg := storage.NewDataStore(ctx)
	node1 := cluster.NewNode(addr2, getPartitions(256), cluster.NODE_STATUS_OK)
	clstr1 := cluster.NewCluster(node1, 3, cluster.CONSISTENCY_LEVEL_ALL)
	ds := datastore.NewDataStore(ctx, stg, clstr1, cp)
	c := NewCoordinator(ctx, clstr1, ds, stg, "127.0.0.1:")

	node2 := cluster.NewNode(addr4, getPartitions(256), cluster.NODE_STATUS_OK)
	clstr2 := cluster.NewCluster(node2, 3, cluster.CONSISTENCY_LEVEL_ALL)
	datastore.NewDataStore(ctx, stg, clstr2, cp)

	node3 := cluster.NewNode(addr6, getPartitions(256), cluster.NODE_STATUS_OK)
	clstr3 := cluster.NewCluster(node3, 3, cluster.CONSISTENCY_LEVEL_ALL)
	datastore.NewDataStore(ctx, stg, clstr3, cp)

	node4 := cluster.NewNode(addr8, getPartitions(256), cluster.NODE_STATUS_OK)
	clstr4 := cluster.NewCluster(node4, 3, cluster.CONSISTENCY_LEVEL_ALL)
	datastore.NewDataStore(ctx, stg, clstr4, cp)

	clstr1.AddNodes(node2, node3, node4)
	clstr2.AddNodes(node1, node3, node4)
	clstr3.AddNodes(node1, node2, node4)
	clstr4.AddNodes(node1, node2, node3)

	failed := 0
	for n := 0; n < b.N; n++ {
		set, err := c.set(ctx, []byte("hasan"), []byte("hooshang"))
		if !set || err != nil {
			fmt.Println(err)
			failed += 1
		}
	}
	b.Log("failed", failed)
}
