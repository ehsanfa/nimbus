package gossip

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	connectionpool "github.com/ehsanfa/nimbus/connection_pool"
)

func TestPickNode(t *testing.T) {
	cp := connectionpool.NewConnectionPool(connectionpool.NewTcpConnector())
	g := NewGossip(Node{
		Id:       12,
		Metadata: []byte{},
	}, cp, "localhost:123", NODE_PICK_NEXT, make(chan Node), make(chan NodeUpdate))
	pickedNode, err := g.pickNode()
	if pickedNode != nil && err == nil {
		t.Error("expected not to be able to pick a node")
	}
	g.cluster.add(&gossipNode{
		id:            2,
		gossipAddress: "localhost:124",
		metadata:      []byte{},
		status:        NODE_STATUS_OK,
		version:       latestVersion(),
	})
	g.cluster.add(&gossipNode{
		id:            3,
		gossipAddress: "localhost:125",
		metadata:      []byte{},
		status:        NODE_STATUS_OK,
		version:       latestVersion(),
	})
	g.cluster.add(&gossipNode{
		id:            4,
		gossipAddress: "localhost:126",
		metadata:      []byte{},
		status:        NODE_STATUS_OK,
		version:       latestVersion(),
	})

	pickedNode, err = g.pickNode()
	if pickedNode == nil {
		t.Error("expected a node to be picked", err)
	}
	if pickedNode != nil && pickedNode.id != 2 {
		t.Error("unexpected id for the picked node", pickedNode.id)
	}
	pickedNode, err = g.pickNode()
	if pickedNode == nil {
		t.Error("expected a node to be picked", err)
	}
	if pickedNode != nil && pickedNode.id != 3 {
		t.Error("unexpected id for the picked node", pickedNode.id)
	}
	pickedNode, err = g.pickNode()
	if pickedNode == nil {
		t.Error("expected a node to be picked", err)
	}
	if pickedNode != nil && pickedNode.id != 4 {
		t.Error("unexpected id for the picked node", pickedNode.id)
	}
	pickedNode, err = g.pickNode()
	if pickedNode == nil {
		t.Error("expected a node to be picked", err)
	}
	if pickedNode != nil && pickedNode.id != 2 {
		t.Error("unexpected id for the picked node", pickedNode.id)
	}
}

func TestSyncWithCluster(t *testing.T) {
	cp := connectionpool.NewConnectionPool(connectionpool.NewTcpConnector())
	newNodeBus := make(chan Node, 2)
	defer close(newNodeBus)
	nodeUpdateBus := make(chan NodeUpdate, 1)
	defer close(nodeUpdateBus)
	g := NewGossip(Node{
		Id:       12,
		Metadata: []byte{},
	}, cp, "localhost:123", NODE_PICK_NEXT, newNodeBus, nodeUpdateBus)
	g.syncWithCluster(&gossipNode{
		id:            2,
		gossipAddress: "node2",
		metadata:      []byte{},
		status:        NODE_STATUS_OK,
		version:       uint64(time.Now().UnixMicro()),
	})
	g.syncWithCluster(&gossipNode{
		id:            5,
		gossipAddress: "node5",
		metadata:      []byte{},
		version:       uint64(time.Now().UnixMicro()),
	})

	if n := <-newNodeBus; n.Id != 2 {
		t.Error("expected to see correct nodeId", n)
	}
	if n := <-newNodeBus; n.Id != 5 {
		t.Error("expected to see correct nodeId", n)
	}

	g.syncWithCluster(&gossipNode{
		id:            5,
		gossipAddress: "node5",
		metadata:      []byte{},
		version:       uint64(time.Now().UnixMicro()),
		status:        NODE_STATUS_UNREACHABLE,
	})
	if n := <-nodeUpdateBus; n.IsReachable != false {
		t.Error("expected to see node not reachable", n)
	}
}

func TestHandleGossip(t *testing.T) {
	cp := connectionpool.NewConnectionPool(connectionpool.NewTcpConnector())
	g := NewGossip(Node{
		Id:       12,
		Metadata: []byte{},
	}, cp, "localhost:123", NODE_PICK_NEXT, make(chan Node, 1), make(chan NodeUpdate, 1))

	var wg sync.WaitGroup

	go func() {
		for range g.nodeInfoBusChan {
			wg.Done()
		}
	}()

	arrivedGossip := map[uint64]uint64{
		2: uint64(time.Now().UnixMicro()),
		3: uint64(time.Now().UnixMicro()),
		4: uint64(time.Now().UnixMicro()),
	}
	wg.Add(3)
	sr := spreadRequest{versions: arrivedGossip, announcerAddress: g.cluster.currentNode.gossipAddress}
	g.handleGossip(sr)
	wg.Wait()
}

func TestHandleFailure(t *testing.T) {
	client, _ := net.Pipe()
	cp := connectionpool.NewConnectionPool(connectionpool.NewMockConnector(client))
	newNodeBus := make(chan Node, 1)
	defer close(newNodeBus)
	nodeUpdateBus := make(chan NodeUpdate, 1)
	defer close(nodeUpdateBus)
	g := NewGossip(Node{
		Id:       1,
		Metadata: []byte{},
	}, cp, "localhost:9060", NODE_PICK_NEXT, newNodeBus, nodeUpdateBus)
	diff, _ := time.ParseDuration("-10s")
	g.cluster.add(&gossipNode{
		id:            2,
		gossipAddress: "localhost:9060",
		metadata:      []byte{},
		version:       uint64(time.Now().Add(diff).UnixMicro()),
	})
	node2 := g.cluster.info[2]
	oldVersion := g.cluster.versions[uint64(node2.id)]
	g.handleFailure(node2)
	if node2.status != NODE_STATUS_UNREACHABLE {
		t.Error("node status expected to be unreachable")
	}
	if g.cluster.versions[uint64(node2.id)] <= oldVersion {
		t.Error("expected to see a bump in version", g.cluster.versions[uint64(node2.id)], oldVersion)
	}
	if n := <-nodeUpdateBus; n.IsReachable != false {
		t.Error("expected to see unreachable update")
	}
}

func TestCallToSpread(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client, server := net.Pipe()
	cp := connectionpool.NewConnectionPool(connectionpool.NewMockConnector(server))
	g := NewGossip(Node{
		Id:       1,
		Metadata: []byte{},
	}, cp, "localhost:9060", NODE_PICK_NEXT, make(chan Node), make(chan NodeUpdate))
	g.Setup(ctx)
	done := make(chan error)
	go g.callToSpread(ctx, g.cluster.currentNode, done)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		<-done
		wg.Done()
	}()

	go func() {
		var l uint32
		if err := binary.Read(client, binary.BigEndian, &l); err != nil {
			panic(err)
		}
		var id byte
		if err := binary.Read(client, binary.BigEndian, &id); err != nil {
			panic(err)
		}
		_, err := decodeSpreadRequest(client)
		if err != nil {
			panic(err)
		}
		resp := &spreadResponse{identifier: IDENTIFIFER_GOSSIP_SPREAD_RESPONSE, ok: true}
		err = resp.encode(context.Background(), client)
		if err != nil {
			panic(err)
		}
		wg.Done()
	}()

	wg.Wait()
	cancel()
}

func TestSpread(t *testing.T) {
	node1Conn, node2Conn := net.Pipe()

	cp1 := connectionpool.NewConnectionPool(connectionpool.NewMockConnector(node2Conn))
	ctx1, cancel1 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel1()
	clstrSize := 8
	partitionSize := 1024
	g1 := NewGossip(Node{
		Id:       1,
		Metadata: []byte{},
	}, cp1, "localhost:9060", NODE_PICK_NEXT, make(chan Node), make(chan NodeUpdate))
	g1.Setup(ctx1)
	for i := range clstrSize {
		var metadata string
		for range partitionSize {
			metadata = metadata + time.Now().GoString()
		}
		g1.cluster.add(&gossipNode{
			id:            uint64(i + 1),
			gossipAddress: "localhost:9060",
			metadata:      []byte(metadata),
			version:       uint64(time.Now().UnixMicro()),
		})
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	cp2 := connectionpool.NewConnectionPool(connectionpool.NewMockConnector(node1Conn))
	g2 := NewGossip(Node{
		Id:       2,
		Metadata: []byte{},
	}, cp2, "localhost:9061", NODE_PICK_NEXT, make(chan Node), make(chan NodeUpdate))
	g2.Setup(ctx2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		// handling spread requests
		var l uint32
		if err := binary.Read(node2Conn, binary.BigEndian, &l); err != nil {
			panic(err)
		}
		var id byte
		if err := binary.Read(node2Conn, binary.BigEndian, &id); err != nil {
			panic(err)
		}
		g2.handleIncoming(ctx2, node2Conn, node1Conn, id)
		wg.Done()
	}()

	go func() {
		// handling node info requests
		var l uint32
		if err := binary.Read(node1Conn, binary.BigEndian, &l); err != nil {
			panic(err)
		}
		var id byte
		if err := binary.Read(node1Conn, binary.BigEndian, &id); err != nil {
			panic(err)
		}
		g1.handleIncoming(ctx1, node1Conn, node2Conn, id)
		wg.Done()
	}()

	g1.spread(ctx1, g2.cluster.currentNode)
	wg.Wait()
	fmt.Println(g1.cluster.versions)
	fmt.Println(g2.cluster.versions)
	if len(g2.cluster.versions) != clstrSize+2 {
		t.Error("expected to see 2 nodes", g2.cluster.versions)
	}
}
