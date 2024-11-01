package gossip

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/ehsanfa/nimbus/cluster"
	connectionpool "github.com/ehsanfa/nimbus/connection_pool"
	"github.com/ehsanfa/nimbus/partition"
)

func TestPickNode(t *testing.T) {
	ctx := context.Background()
	node1 := cluster.NewNode("node1", "localhost:9001", []partition.Token{1}, cluster.NODE_STATUS_OK)
	node2 := cluster.NewNode("node2", "localhost:9001", []partition.Token{2}, cluster.NODE_STATUS_OK)
	node3 := cluster.NewNode("node3", "localhost:9001", []partition.Token{3}, cluster.NODE_STATUS_OK)
	node4 := cluster.NewNode("node4", "localhost:9001", []partition.Token{4}, cluster.NODE_STATUS_OK)
	clstr := cluster.NewCluster(node1, 1, cluster.CONSISTENCY_LEVEL_ONE)
	cp := connectionpool.NewConnectionPool(connectionpool.NewTcpConnector())
	g := NewGossip(ctx, clstr, cp, NODE_PICK_NEXT)
	pickedNode, err := g.pickNode(3)
	if pickedNode != nil {
		t.Error("expected not to be able to pick a node")
	}
	if err != nil {
		t.Error("expected not to see any error")
	}

	clstr.AddNodes(node2, node3, node4)

	pickedNode, err = g.pickNode(10)
	if pickedNode == nil {
		t.Error("expected a node to be picked", err)
	}
	if pickedNode != nil && pickedNode.Id != 2 {
		t.Error("unexpected id for the picked node", pickedNode.Id)
	}
	pickedNode, err = g.pickNode(10)
	if pickedNode == nil {
		t.Error("expected a node to be picked", err)
	}
	if pickedNode != nil && pickedNode.Id != 3 {
		t.Error("unexpected id for the picked node", pickedNode.Id)
	}
	pickedNode, err = g.pickNode(10)
	if pickedNode == nil {
		t.Error("expected a node to be picked", err)
	}
	if pickedNode != nil && pickedNode.Id != 4 {
		t.Error("unexpected id for the picked node", pickedNode.Id)
	}
	pickedNode, err = g.pickNode(10)
	if pickedNode == nil {
		t.Error("expected a node to be picked", err)
	}
	if pickedNode != nil && pickedNode.Id != 2 {
		t.Error("unexpected id for the picked node", pickedNode.Id)
	}
}

func TestSyncWithCluster(t *testing.T) {
	ctx := context.Background()
	node1 := cluster.NewNode("node1", "localhost:9060", []partition.Token{1}, cluster.NODE_STATUS_OK)
	node2 := cluster.NewNode("node2", "localhost:9060", []partition.Token{2}, cluster.NODE_STATUS_OK)
	node3 := cluster.NewNode("node3", "localhost:9060", []partition.Token{3}, cluster.NODE_STATUS_OK)
	node4 := cluster.NewNode("node4", "localhost:9060", []partition.Token{4}, cluster.NODE_STATUS_OK)
	clstr := cluster.NewCluster(node1, 4, cluster.CONSISTENCY_LEVEL_ALL)
	clstr.AddNodes(node2, node3, node4)
	cp := connectionpool.NewConnectionPool(connectionpool.NewTcpConnector())
	g := NewGossip(ctx, clstr, cp, NODE_PICK_NEXT)
	if !g.cluster.IsHealthy() {
		t.Error("expected to have a healthy cluster")
	}
	err := g.syncWithCluster(&gossipNode{
		Id:               2,
		GossipAddress:    "node2",
		DataStoreAddress: "localhost:9060",
		Tokens:           []partition.Token{2},
		Status:           uint8(cluster.NODE_STATUS_UNREACHABLE),
		Version:          uint64(time.Now().UnixMicro()),
	})
	if err != nil {
		t.Error(err)
	}
	if g.cluster.IsHealthy() {
		t.Error("expected to see the cluster unhealthy")
	}
	err = g.syncWithCluster(&gossipNode{
		Id:               5,
		GossipAddress:    "node5",
		DataStoreAddress: "localhost:9060",
		Tokens:           []partition.Token{5},
		Status:           uint8(cluster.NODE_STATUS_OK),
		Version:          uint64(time.Now().UnixMicro()),
	})
	if err != nil {
		t.Error(err)
	}
	if !g.cluster.IsHealthy() {
		t.Error("expected to see the cluster healthy")
	}
}

func TestHandleGossip(t *testing.T) {
	ctx := context.Background()
	node1 := cluster.NewNode("node1", "localhost:9060", []partition.Token{1}, cluster.NODE_STATUS_OK)
	clstr := cluster.NewCluster(node1, 4, cluster.CONSISTENCY_LEVEL_ALL)
	clstr.AddNode(cluster.NewNode("node2", "localhost:9060", []partition.Token{2}, cluster.NODE_STATUS_OK))
	clstr.AddNode(cluster.NewNode("node3", "localhost:9060", []partition.Token{3}, cluster.NODE_STATUS_OK))
	clstr.AddNode(cluster.NewNode("node4", "localhost:9060", []partition.Token{4}, cluster.NODE_STATUS_OK))
	clstr.AddNode(cluster.NewNode("node5", "localhost:9060", []partition.Token{5}, cluster.NODE_STATUS_OK))
	cp := connectionpool.NewConnectionPool(connectionpool.NewTcpConnector())
	g := NewGossip(ctx, clstr, cp, NODE_PICK_NEXT)

	arrivedGossip := map[uint64]uint64{
		2: uint64(time.Now().UnixMicro()),
		3: uint64(time.Now().UnixMicro()),
		4: uint64(time.Now().UnixMicro()),
	}
	sr := spreadRequest{versions: arrivedGossip, announcerAddress: node1.GossipAddress}
	g.handleGossip(sr)
	if !g.cluster.IsHealthy() {
		t.Error("expected to have an healthy cluster")
	}
}

func TestConvertToNodes(t *testing.T) {
	ctx := context.Background()
	node1 := cluster.NewNode("node1", "localhost:9060", []partition.Token{1}, cluster.NODE_STATUS_OK)
	clstr := cluster.NewCluster(node1, 4, cluster.CONSISTENCY_LEVEL_ALL)
	cp := connectionpool.NewConnectionPool(connectionpool.NewTcpConnector())
	g := NewGossip(ctx, clstr, cp, NODE_PICK_NEXT)
	nodes := g.convertToNodes()
	if len(nodes) != 1 {
		t.Error("expected to get 1 nodes")
	}
	if nodes[0].Id != 1 {
		t.Error("ids don't match")
	}
}

func TestHandleFailure(t *testing.T) {
	ctx := context.Background()
	node1 := cluster.NewNode("node1", "localhost:9060", []partition.Token{1}, cluster.NODE_STATUS_OK)
	clstr := cluster.NewCluster(node1, 2, cluster.CONSISTENCY_LEVEL_ALL)
	client, _ := net.Pipe()
	cp := connectionpool.NewConnectionPool(connectionpool.NewMockConnector(client))
	g := NewGossip(ctx, clstr, cp, NODE_PICK_NEXT)
	node2 := cluster.NewNode("node2", "localhost:9060", []partition.Token{2}, cluster.NODE_STATUS_OK)
	clstr.AddNode(node2)
	if !clstr.IsHealthy() {
		t.Error("cluster expected to be healthy")
	}
	node2.Status = cluster.NODE_STATUS_UNREACHABLE
	oldVersion := g.versions[uint64(node2.Id)]
	g.handleFailure(node2)
	if clstr.IsHealthy() {
		t.Error("clusted expected not to be healthy")
	}
	if clstr.NodeFromId(2).Status != cluster.NODE_STATUS_UNREACHABLE {
		t.Error("node status expected to be unreachable")
	}
	if g.versions[uint64(node2.Id)] <= oldVersion {
		t.Error("expected to see a bump in version")
	}
}

func TestCallToSpread(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	node1 := cluster.NewNode("node1", "localhost:9060", []partition.Token{1}, cluster.NODE_STATUS_OK)
	clstr := cluster.NewCluster(node1, 2, cluster.CONSISTENCY_LEVEL_ALL)
	client, server := net.Pipe()
	cp := connectionpool.NewConnectionPool(connectionpool.NewMockConnector(server))
	g := NewGossip(ctx, clstr, cp, NODE_PICK_NEXT)
	done := make(chan error)
	go g.callToSpread(ctx, node1, done)

	go func() {
		var l uint32
		if err := binary.Read(client, binary.BigEndian, &l); err != nil {
			panic(err)
		}
		var id byte
		if err := binary.Read(client, binary.BigEndian, &id); err != nil {
			panic(err)
		}
		sr, err := decodeSpreadRequest(client)
		if err != nil {
			panic(err)
		}
		resp := &spreadResponse{identifier: IDENTIFIFER_GOSSIP_SPREAD_RESPONSE, ok: true}
		err = resp.encode(context.Background(), client)
		if err != nil {
			panic(err)
		}
		fmt.Println("sr", sr)
	}()

	<-done
	cancel()
}

func TestSpread(t *testing.T) {
	node1Conn, node2Conn := net.Pipe()

	ctx1, cancel1 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel1()
	node1 := cluster.NewNode("localhost:", "localhost:9060", []partition.Token{1}, cluster.NODE_STATUS_OK)
	clstr1 := cluster.NewCluster(node1, 2, cluster.CONSISTENCY_LEVEL_ALL)
	clstrSize := 8
	partitionSize := 1024
	for range clstrSize {
		partitions := []partition.Token{}
		for range partitionSize {
			partitions = append(partitions, partition.SuggestToken())
		}
		clstr1.AddNode(cluster.NewNode("localhost:", "localhost:9060", partitions, cluster.NODE_STATUS_OK))
	}
	cp1 := connectionpool.NewConnectionPool(connectionpool.NewMockConnector(node2Conn))
	g1 := NewGossip(ctx1, clstr1, cp1, NODE_PICK_NEXT)

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	node2 := cluster.NewNode("localhost:", "localhost:9060", []partition.Token{2}, cluster.NODE_STATUS_OK)
	clstr2 := cluster.NewCluster(node2, 2, cluster.CONSISTENCY_LEVEL_ALL)
	cp2 := connectionpool.NewConnectionPool(connectionpool.NewMockConnector(node1Conn))
	g2 := NewGossip(ctx2, clstr2, cp2, NODE_PICK_NEXT)
	go func() {
		var l uint32
		if err := binary.Read(node1Conn, binary.BigEndian, &l); err != nil {
			panic(err)
		}
		var id byte
		if err := binary.Read(node1Conn, binary.BigEndian, &id); err != nil {
			panic(err)
		}
		g2.handleIncoming(ctx2, node1Conn, node1Conn, id)
	}()

	g1.spread(ctx1, node2)
	if len(g2.versions) != clstrSize+2 {
		t.Error("expected to see 2 nodes", g2.versions)
	}
}
