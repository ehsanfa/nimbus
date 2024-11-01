package gossip

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/ehsanfa/nimbus/partition"
)

func TestGossipNodeEncodeAndDecode(t *testing.T) {
	var b bytes.Buffer
	gn := gossipNode{
		Id:               100,
		GossipAddress:    "node1:9092",
		DataStoreAddress: "localhost:9060",
		Tokens:           []partition.Token{254354, 3265676, 975335},
		Status:           2,
		Version:          233453234,
	}
	err := gn.encode(&b)
	if err != nil {
		t.Error(err)
	}

	decodedGn, err := decodeGossipNode(&b)
	if err != nil {
		t.Error(err)
	}
	fmt.Println(decodedGn)
	if decodedGn.Id != gn.Id {
		t.Error("Id mismatch")
	}
	if decodedGn.GossipAddress != gn.GossipAddress {
		t.Error("GossipAddress mismatch")
	}
	if decodedGn.DataStoreAddress != gn.DataStoreAddress {
		t.Error("DataStoreAddress mismatch")
	}
	if len(decodedGn.Tokens) != len(gn.Tokens) {
		t.Error("tokens mismatch")
	}
	if decodedGn.Status != gn.Status {
		t.Error("status mismatch")
	}
	if decodedGn.Version != gn.Version {
		t.Error("version mismatch")
	}
}

func TestSpreadRequestEncodeAndDecode(t *testing.T) {
	versions := map[uint64]uint64{
		100: 233453234,
		101: 233453234,
	}
	sr := spreadRequest{
		identifier:       10,
		announcerAddress: "node1:8080",
		versions:         versions,
	}
	var b bytes.Buffer
	err := sr.encode(context.Background(), &b)
	if err != nil {
		t.Error(err)
	}

	var size uint32
	binary.Read(&b, binary.BigEndian, &size)

	var identifier byte
	binary.Read(&b, binary.BigEndian, &identifier)

	decodedSr, err := decodeSpreadRequest(&b)
	if err != nil {
		t.Error(err)
	}
	if len(decodedSr.versions) != 2 {
		t.Error("length mismatch")
	}
	if decodedSr.announcerAddress != "node1:8080" {
		t.Error("announcer addresses mismatch")
	}
}

func TestCatchupResponseEncodeAndDecode(t *testing.T) {
	nodes := []*gossipNode{
		{
			Id:               100,
			GossipAddress:    "node1:9092",
			DataStoreAddress: "localhost:9060",
			Tokens:           []partition.Token{254354, 3265676, 975335},
			Status:           2,
			Version:          233453234,
		},
		{
			Id:               101,
			GossipAddress:    "node1:9092",
			DataStoreAddress: "localhost:9060",
			Tokens:           []partition.Token{254354, 3265676, 975335},
			Status:           2,
			Version:          233453234,
		},
		{
			Id:               101,
			GossipAddress:    "node1:9092",
			DataStoreAddress: "localhost:9060",
			Tokens:           []partition.Token{254354, 3265676, 975335},
			Status:           2,
			Version:          233453234,
		},
		{
			Id:               101,
			GossipAddress:    "node1:9092",
			DataStoreAddress: "localhost:9060",
			Tokens:           []partition.Token{254354, 3265676, 975335},
			Status:           2,
			Version:          233453234,
		},
	}
	sr := catchupResponse{
		identifier: 33,
		nodes:      nodes,
	}
	var b bytes.Buffer
	err := sr.encode(context.Background(), &b)
	if err != nil {
		t.Error(err)
	}

	var size uint32
	binary.Read(&b, binary.BigEndian, &size)

	var identifier byte
	binary.Read(&b, binary.BigEndian, &identifier)

	decodedNodes, err := decodeCatchupResponse(&b)
	if err != nil {
		t.Error(err)
	}
	if len(decodedNodes) != 4 {
		t.Error("length mismatch", len(decodedNodes))
	}
}

func TestNodeInfoRequestEncodeDecode(t *testing.T) {
	var b bytes.Buffer
	ctx := context.Background()
	nodeId := uint64(time.Now().UnixMicro())
	ir := nodeInfoRequest{identifier: 1, nodeId: nodeId}
	ir.encode(ctx, &b)

	var size uint32
	binary.Read(&b, binary.BigEndian, &size)
	var identifier byte
	binary.Read(&b, binary.BigEndian, &identifier)
	decodedIr, err := decodeNodeInfoRequest(&b)
	if err != nil {
		t.Error(err)
	}
	if decodedIr.nodeId != nodeId {
		t.Error("expected to see the same nodeId", decodedIr, nodeId)
	}
}

func TestNodeInfoResponseEncodeDecode(t *testing.T) {
	var b bytes.Buffer
	ctx := context.Background()
	gossipNode := gossipNode{
		Id:               100,
		GossipAddress:    "node1:9092",
		DataStoreAddress: "localhost:9060",
		Tokens:           []partition.Token{254354, 3265676, 975335},
		Status:           2,
		Version:          233453234,
	}
	ir := nodeInfoResponse{identifier: 1, node: &gossipNode}
	ir.encode(ctx, &b)

	var size uint32
	binary.Read(&b, binary.BigEndian, &size)
	var identifier byte
	binary.Read(&b, binary.BigEndian, &identifier)
	decodedIr, err := decodeNodeInfoResponse(&b)
	if err != nil {
		t.Error(err)
	}
	if decodedIr.node.Id != 100 {
		t.Error("unexpected nil Id")
	}
	if decodedIr.node.Version != 233453234 {
		t.Error("unexpected nil Version")
	}
}
