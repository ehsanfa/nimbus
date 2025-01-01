package gossip

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"
)

func TestGossipNodeEncodeAndDecode(t *testing.T) {
	var b bytes.Buffer
	gn := gossipNode{
		id:            100,
		gossipAddress: "node1:9092",
		metadata:      []byte("metadata1"),
		status:        NODE_STATUS_OK,
		version:       233453234,
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
	if decodedGn.id != gn.id {
		t.Error("Id mismatch")
	}
	if decodedGn.gossipAddress != gn.gossipAddress {
		t.Error("GossipAddress mismatch")
	}
	if len(decodedGn.metadata) != len(gn.metadata) {
		t.Error("metadata mismatch")
	}
	if string(decodedGn.metadata) != string(gn.metadata) {
		t.Error("metadata mismatch")
	}
	if decodedGn.status != gn.status {
		t.Error("status mismatch")
	}
	if decodedGn.version != gn.version {
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
	nodes := map[uint64]*gossipNode{
		100: {
			id:            100,
			gossipAddress: "node1:9092",
			metadata:      []byte("metadata1"),
			status:        2,
			version:       233453234,
		},
		101: {
			id:            101,
			gossipAddress: "node1:9092",
			metadata:      []byte("metadata1"),
			status:        2,
			version:       233453234,
		},
		102: {
			id:            102,
			gossipAddress: "node1:9092",
			metadata:      []byte("metadata1"),
			status:        2,
			version:       233453234,
		},
		103: {
			id:            103,
			gossipAddress: "node1:9092",
			metadata:      []byte("metadata1"),
			status:        2,
			version:       233453234,
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
		id:            100,
		gossipAddress: "node1:9092",
		metadata:      []byte("metadata1"),
		status:        2,
		version:       233453234,
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
	if decodedIr.node.id != 100 {
		t.Error("unexpected nil Id")
	}
	if decodedIr.node.version != 233453234 {
		t.Error("unexpected nil Version")
	}
}
