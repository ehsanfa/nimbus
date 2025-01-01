package cluster

import (
	"testing"

	"github.com/ehsanfa/nimbus/partition"
)

func TestClosestElementBefore(t *testing.T) {
	var node *Node
	node1 := NewNode("localhost:9001", []partition.Token{2}, NODE_STATUS_OK)
	node2 := NewNode("localhost:9001", []partition.Token{5}, NODE_STATUS_OK)
	node3 := NewNode("localhost:9001", []partition.Token{10}, NODE_STATUS_OK)
	r := NewRing(node1, node2, node3)
	node = NewNode("localhost:9001", []partition.Token{3}, NODE_STATUS_OK)
	if elem := r.getClosestElemBefore(node.Id); elem.node.Id != node1.Id {
		t.Error("invalid closest elem", elem.node.Id, node1.Id)
	}
	node = NewNode("localhost:9001", []partition.Token{50}, NODE_STATUS_OK)
	if elem := r.getClosestElemBefore(node.Id); elem.node.Id != node3.Id {
		t.Error("invalid closest elem", elem.node.Id, node3.Id)
	}
	node = NewNode("localhost:9001", []partition.Token{7}, NODE_STATUS_OK)
	if elem := r.getClosestElemBefore(node.Id); elem.node.Id != node2.Id {
		t.Error("invalid closest elem", elem.node.Id, node2.Id)
	}
	node = NewNode("localhost:9001", []partition.Token{1}, NODE_STATUS_OK)
	if elem := r.getClosestElemBefore(node.Id); elem.node.Id != node3.Id {
		t.Error("invalid closest elem", elem.node.Id, node3.Id)
	}
}

func TestRingPush(t *testing.T) {
	node1 := NewNode("localhost:9001", []partition.Token{10}, NODE_STATUS_OK)
	node2 := NewNode("localhost:9001", []partition.Token{20}, NODE_STATUS_OK)
	node3 := NewNode("localhost:9001", []partition.Token{30}, NODE_STATUS_OK)
	r := NewRing(node1, node2, node3)
	if r.length != 3 {
		t.Error("error")
	}
	if token := r.next().node.Id; token != 10 {
		t.Error("error", token)
	}
	if r.next().node.Id != 20 {
		t.Error("error")
	}
	if r.next().node.Id != 30 {
		t.Error("error")
	}
	if r.next().node.Id != 10 {
		t.Error("error")
	}
	if err := r.push(node1); err == nil {
		t.Error("error")
	}
	if r.first.node.Id != node1.Id {
		t.Error("unexpected first node")
	}
	if r.last.node.Id != node3.Id {
		t.Error("unexpected last node")
	}

	node4 := NewNode("localhost:9001", []partition.Token{4}, NODE_STATUS_OK)
	r.push(node4)
	if r.first.node.Id != node4.Id {
		t.Error("expected the first node to change")
	}

	node5 := NewNode("localhost:9001", []partition.Token{400}, NODE_STATUS_OK)
	r.push(node5)
	if r.last.node.Id != node5.Id {
		t.Error("expected the last node to change")
	}
}

func TestRingUnlink(t *testing.T) {
	node1 := NewNode("localhost:9001", []partition.Token{1}, NODE_STATUS_OK)
	node2 := NewNode("localhost:9001", []partition.Token{2}, NODE_STATUS_OK)
	node3 := NewNode("localhost:9001", []partition.Token{3}, NODE_STATUS_OK)
	r := NewRing(node1, node2, node3)
	node4 := NewNode("localhost:9001", []partition.Token{4}, NODE_STATUS_OK)
	if err := r.unlink(node4); err == nil {
		t.Error("expected to get errors while unlinking a node which doesn't exist")
	}

	err := r.unlink(node2)
	if err != nil {
		t.Error(err)
	}

	if r.length != 2 {
		t.Error("expected to see a reduced length")
	}

	if n1 := r.next(); n1.node.Id != node1.Id {
		t.Error("wrong first node")
	}
	if n3 := r.next(); n3.node.Id != node3.Id {
		t.Error("wrong second node")
	}
	if n1 := r.next(); n1.node.Id != node1.Id {
		t.Error("incorrect turn")
	}

	err = r.unlink(node1)
	if err != nil {
		t.Error(err)
	}
	if r.first.node.Id != node3.Id {
		t.Error("invalid first node")
	}
	if r.last.node.Id != node3.Id {
		t.Error("invalid last node")
	}
}

func TestNthNode(t *testing.T) {
	r := NewRing()
	if r.nthElem(1) != nil {
		t.Error("expected to receive nil")
	}
	node1 := NewNode("localhost:9001", []partition.Token{1}, NODE_STATUS_OK)
	node2 := NewNode("localhost:9001", []partition.Token{2}, NODE_STATUS_OK)
	node3 := NewNode("localhost:9001", []partition.Token{3}, NODE_STATUS_OK)
	r = NewRing(node1, node2, node3)
	if r.nthElem(2) == nil || r.nthElem(2).node != node3 {
		t.Error("expected to receive node 3", r.nthElem(2).node, node3)
	}
	if r.nthElem(1) == nil || r.nthElem(1).node != node2 {
		t.Error("expected to receive node 2", r.nthElem(1).node, node2)
	}
	if r.nthElem(0) == nil || r.nthElem(0).node != node1 {
		t.Error("expected to receive node 1", r.nthElem(0).node, node1)
	}
	if r.nthElem(5) != nil {
		t.Error("expected to receive nil")
	}
}
