package main

import "testing"

func TestClosestElementBefore(t *testing.T) {
	var node node
	node1 := NewNode("localhost:9000", 2)
	node2 := NewNode("localhost:9001", 5)
	node3 := NewNode("localhost:9002", 10)
	r := NewRing(&node1, &node2, &node3)
	node = NewNode("localhost:9004", 3)
	if elem := r.getClosestElemBefore(node.token); *elem.node != node1 {
		t.Error("invalid closest elem", elem.node.token, node1.token)
	}
	node = NewNode("localhost:9004", 50)
	if elem := r.getClosestElemBefore(node.token); *elem.node != node3 {
		t.Error("invalid closest elem", elem.node.token, node3.token)
	}
	node = NewNode("localhost:9004", 7)
	if elem := r.getClosestElemBefore(node.token); *elem.node != node2 {
		t.Error("invalid closest elem", elem.node.token, node2.token)
	}
	node = NewNode("localhost:9004", 1)
	if elem := r.getClosestElemBefore(node.token); *elem.node != node3 {
		t.Error("invalid closest elem", elem.node.token, node3.token)
	}
}

func TestRingPush(t *testing.T) {
	node1 := NewNode("localhost:9000", 10)
	node2 := NewNode("localhost:9001", 20)
	node3 := NewNode("localhost:9002", 30)
	r := NewRing(&node1, &node2, &node3)
	if r.length != 3 {
		t.Error("error")
	}
	if token := r.next().token; token != 10 {
		t.Error("error", token)
	}
	if r.next().token != 20 {
		t.Error("error")
	}
	if r.next().token != 30 {
		t.Error("error")
	}
	if r.next().token != 10 {
		t.Error("error")
	}
	if err := r.push(&node1); err == nil {
		t.Error("error")
	}
	if r.first.node.token != node1.token {
		t.Error("unexpected first node")
	}
	if r.last.node.token != node3.token {
		t.Error("unexpected last node")
	}

	node4 := NewNode("localhost:9003", 4)
	r.push(&node4)
	if r.first.node.token != node4.token {
		t.Error("expected the first node to change")
	}

	node5 := NewNode("localhost:9003", 400)
	r.push(&node5)
	if r.last.node.token != node5.token {
		t.Error("expected the last node to change")
	}
}

func TestRingUnlink(t *testing.T) {
	node1 := NewNode("localhost:9000", 1)
	node2 := NewNode("localhost:9001", 2)
	node3 := NewNode("localhost9002", 3)
	r := NewRing(&node1, &node2, &node3)
	node4 := NewNode("localhost:9003", 4)
	if err := r.unlink(&node4); err == nil {
		t.Error("expected to get errors while unlinking a node which doesn't exist")
	}

	err := r.unlink(&node2)
	if err != nil {
		t.Error(err)
	}

	if r.length != 2 {
		t.Error("expected to see a reduced length")
	}

	if n1 := r.next(); n1.token != node1.token {
		t.Error("wrong first node")
	}
	if n3 := r.next(); n3.token != node3.token {
		t.Error("wrong second node")
	}
	if n1 := r.next(); n1.token != node1.token {
		t.Error("incorrect turn")
	}

	err = r.unlink(&node1)
	if err != nil {
		t.Error(err)
	}
	if r.first.node.token != node3.token {
		t.Error("invalid first node")
	}
	if r.last.node.token != node3.token {
		t.Error("invalid last node")
	}
}

func TestNthNode(t *testing.T) {
	r := NewRing()
	if r.nthElem(1) != nil {
		t.Error("expected to receive nil")
	}
	node1 := NewNode("localhost:9000", 1)
	node2 := NewNode("localhost:9001", 2)
	node3 := NewNode("localhost:9002", 3)
	r = NewRing(&node1, &node2, &node3)
	if r.nthElem(2) == nil || r.nthElem(2).node != &node3 {
		t.Error("expected to receive node 3", r.nthElem(2).node, &node3)
	}
	if r.nthElem(1) == nil || r.nthElem(1).node != &node2 {
		t.Error("expected to receive node 2", r.nthElem(1).node, &node2)
	}
	if r.nthElem(0) == nil || r.nthElem(0).node != &node1 {
		t.Error("expected to receive node 1", r.nthElem(0).node, &node1)
	}
	if r.nthElem(5) != nil {
		t.Error("expected to receive nil")
	}
}
