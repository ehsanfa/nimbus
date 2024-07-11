package main

import "testing"

func TestRingPush(t *testing.T) {
	node1 := NewNode("localhost", 9000, "node1")
	node2 := NewNode("localhost", 9001, "node2")
	node3 := NewNode("localhost", 9002, "node3")
	r := NewRing(node1, node2, node3)
	if r.length != 3 {
		t.Error("error")
	}
	if r.next().token != "node1" {
		t.Error("error", r.next().token)
	}
	if r.next().token != "node2" {
		t.Error("error")
	}
	if r.next().token != "node3" {
		t.Error("error")
	}
	if r.next().token != "node1" {
		t.Error("error")
	}
	if err := r.push(node1); err == nil {
		t.Error("error")
	}
	if r.first.node.id() != node1.id() {
		t.Error("unexpected first node")
	}
	if r.last.node.id() != node3.id() {
		t.Error("unexpected last node")
	}

	node4 := NewNode("localhost", 9003, "node3")
	r.push(node4)
	if r.last.node.id() != node4.id() {
		t.Error("expected the last node to change")
	}
}

func TestRingUnlink(t *testing.T) {
	node1 := NewNode("localhost", 9000, "node1")
	node2 := NewNode("localhost", 9001, "node2")
	node3 := NewNode("localhost", 9002, "node3")
	r := NewRing(node1, node2, node3)
	node4 := NewNode("localhost", 9003, "node4")
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

	if n1 := r.next(); n1.id() != node1.id() {
		t.Error("wrong first node")
	}
	if n3 := r.next(); n3.id() != node3.id() {
		t.Error("wrong second node")
	}
	if n1 := r.next(); n1.id() != node1.id() {
		t.Error("incorrect turn")
	}

	err = r.unlink(node1)
	if err != nil {
		t.Error(err)
	}
	if r.first.node.id() != node3.id() {
		t.Error("invalid first node")
	}
	if r.last.node.id() != node3.id() {
		t.Error("invalid last node")
	}
}
