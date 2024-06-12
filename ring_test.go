package main

import "testing"

func TestRing(t *testing.T) {
	node1 := node{"node1"}
	node2 := node{"node2"}
	node3 := node{"node3"}
	r := NewRing()
	r.push(node1)
	r.push(node2)
	r.push(node3)
	r.show()
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
}
