package gossip

import "testing"

func TestQueue(t *testing.T) {
	q := newQueue()
	if q.pop() != nil {
		t.Error("expected to get nil as the pointer")
	}
	q.add(&gossipNode{id: 12})
	if q.first == nil {
		t.Error("unexpected nil for first node")
	}
	if q.pointer != nil {
		t.Error("unexpected value for pointer node")
	}
	if q.last == nil {
		t.Error("unexpected nil for last node")
	}
	q.add(&gossipNode{id: 15})
	q.add(&gossipNode{id: 16})
	q.add(&gossipNode{id: 17})
	var pn *gossipNode
	pn = q.pop()
	if pn == nil || pn.id != 12 {
		t.Error("unexpected popped node", pn)
	}
	pn = q.pop()
	if pn == nil || pn.id != 15 {
		t.Error("unexpected popped node", pn)
	}
	pn = q.pop()
	if pn == nil || pn.id != 16 {
		t.Error("unexpected popped node", pn)
	}
	pn = q.pop()
	if pn == nil || pn.id != 17 {
		t.Error("unexpected popped node", pn)
	}
	pn = q.pop()
	if pn == nil || pn.id != 12 {
		t.Error("unexpected popped node", pn)
	}
}
