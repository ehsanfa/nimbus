package main

import (
	"testing"
)

func TestMinNodesRequired(t *testing.T) {
	var m int
	m = NewCluster([]*node{}, 0, CONSISTENCY_LEVEL_ONE).minNodesRequired()
	if m != 1 {
		t.Error("Error")
	}
	m = NewCluster([]*node{}, 3, CONSISTENCY_LEVEL_ONE).minNodesRequired()
	if m != 1 {
		t.Error("Error")
	}
	m = NewCluster([]*node{}, 3, CONSISTENCY_LEVEL_QUORUM).minNodesRequired()
	if m != 2 {
		t.Error("Error")
	}
	m = NewCluster([]*node{}, 3, CONSISTENCY_LEVEL_ALL).minNodesRequired()
	if m != 3 {
		t.Error("Error")
	}
}

func TestIsHealthy(t *testing.T) {
	var c cluster
	n := NewNode("localhost:9000", 1)
	c = NewCluster([]*node{&n}, 3, CONSISTENCY_LEVEL_QUORUM)
	if c.isHealthy() != false {
		t.Error("error")
	}
	n1 := NewNode("localhost:9000", 1)
	n2 := NewNode("localhost:9000", 2)
	n3 := NewNode("localhost:9000", 3)
	c = NewCluster([]*node{
		&n1,
		&n2,
		&n3,
	}, 3, CONSISTENCY_LEVEL_QUORUM)
	if c.isHealthy() != true {
		t.Error("error")
	}
}
