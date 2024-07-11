package main

import (
	"testing"
)

func TestMinNodesRequired(t *testing.T) {
	if m := minNodesRequired(CONSISTENCY_LEVEL_ONE, 0); m != 1 {
		t.Error("Error")
	}
	if m := minNodesRequired(CONSISTENCY_LEVEL_ONE, 3); m != 1 {
		t.Error("Error")
	}
	if m := minNodesRequired(CONSISTENCY_LEVEL_QUORUM, 3); m != 2 {
		t.Error("Error")
	}
	if m := minNodesRequired(CONSISTENCY_LEVEL_ALL, 3); m != 3 {
		t.Error("Error")
	}
}

func TestIsHealthy(t *testing.T) {
	var c cluster
	c = NewCluster([]node{NewNode("localhost", 9000, "node1")}, 3, CONSISTENCY_LEVEL_QUORUM)
	if c.isHealthy() != false {
		t.Error("error")
	}
	c = NewCluster([]node{
		NewNode("localhost", 9000, "node1"),
		NewNode("localhost", 9001, "node2"),
		NewNode("localhost", 9002, "node3"),
	}, 3, CONSISTENCY_LEVEL_QUORUM)
	if c.isHealthy() != true {
		t.Error("error")
	}
}
