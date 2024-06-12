package main

import (
	"testing"
)

func TestMinNodesRequired(t *testing.T) {
	if m := minNodesRequired(ONE, 0); m != 1 {
		t.Error("Error")
	}
	if m := minNodesRequired(ONE, 3); m != 1 {
		t.Error("Error")
	}
	if m := minNodesRequired(QUORUM, 3); m != 2 {
		t.Error("Error")
	}
	if m := minNodesRequired(ALL, 3); m != 3 {
		t.Error("Error")
	}
}

func TestIsHealthy(t *testing.T) {
	var c cluster
	c = NewCluster([]node{{"node1"}}, 3, QUORUM)
	if c.isHealthy() != false {
		t.Error("error")
	}
	c = NewCluster([]node{{"node1"}, {"node2"}, {"node3"}}, 3, QUORUM)
	if c.isHealthy() != true {
		t.Error("error")
	}
}
