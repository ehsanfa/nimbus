package cluster

import (
	"testing"

	"github.com/ehsanfa/nimbus/partition"
)

func TestMinNodesRequired(t *testing.T) {
	var m int
	m = NewCluster(NewNode("localhost:9001", []partition.Token{1}, NODE_STATUS_OK), 0, CONSISTENCY_LEVEL_ONE).minNodesRequired()
	if m != 1 {
		t.Error("Error")
	}
	m = NewCluster(NewNode("localhost:9001", []partition.Token{1}, NODE_STATUS_OK), 3, CONSISTENCY_LEVEL_ONE).minNodesRequired()
	if m != 1 {
		t.Error("Error")
	}
	m = NewCluster(NewNode("localhost:9001", []partition.Token{1}, NODE_STATUS_OK), 3, CONSISTENCY_LEVEL_QUORUM).minNodesRequired()
	if m != 2 {
		t.Error("Error")
	}
	m = NewCluster(NewNode("localhost:9001", []partition.Token{1}, NODE_STATUS_OK), 3, CONSISTENCY_LEVEL_ALL).minNodesRequired()
	if m != 3 {
		t.Error("Error")
	}
}

func TestIsHealthy(t *testing.T) {
	var c *Cluster
	n := NewNode("localhost:9001", []partition.Token{1}, NODE_STATUS_OK)
	c = NewCluster(n, 3, CONSISTENCY_LEVEL_QUORUM)
	if c.IsHealthy() != false {
		t.Error("error")
	}
	n1 := NewNode("localhost:9001", []partition.Token{1}, NODE_STATUS_OK)
	n2 := NewNode("localhost:9001", []partition.Token{2}, NODE_STATUS_OK)
	n3 := NewNode("localhost:9001", []partition.Token{3}, NODE_STATUS_OK)
	c = NewCluster(n1, 3, CONSISTENCY_LEVEL_QUORUM)
	c.AddNodes(n2, n3)
	if c.IsHealthy() != true {
		t.Error("error")
	}
}
