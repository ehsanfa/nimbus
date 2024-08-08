package main

import (
	"math/rand"
	"time"

	"github.com/ehsanfa/nimbus/partition"
)

type consistencyLevel int
type replicationFactor int

const (
	CONSISTENCY_LEVEL_ONE consistencyLevel = iota
	CONSISTENCY_LEVEL_QUORUM
	CONSISTENCY_LEVEL_ALL
)

type cluster struct {
	ring *ring
	rf   replicationFactor
	cl   consistencyLevel
}

func (c cluster) minNodesRequired() int {
	switch c.cl {
	case CONSISTENCY_LEVEL_ONE:
		return 1
	case CONSISTENCY_LEVEL_QUORUM:
		return (int(c.rf) / 2) + 1
	case CONSISTENCY_LEVEL_ALL:
		return int(c.rf)
	default:
		panic("invalid replication factor")
	}
}

func (c cluster) isHealthy() bool {
	healthyNodes := 0
	for _, elem := range c.ring.elemsByToken {
		if elem.node.isOk() {
			healthyNodes++
		}
		if healthyNodes >= c.minNodesRequired() {
			return true
		}
	}
	return false
}

func (c cluster) getResponsibleNodes(token partition.Token) ([]*node, error) {
	mainNode := c.ring.getClosestElemBefore(token)
	nodes := []*node{}
	elem := mainNode
	for i := 1; i <= c.minNodesRequired(); i++ {
		nodes = append(nodes, elem.node)
		elem = elem.next
	}
	return nodes, nil
}

func (c cluster) randomNode() *node {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	elem := c.ring.nthElem(rnd.Intn(c.ring.length))
	if elem == nil {
		return nil
	}
	return elem.node
}

func (c cluster) nodes() map[partition.Token]*node {
	nodes := make(map[partition.Token]*node)
	for t, e := range c.ring.elemsByToken {
		nodes[t] = e.node
	}
	return nodes
}

func (c cluster) updateNode(node *node) error {
	knownNode := c.ring.elemsByToken[node.token]
	if knownNode == nil {
		return c.addNode(node)
	}
	if knownNode.token == node.token {
		c.ring.elemsByToken[node.token].node = node
		return nil
	}
	switch node.status {
	case NODE_STATUS_OK:
		return c.addNode(node)
	case NODE_STATUS_UNREACHABLE:
		return c.removeNode(node)
	}

	return nil
}

func (c cluster) nodeFromToken(t partition.Token) *node {
	elem, ok := c.ring.elemsByToken[t]
	if !ok {
		return nil
	}
	return elem.node
}

func (c cluster) addNode(node *node) error {
	return c.ring.push(node)
}

func (c cluster) removeNode(node *node) error {
	return c.ring.unlink(node)
}

func NewCluster(nodes []*node, rf replicationFactor, cl consistencyLevel) cluster {
	ring := NewRing(nodes...)
	return cluster{ring, rf, cl}
}
