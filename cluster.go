package main

import (
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/ehsanfa/nimbus/partition"
	"github.com/google/btree"
)

type consistencyLevel int
type replicationFactor int

var updateNodeMu sync.Mutex

const (
	CONSISTENCY_LEVEL_ONE consistencyLevel = iota
	CONSISTENCY_LEVEL_QUORUM
	CONSISTENCY_LEVEL_ALL
)

type cluster struct {
	ring         *ring
	rf           replicationFactor
	cl           consistencyLevel
	nodesByToken *btree.BTree
}

type nodeItem struct {
	node  *node
	token partition.Token
}

func (n nodeItem) Less(than btree.Item) bool {
	return n.token.Less(than.(nodeItem).token)
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
	for _, elem := range c.ring.elemsById {
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
	mainNode := c.getMainNode(token)
	nodes := []*node{}
	elem := c.ring.elemsById[mainNode.id]
	for i := 1; i <= c.minNodesRequired(); i++ {
		nodes = append(nodes, elem.node)
		elem = elem.next
	}
	return nodes, nil
}

func (c cluster) nodeMatchingToken(token partition.Token) *node {
	var matchingNode *node
	c.nodesByToken.DescendLessOrEqual(nodeItem{nil, token}, func(i btree.Item) bool {
		matchingNode = i.(nodeItem).node
		return false
	})
	return matchingNode
}

func (c cluster) getMainNode(token partition.Token) *node {
	if c.ring.length == 0 {
		panic("empty ring")
	}
	if c.ring.length == 1 {
		return c.ring.first.node
	}

	node := c.nodeMatchingToken(token)
	if node == nil {
		return c.ring.first.node
	}
	return node
}

func (c cluster) randomNode() *node {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	elem := c.ring.nthElem(rnd.Intn(c.ring.length))
	if elem == nil {
		return nil
	}
	return elem.node
}

func (c cluster) nodes() map[nodeId]*node {
	nodes := make(map[nodeId]*node)
	for t, e := range c.ring.elemsById {
		nodes[t] = e.node
	}
	return nodes
}

func (c cluster) updateNode(node *node) error {
	knownNode := c.ring.elemsById[node.id]
	if knownNode == nil {
		return c.addNode(node)
	}
	if knownNode.id == node.id {
		c.ring.elemsById[node.id].node = node
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

func (c cluster) nodeFromId(id nodeId) *node {
	elem, ok := c.ring.elemsById[id]
	if !ok {
		return nil
	}
	return elem.node
}

func (c cluster) addNode(node *node) error {
	updateNodeMu.Lock()
	defer updateNodeMu.Unlock()
	for _, t := range node.tokens {
		if c.nodesByToken.Has(nodeItem{node, t}) {
			return errors.New("nodes with similar tokens already exists")
		}
	}
	err := c.ring.push(node)
	if err != nil {
		return err
	}
	for _, t := range node.tokens {
		c.nodesByToken.ReplaceOrInsert(nodeItem{node, t})
	}
	fmt.Println("tree length", c.nodesByToken.Len())
	return nil
}

func (c cluster) removeNode(node *node) error {
	return c.ring.unlink(node)
}

func NewCluster(nodes []*node, rf replicationFactor, cl consistencyLevel) cluster {
	ring := NewRing()
	c := cluster{
		ring:         ring,
		rf:           rf,
		cl:           cl,
		nodesByToken: btree.New(5),
	}
	for _, n := range nodes {
		err := c.addNode(n)
		if err != nil {
			panic(err)
		}
	}
	return c
}
