package cluster

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

type Cluster struct {
	ring         *ring
	rf           replicationFactor
	cl           consistencyLevel
	nodesByToken *btree.BTree
	currentNode  *Node
}

type nodeItem struct {
	node  *Node
	token partition.Token
}

func (n nodeItem) Less(than btree.Item) bool {
	return n.token.Less(than.(nodeItem).token)
}

func (c *Cluster) minNodesRequired() int {
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

func (c *Cluster) IsHealthy() bool {
	healthyNodes := 0
	for _, elem := range c.ring.elemsById {
		if elem.node.IsOk() {
			healthyNodes++
		}
		if healthyNodes >= c.minNodesRequired() {
			return true
		}
	}
	return false
}

func (c *Cluster) GetResponsibleNodes(token partition.Token) ([]*Node, error) {
	mainNode := c.getMainNode(token)
	nodes := []*Node{}
	elem := c.ring.elemsById[mainNode.Id]
	for i := 1; i <= c.minNodesRequired(); i++ {
		nodes = append(nodes, elem.node)
		elem = elem.next
	}
	return nodes, nil
}

func (c *Cluster) nodeMatchingToken(token partition.Token) *Node {
	var matchingNode *Node
	c.nodesByToken.DescendLessOrEqual(nodeItem{nil, token}, func(i btree.Item) bool {
		matchingNode = i.(nodeItem).node
		return false
	})
	return matchingNode
}

func (c *Cluster) getMainNode(token partition.Token) *Node {
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

func (c *Cluster) RandomNode() (*Node, error) {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	elem := c.ring.nthElem(rnd.Intn(c.ring.length))
	if elem == nil {
		return nil, errors.New("nil elem in the ring")
	}
	return elem.node, nil
}

func (c *Cluster) NodeAfter(n *Node) (*Node, error) {
	if n == nil {
		panic("nil node provided")
	}
	elem, ok := c.ring.elemsById[n.Id]
	if !ok {
		return nil, errors.New("elem not found in the ring")
	}
	if elem == nil {
		return nil, errors.New("nil elem in the ring")
	}
	return elem.next.node, nil
}

func (c *Cluster) Nodes() map[NodeId]*Node {
	nodes := make(map[NodeId]*Node)
	for t, e := range c.ring.elemsById {
		nodes[t] = e.node
	}
	return nodes
}

func (c *Cluster) UpdateNode(node *Node) error {
	knownNode := c.ring.elemsById[node.Id]
	if knownNode == nil {
		return c.AddNode(node)
	}
	c.ring.elemsById[node.Id].node = node
	fmt.Println("here", node, c.ring.elemsById[node.Id].node)
	return nil
}

func (c *Cluster) NodeFromId(id NodeId) *Node {
	elem, ok := c.ring.elemsById[id]
	if !ok {
		return nil
	}
	return elem.node
}

func (c *Cluster) AddNodes(nodes ...*Node) error {
	for _, n := range nodes {
		err := c.AddNode(n)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Cluster) CurrentNode() *Node {
	return c.currentNode
}

func (c *Cluster) AddNode(node *Node) error {
	updateNodeMu.Lock()
	defer updateNodeMu.Unlock()
	for _, t := range node.Tokens {
		if c.nodesByToken.Has(nodeItem{node, t}) {
			return errors.New("nodes with similar tokens already exists")
		}
	}
	err := c.ring.push(node)
	if err != nil {
		return err
	}
	for _, t := range node.Tokens {
		c.nodesByToken.ReplaceOrInsert(nodeItem{node, t})
	}
	return nil
}

func (c *Cluster) removeNode(node *Node) error {
	return c.ring.unlink(node)
}

func NewCluster(currentNode *Node, rf replicationFactor, cl consistencyLevel) *Cluster {
	ring := NewRing()
	c := &Cluster{
		ring:         ring,
		rf:           rf,
		cl:           cl,
		nodesByToken: btree.New(5),
		currentNode:  currentNode,
	}
	err := c.AddNode(currentNode)
	if err != nil {
		panic(err)
	}
	return c
}
