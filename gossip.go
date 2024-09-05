package main

import (
	"context"
	"fmt"
	"net/rpc"
	"sync"
	"time"

	"github.com/ehsanfa/nimbus/partition"
)

const PICK_NODE_MAX_TRIES int = 10

var info sync.Map

type Gossip struct {
	context context.Context
	cluster cluster
	self    *node
}

type Node struct {
	Address string
	Id      int64
	Tokens  []int64
	Status  int8
	Version int64
}

func (node Node) IsActive() bool {
	return node.Status == int8(NODE_STATUS_OK)
}

func convertToNodes() map[int64]Node {
	nodes := make(map[int64]Node)
	info.Range(func(_, node any) bool {
		n, ok := node.(Node)
		if !ok {
			panic("invalid type")
		}
		nodes[n.Id] = n
		return true
	})
	return nodes
}

func (g Gossip) pickNode() *node {
	tries := 0
	for tries < PICK_NODE_MAX_TRIES {
		n := g.cluster.randomNode()
		if !n.isOk() {
			continue
		}
		if n != g.self {
			return n
		}
		tries++
	}
	return nil
}

func (g Gossip) gossip() {
	n := g.pickNode()
	if n == nil {
		fmt.Println("failed to pick a node")
		return
	}

	err := g.spread(n)
	if err != nil {
		g.handleFailure(n)
		fmt.Println(err, "spread error")
	}
}

func (g Gossip) spread(n *node) error {
	var resp SpreadResponse
	c, err := getClient(n.address)
	if err != nil {
		fmt.Println(err, "get client error")
		return err
	}
	err = c.Call("Gossip.Spread", SpreadRequest{convertToNodes()}, &resp)
	if err != nil {
		fmt.Println("error while spreading", err, n.address)
		refreshClient(n.address)
		return err
	}
	return nil
}

func (g Gossip) handleFailure(node *node) {
	node.markAsUnrechable()
	n, ok := info.Load(int64(node.id))
	if !ok {
		panic("node not found")
	}
	retrievedNode, typeOk := n.(Node)
	if !typeOk {
		panic("invalid Node type")
	}
	retrievedNode.Status = int8(NODE_STATUS_UNREACHABLE)
	retrievedNode.Version = latestVersion()
	info.Store(int64(node.id), retrievedNode)
}

func createNode(n *node, version int64) Node {
	tokens := []int64{}
	for _, t := range n.tokens {
		tokens = append(tokens, int64(t))
	}
	return Node{n.address, int64(n.id), tokens, int8(n.status), version}
}

func latestVersion() int64 {
	return time.Now().UnixMicro()
}

func createLatestVersion(n *node) Node {
	return createNode(n, latestVersion())
}

type SpreadRequest struct {
	Nodes map[int64]Node
}

type SpreadResponse struct {
}

func (g Gossip) Spread(req *SpreadRequest, resp *SpreadResponse) error {
	g.handleGossip(req.Nodes)
	return nil
}

type CatchUpRequest struct {
}

type CatchUpResponse struct {
	Nodes map[int64]Node
}

func (g Gossip) CatchUp(req *CatchUpRequest, resp *CatchUpResponse) error {
	resp.Nodes = convertToNodes()
	return nil
}

func (g Gossip) catchUp(ctx context.Context, initiatorAddress string, done chan bool) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var resp CatchUpResponse
	for {
		select {
		case <-ticker.C:
			c, err := rpc.Dial("tcp", initiatorAddress)
			fmt.Println("dialing gossip")
			if err != nil {
				fmt.Println(err)
				continue
			}
			defer c.Close()
			fmt.Println("calling initiator", initiatorAddress)

			callCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()

			call := c.Go("Gossip.CatchUp", CatchUpRequest{}, &resp, nil)
			select {
			case <-call.Done:
				if call.Error != nil {
					fmt.Printf("RPC call failed: %v\n", call.Error)
					continue
				}
				g.handleGossip(resp.Nodes)
				done <- true
				return
			case <-callCtx.Done():
				fmt.Println("RPC call timed out")
				continue
			}
		case <-ctx.Done():
			done <- false
			return
		}
	}
}

func (g Gossip) handleGossip(nodes map[int64]Node) {
	for t, n := range nodes {
		if n.Id == int64(g.self.id) {
			info.Store(n.Id, createLatestVersion(g.self))
			continue
		}
		if knownNode, ok := info.Load(t); !ok {
			g.syncWithCluster(n)
		} else {
			knownNode, ok := knownNode.(Node)
			if !ok {
				panic("unknown type")
			}
			if knownNode.Version > n.Version {
				if !n.IsActive() {
					continue
				}
				node := g.cluster.nodeFromId(nodeId(n.Id))
				if node == nil {
					panic("unknown node in cluster")
				}
				g.spread(node)
				continue
			}
			if !n.IsActive() {
				continue
			}
			if knownNode.Version < n.Version {
				g.syncWithCluster(n)
			}
		}
	}
	// fmt.Println(nodes)
}

func (g Gossip) syncWithCluster(n Node) {
	tokens := []partition.Token{}
	for _, t := range n.Tokens {
		tokens = append(tokens, partition.Token(t))
	}
	node := NewNode(n.Address, tokens, nodeStatus(n.Status))
	info.Store(n.Id, n)
	g.cluster.updateNode(&node)
}

func (g Gossip) start() {
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		for {
			select {
			case <-ticker.C:
				g.gossip()
			case <-g.context.Done():
				return
			}
		}
	}()
}

func NewGossip(ctx context.Context, c cluster, self *node) Gossip {
	for t, n := range c.nodes() {
		info.Store(int64(t), createNode(n, 1))
	}
	return Gossip{ctx, c, self}
}
