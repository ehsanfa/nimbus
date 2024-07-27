package main

import (
	"context"
	"fmt"
	"net/rpc"
	"time"

	"github.com/ehsanfa/nimbus/partition"
)

const PICK_NODE_MAX_TRIES int = 10

type clusterInfo map[int64]Node

type Gossip struct {
	context context.Context
	cluster cluster
	self    *node
	info    clusterInfo
}

type Node struct {
	Address string
	Token   int64
	Status  int8
	Version int64
}

func (g Gossip) pickNode() *node {
	tries := 0
	for tries < PICK_NODE_MAX_TRIES {
		n := g.cluster.randomNode()
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
	var resp SpreadResponse
	c, err := n.getClient()
	if err != nil {
		fmt.Println(err)
		n.reconnect()
		return
	}

	err = c.Call("Gossip.Spread", SpreadRequest{g.info}, &resp)
	if err != nil {
		fmt.Println(err, "here")
		n.reconnect()
	}
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
	resp.Nodes = g.info
	return nil
}

func (g Gossip) catchUp(initiatorAddress string) error {
	fmt.Println("catching up")
	c, err := rpc.Dial("tcp", initiatorAddress)
	if err != nil {
		return err
	}
	var resp CatchUpResponse
	err = c.Call("Gossip.CatchUp", CatchUpRequest{}, &resp)
	if err != nil {
		return err
	}
	g.handleGossip(resp.Nodes)
	return nil
}

func (g Gossip) handleGossip(nodes map[int64]Node) {
	for t, n := range nodes {
		if _, ok := g.info[t]; !ok {
			node := NewNode(n.Address, partition.Token(n.Token))
			g.info[t] = n
			g.cluster.addNode(&node)
		}
	}
	fmt.Println("gossip handled", g.info)
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
	nodes := make(map[int64]Node)
	for t, n := range c.nodes() {
		nodes[int64(t)] = Node{n.address, int64(n.token), int8(n.status), 1}
	}
	return Gossip{ctx, c, self, nodes}
}
