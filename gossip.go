package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ehsanfa/nimbus/partition"

	pbgsp "github.com/ehsanfa/nimbus/gossip"
)

const PICK_NODE_MAX_TRIES int = 10

var info sync.Map

type gossip struct {
	context context.Context
	cluster cluster
	self    *node
}

func convertToNodes() map[int64]*pbgsp.Node {
	nodes := make(map[int64]*pbgsp.Node)
	info.Range(func(_, node any) bool {
		n, ok := node.(*pbgsp.Node)
		if !ok {
			panic("invalid type")
		}
		nodes[n.Id] = n
		return true
	})
	return nodes
}

func (g gossip) pickNode() *node {
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

func (g gossip) gossip() {
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

func (g gossip) spread(n *node) error {
	ctx, cancel := context.WithTimeout(g.context, 1*time.Second)
	defer cancel()
	client, err := getClient(n.address)
	if err != nil {
		fmt.Println(err, "get client error")
		return err
	}
	c := pbgsp.NewGossipServiceClient(client)
	_, err = c.Spread(ctx, &pbgsp.SpreadRequest{Nodes: convertToNodes()})
	if err != nil {
		fmt.Println("error while spreading", err, n.address)
		refreshClient(n.address)
		return err
	}
	return nil
}

func (g gossip) handleFailure(node *node) {
	node.markAsUnrechable()
	n, ok := info.Load(int64(node.id))
	if !ok {
		panic("node not found")
	}
	retrievedNode, typeOk := n.(*pbgsp.Node)
	if !typeOk {
		panic("invalid Node type")
	}
	retrievedNode.Status = int32(NODE_STATUS_UNREACHABLE)
	retrievedNode.Version = latestVersion()
	info.Store(int64(node.id), retrievedNode)
}

func createNode(n *node, version int64) *pbgsp.Node {
	tokens := []int64{}
	for _, t := range n.tokens {
		tokens = append(tokens, int64(t))
	}
	return &pbgsp.Node{Address: n.address, Id: int64(n.id), Tokens: tokens, Status: int32(n.status), Version: version}
}

func latestVersion() int64 {
	return time.Now().UnixMicro()
}

func createLatestVersion(n *node) *pbgsp.Node {
	return createNode(n, latestVersion())
}

type gossipServer struct {
	pbgsp.UnimplementedGossipServiceServer
	g gossip
}

func (gs *gossipServer) Spread(ctx context.Context, req *pbgsp.SpreadRequest) (*pbgsp.SpreadResponse, error) {
	gs.g.handleGossip(req.Nodes)
	return &pbgsp.SpreadResponse{}, nil
}

func (gs *gossipServer) CatchUp(ctx context.Context, req *pbgsp.CatchupRequest) (*pbgsp.CatchupResponse, error) {
	resp := &pbgsp.CatchupResponse{}
	resp.Nodes = convertToNodes()
	fmt.Println("call catchup", resp)
	return resp, nil
}

func (g gossip) catchUp(parentCtx context.Context, initiatorAddress string, done chan bool) {
	ctx, cancel := context.WithTimeout(parentCtx, 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			client, err := getClient(initiatorAddress)
			if err != nil {
				fmt.Println(err)
				continue
			}
			defer client.Close()
			fmt.Println("calling initiator", initiatorAddress)
			c := pbgsp.NewGossipServiceClient(client)
			resp, err := c.CatchUp(ctx, &pbgsp.CatchupRequest{})
			if err != nil {
				fmt.Println(err)
				done <- false
				return
			}
			g.handleGossip(resp.Nodes)

		case <-ctx.Done():
			done <- false
			return
		}
	}
}

func (g gossip) handleGossip(nodes map[int64]*pbgsp.Node) {
	for t, n := range nodes {
		if n.Id == int64(g.self.id) {
			info.Store(n.Id, createLatestVersion(g.self))
			continue
		}
		if knownNode, ok := info.Load(t); !ok {
			g.syncWithCluster(n)
		} else {
			knownNode, ok := knownNode.(*pbgsp.Node)
			if !ok {
				panic("unknown type")
			}
			if knownNode.Version > n.Version {
				if n.Status != int32(NODE_STATUS_OK) {
					continue
				}
				node := g.cluster.nodeFromId(nodeId(n.Id))
				if node == nil {
					panic("unknown node in cluster")
				}
				g.spread(node)
				continue
			}
			if n.Status != int32(NODE_STATUS_OK) {
				continue
			}
			if knownNode.Version < n.Version {
				g.syncWithCluster(n)
			}
		}
	}
	// fmt.Println(nodes)
}

func (g gossip) syncWithCluster(n *pbgsp.Node) {
	tokens := []partition.Token{}
	for _, t := range n.Tokens {
		tokens = append(tokens, partition.Token(t))
	}
	node := NewNode(n.Address, tokens, nodeStatus(n.Status))
	info.Store(n.Id, n)
	g.cluster.updateNode(&node)
}

func (g gossip) start() {
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

func NewGossipServer(ctx context.Context, c cluster, self *node) *gossipServer {
	for t, n := range c.nodes() {
		info.Store(int64(t), createNode(n, 1))
	}
	g := gossip{ctx, c, self}
	return &gossipServer{g: g}
}
