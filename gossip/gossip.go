package gossip

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	connectionpool "github.com/ehsanfa/nimbus/connection_pool"
)

const (
	IDENTIFIER_GOSSIP_SPREAD_REQUEST byte = iota + 31
	IDENTIFIFER_GOSSIP_SPREAD_RESPONSE
	IDENTIFIER_GOSSIP_CATCHUP_REQUEST
	IDENTIFIER_GOSSIP_CATCHUP_RESPONSE
	IDENTIFIER_GOSSIP_NODE_INFO_REQUEST
	IDENTIFIER_GOSSIP_NODE_INFO_RESPONSE
)

type nodePickingStrategy byte

const (
	NODE_PICK_NEXT nodePickingStrategy = iota
	NODE_PICK_RANDOM
)

type nodeInfoBus struct {
	nodeId        uint64
	targetAddress string
}

type Gossip struct {
	cluster             *cluster
	catchupChan         chan catchupRequest
	spreadChan          chan spreadRequest
	cp                  *connectionpool.ConnectionPool
	nodePickingStrategy nodePickingStrategy
	nodeInfoBusChan     chan nodeInfoBus
	nodeInfoChan        chan nodeInfoRequest
	newNodeBus          chan<- Node
	nodeUpdateBus       chan<- NodeUpdate
}

type Node struct {
	Id       uint64
	Metadata []byte
}

type NodeUpdate struct {
	Id          uint64
	IsReachable bool
}

func (g *Gossip) pickNode() (*gossipNode, error) {
	pickedNode := g.cluster.nextNode(g.nodePickingStrategy)
	if pickedNode == nil {
		return nil, errors.New("no node found in the queue")
	}
	return pickedNode, nil
}

func (g *Gossip) gossip(ctx context.Context) {
	n, err := g.pickNode()
	if n == nil {
		fmt.Println("failed to pick a node", err)
		return
	}

	// fmt.Println("current versions", g.versions, g.cluster.Nodes())

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err = g.spread(ctx, n)
	if err != nil {
		fmt.Println(err, "spread error")
		g.handleFailure(n)
		return
	}
	if !n.isOk() {
		g.handleRecovery(n)
		fmt.Println("marked node as recovered")
	}
}

func (g *Gossip) callToSpread(ctx context.Context, node *gossipNode, done chan error) {
	client, err := g.cp.GetClient(node.gossipAddress, "gossip_spread")
	if err != nil {
		done <- err
		return
	}
	sr := spreadRequest{
		identifier:       IDENTIFIER_GOSSIP_SPREAD_REQUEST,
		versions:         g.cluster.versions,
		announcerAddress: g.cluster.currentNode.gossipAddress,
	}
	err = sr.encode(ctx, client)
	if err != nil {
		done <- err
		return
	}
	var l uint32
	if err := binary.Read(client, binary.BigEndian, &l); err != nil {
		fmt.Println(err, "callToSpread read l", l)
		return
	}
	b := make([]byte, l)
	client.Read(b)
	buff := bytes.NewBuffer(b)
	var identifier byte
	if err := binary.Read(buff, binary.BigEndian, &identifier); err != nil {
		fmt.Println(err, "callToSpread read id")
		return
	}
	_, err = decodeSpreadResponse(buff)
	if err != nil {
		done <- err
		return
	}
	done <- nil
}

func (g *Gossip) spread(ctx context.Context, n *gossipNode) error {
	done := make(chan error)
	go g.callToSpread(ctx, n, done)
	select {
	case <-ctx.Done():
		err := ctx.Err()
		if err == context.DeadlineExceeded {
			g.handleFailure(n)
			return errors.New("deadline exceeded")
		}
		return errors.New("context closed")
	case err := <-done:
		if err != nil {
			fmt.Println("error while spreading", err, n.gossipAddress)
			g.handleFailure(n)
			return err
		}
	}
	return nil
}

func (g *Gossip) handleFailure(node *gossipNode) {
	node.markAsUnreachable()
	node.version = latestVersion()
	g.cluster.updateVersion(node, node.version)
	g.syncWithCluster(node)
	g.cp.Invalidate(node.gossipAddress)
}

func (g *Gossip) handleRecovery(node *gossipNode) {
	node.markAsOk()
	g.syncWithCluster(node)
}

func (g *Gossip) handleGossip(sr spreadRequest) {
	for id, externalVersion := range sr.versions {
		if id == g.cluster.currentNode.id {
			continue
		}
		knownVersion, ok := g.cluster.getVersion(id)
		if !ok || knownVersion < externalVersion {
			g.nodeInfoBusChan <- nodeInfoBus{id, sr.announcerAddress}
		}
	}
}

func (g *Gossip) nodeInfoRequestBus(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case ni := <-g.nodeInfoBusChan:
			go g.requestNodeInfo(ni.nodeId, ni.targetAddress)
		}
	}
}

func (g *Gossip) requestNodeInfo(id uint64, from string) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	client, err := g.cp.GetClient(from, "gossip_node_info")
	if err != nil {
		fmt.Println(err, "here1")
		return
	}
	cr := &nodeInfoRequest{identifier: IDENTIFIER_GOSSIP_NODE_INFO_REQUEST, nodeId: id}
	err = cr.encode(ctx, client)
	if err != nil {
		fmt.Println(err, "here2")
		return
	}
	var l uint32
	if err := binary.Read(client, binary.BigEndian, &l); err != nil {
		fmt.Println(err, "here3")
		return
	}
	b := make([]byte, l)
	client.Read(b)
	buff := bytes.NewBuffer(b)
	var identifier byte
	if err := binary.Read(buff, binary.BigEndian, &identifier); err != nil {
		fmt.Println(err, "here4")
		return
	}
	if buff.Available() == 0 {
		fmt.Println("no buff available")
		return
	}
	resp, err := decodeNodeInfoResponse(buff)
	if err != nil {
		fmt.Println(err, "here5")
		return
	}
	if resp.node == nil {
		return
	}
	g.cluster.add(resp.node)
}

func (g *Gossip) syncWithCluster(n *gossipNode) {
	if !g.cluster.exists(n) {
		g.addNewNode(n)
	} else {
		g.updateNode(n)
	}
}

func (g *Gossip) addNewNode(n *gossipNode) {
	g.newNodeBus <- Node{
		Id:       n.id,
		Metadata: n.metadata,
	}
	g.cluster.add(n)
}

func (g *Gossip) updateNode(n *gossipNode) {
	g.nodeUpdateBus <- NodeUpdate{
		Id:          n.id,
		IsReachable: n.isOk(),
	}
	g.cluster.updateVersion(n, n.version)
}

func (g *Gossip) Start(ctx context.Context, interval time.Duration) {

	gossipTicker := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-gossipTicker.C:
				fmt.Println(g.cluster.info, g.cluster.versions)
				g.gossip(ctx)
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (g *Gossip) handleSpreads(ctx context.Context) {
	defer close(g.spreadChan)

	for {
		select {
		case sr := <-g.spreadChan:
			g.handleGossip(sr)
			sr.replyTo <- spreadResponse{identifier: IDENTIFIFER_GOSSIP_SPREAD_RESPONSE, ok: true}
		case <-ctx.Done():
			return
		}
	}
}

func (g *Gossip) handleNodeInfos(ctx context.Context) {
	defer close(g.nodeInfoChan)

	for {
		select {
		case nir := <-g.nodeInfoChan:
			gn := g.cluster.getNode(nir.nodeId)
			nir.replyTo <- nodeInfoResponse{identifier: IDENTIFIER_GOSSIP_NODE_INFO_RESPONSE, node: gn}
		case <-ctx.Done():
			return
		}
	}
}

func (g *Gossip) handleIncoming(ctx context.Context, r io.Reader, w io.Writer, identifier byte) error {
	switch identifier {
	case IDENTIFIER_GOSSIP_SPREAD_REQUEST:
		sreq, err := decodeSpreadRequest(r)
		if err != nil {
			return err
		}
		ch := make(chan spreadResponse)
		sreq.replyTo = ch
		g.spreadChan <- *sreq
		sresp := <-ch
		sresp.encode(ctx, w)
	case IDENTIFIFER_GOSSIP_SPREAD_RESPONSE:
	case IDENTIFIER_GOSSIP_CATCHUP_REQUEST:
		ch := make(chan catchupResponse)
		g.catchupChan <- catchupRequest{replyTo: ch}
		resp := <-ch
		resp.encode(ctx, w)
		close(ch)
	case IDENTIFIER_GOSSIP_CATCHUP_RESPONSE:
	case IDENTIFIER_GOSSIP_NODE_INFO_REQUEST:
		nir, err := decodeNodeInfoRequest(r)
		if err != nil {
			return err
		}
		ch := make(chan nodeInfoResponse)
		nir.replyTo = ch
		g.nodeInfoChan <- *nir
		resp := <-ch
		resp.encode(ctx, w)
	default:
		panic("unknown identifier")
	}

	return nil
}

func NewGossip(
	currentNode Node,
	cp *connectionpool.ConnectionPool,
	serverAddress string,
	nodePickingStrategy nodePickingStrategy,
	newNodeBus chan<- Node,
	nodeUpdateBus chan<- NodeUpdate,
) *Gossip {
	catchupChan := make(chan catchupRequest)
	spreadChan := make(chan spreadRequest)
	nodeInfoChan := make(chan nodeInfoRequest)
	nodeInfoBusChan := make(chan nodeInfoBus)
	cluster := newCluster(currentNode, serverAddress)
	g := &Gossip{
		cluster:             cluster,
		catchupChan:         catchupChan,
		spreadChan:          spreadChan,
		cp:                  cp,
		nodePickingStrategy: nodePickingStrategy,
		nodeInfoBusChan:     nodeInfoBusChan,
		nodeInfoChan:        nodeInfoChan,
		newNodeBus:          newNodeBus,
		nodeUpdateBus:       nodeUpdateBus,
	}
	return g
}

func (g *Gossip) Setup(ctx context.Context) {
	go g.serve(ctx, g.cluster.currentNode.gossipAddress)

	go g.handleNodeInfos(ctx)
	go g.nodeInfoRequestBus(ctx)
	go g.handleCatchups(ctx)
	go g.handleSpreads(ctx)
}
