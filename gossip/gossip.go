package gossip

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	cluster "github.com/ehsanfa/nimbus/cluster"
	connectionpool "github.com/ehsanfa/nimbus/connection_pool"
	"github.com/ehsanfa/nimbus/partition"
)

const PICK_NODE_MAX_TRIES int = 10

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
	nodeId        cluster.NodeId
	targetAddress string
}

type Gossip struct {
	versions            map[uint64]uint64
	versionsMutex       sync.RWMutex
	cluster             *cluster.Cluster
	catchupChan         chan catchupRequest
	spreadChan          chan spreadRequest
	cp                  *connectionpool.ConnectionPool
	nodePickingStrategy nodePickingStrategy
	lastPickedNode      *cluster.Node
	nodeInfoBusChan     chan nodeInfoBus
	nodeInfoChan        chan nodeInfoRequest
}

type gossipNode struct {
	Id               uint64
	GossipAddress    string
	DataStoreAddress string
	Tokens           []partition.Token
	Status           uint8
	Version          uint64
}

func (g *Gossip) getVersion(id cluster.NodeId) (uint64, bool) {
	g.versionsMutex.RLock()
	v, ok := g.versions[uint64(id)]
	g.versionsMutex.RUnlock()
	return v, ok
}

func (g *Gossip) convertToNode(id cluster.NodeId) *gossipNode {
	n := g.cluster.NodeFromId(id)
	if n == nil {
		return nil
	}
	v, ok := g.getVersion(id)
	if !ok {
		panic("unknown version")
	}
	return &gossipNode{
		uint64(n.Id),
		n.GossipAddress,
		n.DataStoreAddress,
		n.Tokens,
		uint8(n.Status),
		v,
	}
}

func (g *Gossip) convertToNodes() []*gossipNode {
	var nodes []*gossipNode
	for _, n := range g.cluster.Nodes() {
		nodes = append(nodes, g.convertToNode(n.Id))
	}
	return nodes
}

func (g *Gossip) nextNode() (*cluster.Node, error) {
	switch g.nodePickingStrategy {
	case NODE_PICK_RANDOM:
		return g.cluster.RandomNode()
	case NODE_PICK_NEXT:
		if g.lastPickedNode == nil {
			g.lastPickedNode = g.cluster.CurrentNode()
		}
		pn, err := g.cluster.NodeAfter(g.lastPickedNode)
		if err != nil {
			return nil, err
		}
		g.lastPickedNode = pn
		return pn, nil
	default:
		panic("invalid node picking strategy")
	}
}

func (g *Gossip) pickNode(maxRetries int) (*cluster.Node, error) {
	tries := 0
	var err error
	var pickedNode *cluster.Node
	for tries < maxRetries {
		pickedNode, err = g.nextNode()
		if pickedNode == g.cluster.CurrentNode() {
			tries++
			continue
		}
		return pickedNode, nil
	}
	return nil, err
}

func (g *Gossip) gossip(ctx context.Context) {
	n, err := g.pickNode(PICK_NODE_MAX_TRIES)
	if n == nil {
		fmt.Println("failed to pick a node", err)
		return
	}

	fmt.Println("current versions", g.versions, g.cluster.Nodes())

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	err = g.spread(ctx, n)
	if err != nil {
		fmt.Println(err, "spread error")
		g.handleFailure(n)
		return
	}
	if !n.IsOk() {
		g.handleRecovery(n)
		fmt.Println("marked node as recovered")
	}
}

func (g *Gossip) callToSpread(ctx context.Context, node *cluster.Node, done chan error) {
	client, err := g.cp.GetClient(node.GossipAddress)
	if err != nil {
		done <- err
		return
	}
	sr := spreadRequest{
		identifier:       IDENTIFIER_GOSSIP_SPREAD_REQUEST,
		versions:         g.versions,
		announcerAddress: g.cluster.CurrentNode().GossipAddress,
	}
	err = sr.encode(ctx, client)
	if err != nil {
		done <- err
		return
	}
	var l uint32
	if err := binary.Read(client, binary.BigEndian, &l); err != nil {
		fmt.Println(err)
		return
	}
	b := make([]byte, l)
	client.Read(b)
	buff := bytes.NewBuffer(b)
	var identifier byte
	if err := binary.Read(buff, binary.BigEndian, &identifier); err != nil {
		fmt.Println(err)
		return
	}
	_, err = decodeSpreadResponse(buff)
	if err != nil {
		done <- err
		return
	}
	done <- nil
}

func (g *Gossip) spread(ctx context.Context, n *cluster.Node) error {
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
			fmt.Println("error while spreading", err, n.GossipAddress)
			g.handleFailure(n)
			return err
		}
	}
	return nil
}

func (g *Gossip) handleFailure(node *cluster.Node) {
	node.MarkAsUnrechable()
	retrievedNode := g.readInfo(node.Id)
	if retrievedNode == nil {
		panic(fmt.Sprintf("node %s was not found", node.GossipAddress))
	}
	retrievedNode.Status = cluster.NODE_STATUS_UNREACHABLE
	g.updateToLatestVersion(retrievedNode)
	g.cp.Invalidate(node.GossipAddress)
}

func (g *Gossip) handleRecovery(node *cluster.Node) {
	node.MarkNodeAsOk()
	retrievedNode := g.readInfo(node.Id)
	if retrievedNode == nil {
		panic(fmt.Sprintf("node %s was not found", node.GossipAddress))
	}
	retrievedNode.Status = cluster.NODE_STATUS_OK
	g.updateToLatestVersion(retrievedNode)
}

func (g *Gossip) updateVersion(n *cluster.Node, version uint64) {
	g.versionsMutex.Lock()
	g.versions[uint64(n.Id)] = version
	g.versionsMutex.Unlock()
}

func (g *Gossip) updateToLatestVersion(n *cluster.Node) {
	g.updateVersion(n, uint64(time.Now().UnixMilli()))
}

func (g *Gossip) readInfo(id cluster.NodeId) *cluster.Node {
	return g.cluster.NodeFromId(id)
}

func (g *Gossip) handleGossip(sr spreadRequest) {
	for id, externalVersion := range sr.versions {
		nodeId := cluster.NodeId(id)
		if nodeId == g.cluster.CurrentNode().Id {
			continue
		}
		knownVersion, ok := g.getVersion(nodeId)
		if !ok || knownVersion < externalVersion {
			fmt.Println("new node discovered")
			g.nodeInfoBusChan <- nodeInfoBus{nodeId, sr.announcerAddress}
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

func (g *Gossip) requestNodeInfo(id cluster.NodeId, from string) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	client, err := g.cp.GetClient(from)
	if err != nil {
		fmt.Println(err)
		return
	}
	cr := &nodeInfoRequest{identifier: IDENTIFIER_GOSSIP_NODE_INFO_REQUEST, nodeId: uint64(id)}
	err = cr.encode(ctx, client)
	if err != nil {
		fmt.Println(err)
		return
	}
	var l uint32
	if err := binary.Read(client, binary.BigEndian, &l); err != nil {
		fmt.Println(err)
		return
	}
	b := make([]byte, l)
	client.Read(b)
	buff := bytes.NewBuffer(b)
	var identifier byte
	if err := binary.Read(buff, binary.BigEndian, &identifier); err != nil {
		fmt.Println(err)
		return
	}
	resp, err := decodeNodeInfoResponse(buff)
	if err != nil {
		fmt.Println(err)
		return
	}
	if resp.notFound {
		return
	}
	g.syncWithCluster(resp.node)
}

func (g *Gossip) syncWithCluster(n *gossipNode) error {
	tokens := []partition.Token{}
	if n == nil {
		return errors.New("nil node provided")
	}
	for _, t := range n.Tokens {
		tokens = append(tokens, partition.Token(t))
	}
	if len(tokens) == 0 {
		return errors.New("no tokens provided for the node")
	}
	node := cluster.NewNode(n.GossipAddress, n.DataStoreAddress, tokens, cluster.NodeStatus(n.Status))
	g.updateVersion(node, n.Version)
	return g.cluster.UpdateNode(node)
}

func (g *Gossip) Start(ctx context.Context, initiatorAddress string, interval time.Duration) {
	go g.serve(ctx, g.cluster.CurrentNode().GossipAddress)

	catchupDone := make(chan bool)
	go g.catchUp(ctx, initiatorAddress, catchupDone)
	<-catchupDone

	ticker := time.NewTicker(interval)
	go func() {
		for {
			select {
			case <-ticker.C:
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
			gn := g.convertToNode(cluster.NodeId(nir.nodeId))
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
	ctx context.Context,
	clstr *cluster.Cluster,
	cp *connectionpool.ConnectionPool,
	nodePickingStrategy nodePickingStrategy,
) *Gossip {
	versions := make(map[uint64]uint64)
	catchupChan := make(chan catchupRequest)
	spreadChan := make(chan spreadRequest)
	nodeInfoChan := make(chan nodeInfoRequest)
	nodeInfoBusChan := make(chan nodeInfoBus)
	g := &Gossip{
		versions:            versions,
		cluster:             clstr,
		catchupChan:         catchupChan,
		spreadChan:          spreadChan,
		cp:                  cp,
		nodePickingStrategy: nodePickingStrategy,
		nodeInfoBusChan:     nodeInfoBusChan,
		nodeInfoChan:        nodeInfoChan,
	}

	g.updateToLatestVersion(clstr.CurrentNode())

	go g.handleNodeInfos(ctx)
	go g.nodeInfoRequestBus(ctx)
	go g.handleCatchups(ctx)
	go g.handleSpreads(ctx)
	return g
}
