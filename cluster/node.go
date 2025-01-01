package cluster

import (
	"github.com/ehsanfa/nimbus/partition"
)

type NodeStatus int8
type NodeId uint64

const (
	NODE_STATUS_OK          NodeStatus = 1
	NODE_STATUS_UNREACHABLE NodeStatus = 2
	NODE_STATUS_REMOVED     NodeStatus = 3
)

type Node struct {
	DataStoreAddress string
	Tokens           []partition.Token
	Status           NodeStatus
	Id               NodeId
}

func (n *Node) MarkAsUnrechable() {
	n.Status = NODE_STATUS_UNREACHABLE
}

func (n *Node) MarkNodeAsOk() {
	n.Status = NODE_STATUS_OK
}

func (n *Node) IsOk() bool {
	return n.Status == NODE_STATUS_OK
}

func NewNode(datastoreAddr string, tokens []partition.Token, status NodeStatus) *Node {
	return &Node{datastoreAddr, tokens, status, NodeId(tokens[0])}
}
