package main

import (
	"github.com/ehsanfa/nimbus/partition"
)

type nodeStatus int8
type nodeId int64

const (
	NODE_STATUS_OK          nodeStatus = 1
	NODE_STATUS_UNREACHABLE nodeStatus = 2
	NODE_STATUS_REMOVED     nodeStatus = 3
)

type node struct {
	address string
	tokens  []partition.Token
	status  nodeStatus
	id      nodeId
}

func (n *node) markAsUnrechable() {
	n.status = NODE_STATUS_UNREACHABLE
}

func (n *node) isOk() bool {
	return n.status == NODE_STATUS_OK
}

func NewNode(address string, tokens []partition.Token, status nodeStatus) node {
	return node{address, tokens, NODE_STATUS_OK, nodeId(tokens[0])}
}
