package main

import (
	"crypto/sha1"
	"fmt"
)

type address struct {
	hostname string
	port     int16
}

func (a address) String() string {
	return fmt.Sprintf("%s:%d", a.hostname, a.port)
}

type nodeStatus int8

const (
	NODE_STATUS_NEW  nodeStatus = 0
	NODE_STATUS_OK   nodeStatus = 1
	NODE_STATUS_DOWN nodeStatus = 2
)

type node struct {
	address
	token  string
	status nodeStatus
}

func (n node) id() string {
	id := sha1.New()
	id.Write([]byte(n.address.String()))
	return string(id.Sum(nil))
}

func NewNode(hostname string, port int16, token string) node {
	return node{address{hostname, port}, token, NODE_STATUS_NEW}
}

type consistencyLevel int
type replicationFactor int

const (
	CONSISTENCY_LEVEL_ONE consistencyLevel = iota
	CONSISTENCY_LEVEL_QUORUM
	CONSISTENCY_LEVEL_ALL
)

type cluster struct {
	ring *ring
	replicationFactor
	consistencyLevel
}

func minNodesRequired(cl consistencyLevel, rf replicationFactor) int {
	switch cl {
	case CONSISTENCY_LEVEL_ONE:
		return 1
	case CONSISTENCY_LEVEL_QUORUM:
		return (int(rf) / 2) + 1
	case CONSISTENCY_LEVEL_ALL:
		return int(rf)
	default:
		panic("invalid replication factor")
	}
}

func (c cluster) isHealthy() bool {
	return c.length() >= minNodesRequired(c.consistencyLevel, c.replicationFactor)
}

func (c cluster) length() int {
	return c.ring.length
}

func NewCluster(nodes []node, rf replicationFactor, cl consistencyLevel) cluster {
	ring := NewRing(nodes...)
	return cluster{ring, rf, cl}
}
