package main

import (
	"net/rpc"

	"github.com/ehsanfa/nimbus/partition"
)

type nodeStatus int8

const (
	NODE_STATUS_NEW  nodeStatus = 0
	NODE_STATUS_OK   nodeStatus = 1
	NODE_STATUS_DOWN nodeStatus = 2
)

type node struct {
	address string
	token   partition.Token
	status  nodeStatus
	client  *rpc.Client
}

// func hostnameAndPortFromAddress(addr string) (string, uint16, error) {
// 	u, err := url.Parse("tcp://" + addr)
// 	if err != nil {
// 		return "", 0, err
// 	}
// 	p64, err := strconv.Atoi(u.Port())
// 	if err != nil {
// 		return "", 0, err
// 	}
// 	return u.Hostname(), uint16(p64), nil
// }

func (n *node) getClient() (*rpc.Client, error) {
	if n.client != nil {
		return n.client, nil
	}
	return n.connect()
}

func (n *node) connect() (*rpc.Client, error) {
	c, err := rpc.Dial("tcp", n.address)
	if err != nil {
		return nil, err
	}
	n.client = c
	return n.client, err
}

func (n *node) reconnect() (*rpc.Client, error) {
	return n.connect()
}

func NewNode(address string, token partition.Token) node {
	return node{address, token, NODE_STATUS_NEW, nil}
}
