package main

import (
	"errors"
	"fmt"
	"net/rpc"
	"sync"
)

var connectionPool sync.Map

func getClient(address string) (*rpc.Client, error) {
	c, ok := connectionPool.Load(address)
	if ok {
		asserted, ok := c.(*rpc.Client)
		if !ok {
			return nil, errors.New("failed to assert the rpc.Client type")
		}
		return asserted, nil
	}
	connection, err := connect(address)
	if err != nil {
		return nil, err
	}
	connectionPool.Store(address, connection)
	return connection, nil
}

func refreshClient(address string) error {
	connectionPool.Delete(address)
	_, err := getClient(address)
	return err
}

func connect(address string) (*rpc.Client, error) {
	c, err := rpc.Dial("tcp", address)
	fmt.Println("dialing node to connect", address)
	if err != nil {
		return nil, err
	}
	return c, err
}
