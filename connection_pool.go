package main

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var connectionPool sync.Map

func getClient(address string) (*grpc.ClientConn, error) {
	c, ok := connectionPool.Load(address)
	if ok {
		asserted, ok := c.(*grpc.ClientConn)
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

func connect(address string) (*grpc.ClientConn, error) {
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	fmt.Println("dialing node to connect", address)
	if err != nil {
		return nil, err
	}
	return conn, err
}

func connectTcp(address string) (net.Conn, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	return conn, nil
}
