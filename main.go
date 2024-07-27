package main

import (
	"context"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/ehsanfa/nimbus/partition"
)

func serve(ctx context.Context, l net.Listener, handler *rpc.Server) {
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				select {
				case <-ctx.Done():
					fmt.Println("Shutting down server")
					return
				default:
					fmt.Println("Accept error:", err)
					continue
				}
			}
			go func(conn net.Conn) {
				defer conn.Close()

				handler.ServeConn(conn)
				fmt.Println("served")
			}(conn)
		}
	}()
}

func main() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	done := make(chan bool)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-sigs
		cancel()
		done <- true
	}()
	givenAddress := os.Getenv("ADDRESS")
	if givenAddress == "" {
		givenAddress = "localhost"
	}
	givenPort := os.Getenv("PORT")
	if givenPort == "" {
		givenPort = "0"
	}
	port, err := strconv.Atoi(givenPort)
	if err != nil {
		panic(err)
	}
	address := fmt.Sprintf("%s:%d", givenAddress, port)
	l, err := net.Listen("tcp", address)
	if err != nil {
		panic(err)
	}
	address = l.Addr().String()
	fmt.Println("serving on ", address)

	self := NewNode(address, partition.SuggestPartition())

	cluster := NewCluster([]*node{&self}, 5, CONSISTENCY_LEVEL_QUORUM)

	initiatorAddress := os.Getenv("INITIATOR")
	gossip := NewGossip(ctx, cluster, &self)
	if initiatorAddress != "" {
		err := gossip.catchUp(initiatorAddress)
		if err != nil {
			panic(err)
		}
	}

	coordinator := NewCoordinator(ctx, &self, cluster)

	d := NewDataStore(ctx, l)

	handler := rpc.NewServer()
	handler.Register(d)
	handler.Register(gossip)
	handler.Register(coordinator)
	serve(ctx, l, handler)
	gossip.start()
	<-done
}
