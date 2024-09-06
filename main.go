package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	pbcoor "github.com/ehsanfa/nimbus/coordinator"
	pbds "github.com/ehsanfa/nimbus/datastore"
	pbgsp "github.com/ehsanfa/nimbus/gossip"
	"github.com/ehsanfa/nimbus/partition"
	"google.golang.org/grpc"
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

func getToken() []partition.Token {
	var token partition.Token

	givenToken := os.Getenv("TOKEN")
	if givenToken != "" {
		gt, err := strconv.Atoi(givenToken)
		if err != nil {
			panic(err)
		}
		return []partition.Token{partition.Token(gt)}
	}

	tokens := []partition.Token{}
	tokenFile, err := os.Open("/tmp/nimbus/token.nimbus")
	if os.IsNotExist(err) {
		var err error
		err = os.Mkdir("/tmp/nimbus", 0755)
		if err != nil {
			if !os.IsExist(err) {
				panic(err)
			}
		}
		c, err := os.Create("/tmp/nimbus/token.nimbus")
		if err != nil {
			panic(err)
		}
		defer c.Close()
		writer := bufio.NewWriter(c)
		for range 256 {
			token = partition.SuggestToken()
			_, err := fmt.Fprintln(writer, token)
			if err != nil {
				panic(err)
			}
			tokens = append(tokens, token)
		}
		writer.Flush()
	} else if err != nil {
		panic(err)
	} else {
		scanner := bufio.NewScanner(tokenFile)
		for scanner.Scan() {
			tInt, err := strconv.Atoi(string(scanner.Text()))
			if err != nil {
				panic(err)
			}
			tokens = append(tokens, partition.Token(tInt))
		}
	}
	return tokens
}

func getHostname() string {
	givenHostname := os.Getenv("HOST")
	if givenHostname == "" {
		hostName, err := os.Hostname()
		if err != nil {
			panic(err)
		}
		givenHostname = hostName
	}
	return givenHostname
}

func getPort() int {
	givenPort := os.Getenv("PORT")
	if givenPort == "" {
		givenPort = "0"
	}
	port, err := strconv.Atoi(givenPort)
	if err != nil {
		panic(err)
	}
	return port
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
	hostname := getHostname()
	port := getPort()

	l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", hostname, port))
	if err != nil {
		panic(err)
	}
	defer l.Close()

	address := l.Addr().String()
	fmt.Println("serving on ", address)

	self := NewNode(address, getToken(), NODE_STATUS_OK)
	cluster := NewCluster([]*node{&self}, 3, CONSISTENCY_LEVEL_ALL)
	initiatorAddress := os.Getenv("INITIATOR")
	gossip := NewGossipServer(ctx, cluster, &self)

	coordinator := NewCoordinatorServer(ctx, &self, cluster, l)

	d := NewDataStoreServer(ctx, l)
	d.d.dataStore.Rehydrate()

	// handler := rpc.NewServer()
	// handler.Register(d)
	// handler.Register(gossip)
	// handler.Register(coordinator)
	// serve(ctx, l, handler)

	dsb := newDataStoreBinary()
	go dsb.serve()

	s := grpc.NewServer()
	pbds.RegisterDataStoreServiceServer(s, d)
	pbgsp.RegisterGossipServiceServer(s, gossip)
	pbcoor.RegisterCoordinatorServiceServer(s, coordinator)
	go func() {
		if err := s.Serve(l); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	clusterType := os.Getenv("CLUSTER_TYPE")
	if clusterType == "STANDALONE" {
		<-done
		return
	}

	if initiatorAddress == "" {
		if os.Getenv("ONBOARDING_TYPE") == "MULTICASTING" {
			udpAddress := "224.1.1.1:5008"
			StartReceiver(address, udpAddress)
			initiatorAddress, err = GetInitiator(ctx, address, udpAddress)
			if err != nil {
				panic(err)
			}
		} else {
			panic("INITIATOR cannot be null")
		}
	}
	catchupDone := make(chan bool)
	go gossip.g.catchUp(ctx, initiatorAddress, catchupDone)
	<-catchupDone

	gossip.g.start()
	<-done
}
