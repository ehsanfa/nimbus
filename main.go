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

func getToken() partition.Token {
	var token partition.Token

	givenToken := os.Getenv("TOKEN")
	if givenToken != "" {
		gt, err := strconv.Atoi(givenToken)
		if err != nil {
			panic(err)
		}
		return partition.Token(gt)
	}

	tokenFile, err := os.ReadFile("/tmp/nimbus/token.nimbus")
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
		c.Close()
		token = partition.SuggestToken()
		err = os.WriteFile("/tmp/nimbus/token.nimbus", []byte(fmt.Sprintf("%d", token)), 0755)
		if err != nil {
			panic(err)
		}
		return token
	} else if err != nil {
		panic(err)
	} else {
		tInt, err := strconv.Atoi(string(tokenFile))
		if err != nil {
			panic(err)
		}
		return partition.Token(tInt)
	}
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
	givenHostname := os.Getenv("HOST")
	if givenHostname == "" {
		hostName, err := os.Hostname()
		if err != nil {
			panic(err)
		}
		givenHostname = hostName
	}
	givenPort := os.Getenv("PORT")
	if givenPort == "" {
		givenPort = "0"
	}
	port, err := strconv.Atoi(givenPort)
	if err != nil {
		panic(err)
	}
	address := fmt.Sprintf("%s:%d", givenHostname, port)
	l, err := net.Listen("tcp", address)
	if err != nil {
		panic(err)
	}
	defer l.Close()

	address = l.Addr().String()
	fmt.Println("serving on ", address)
	udpAddress := "224.1.1.1:5008"

	go setupMarcoReceiver(address, udpAddress)

	self := NewNode(address, getToken(), NODE_STATUS_OK)
	cluster := NewCluster([]*node{&self}, 5, CONSISTENCY_LEVEL_ONE)
	initiatorAddress := os.Getenv("INITIATOR")
	gossip := NewGossip(ctx, cluster, &self)

	coordinator := NewCoordinator(ctx, &self, cluster)

	d := NewDataStore(ctx, l)

	handler := rpc.NewServer()
	handler.Register(d)
	handler.Register(gossip)
	handler.Register(coordinator)
	serve(ctx, l, handler)

	clusterType := os.Getenv("CLUSTER_TYPE")
	if clusterType == "STANDALONE" {
		<-done
		return
	}

	if initiatorAddress == "" {
		marcoPoloCtx, cancel := context.WithCancel(ctx)
		go marco(marcoPoloCtx, udpAddress)
		initiatorAddress, err = listenPoloReceiver(marcoPoloCtx, address, udpAddress)
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println("initiator address", initiatorAddress)
		cancel()
	}
	catchupDone := make(chan bool)
	go gossip.catchUp(ctx, initiatorAddress, catchupDone)
	<-catchupDone

	gossip.start()
	<-done
}
