package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/ehsanfa/nimbus/cluster"
	connectionpool "github.com/ehsanfa/nimbus/connection_pool"
	"github.com/ehsanfa/nimbus/coordinator"
	datastore "github.com/ehsanfa/nimbus/data_store"
	"github.com/ehsanfa/nimbus/gossip"
	"github.com/ehsanfa/nimbus/partition"
	"github.com/ehsanfa/nimbus/storage"
)

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
		for range 1024 {
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
	// f, err := os.Create(fmt.Sprintf("mem-%s.prof", getHostname()))
	// if err != nil {
	// 	fmt.Println("Could not create CPU profile:", err)
	// 	return
	// }
	// defer f.Close()
	// if err := pprof.WriteHeapProfile(f); err != nil {
	// 	fmt.Println("Could not start CPU profile:", err)
	// 	return
	// }

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	done := make(chan bool)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-sigs
		fmt.Println("signal died")
		done <- true
	}()
	hostname := getHostname()
	port := getPort()

	address := fmt.Sprintf("%s:%d", hostname, port)
	fmt.Println("serving on ", address)

	gossipAddress := fmt.Sprintf("%s:%d", hostname, 9040)
	datastoreAddress := fmt.Sprintf("%s:%d", hostname, 9041)

	self := cluster.NewNode(gossipAddress, datastoreAddress, getToken(), cluster.NODE_STATUS_OK)
	cluster := cluster.NewCluster(self, 3, cluster.CONSISTENCY_LEVEL_ALL)
	initiatorAddress := os.Getenv("INITIATOR")
	cp := connectionpool.NewConnectionPool(
		connectionpool.NewTcpConnector(),
	)
	g := gossip.NewGossip(ctx, cluster, cp, gossip.NODE_PICK_NEXT)

	stg := storage.NewDataStore(ctx)

	ds := datastore.NewDataStore(ctx, stg, cluster, cp)

	coordinator.NewCoordinator(ctx, cluster, ds, stg, address)

	// d.d.dataStore.Rehydrate()

	clusterType := os.Getenv("CLUSTER_TYPE")
	if clusterType == "STANDALONE" {
		<-done
		return
	}

	onboardingType := os.Getenv("ONBOARDING_TYPE")
	if onboardingType == "" {
		onboardingType = "INITIATOR"
	}

	if onboardingType == "INITIATOR" {
		if initiatorAddress == "" {
			panic("INITIATOR cannot be null")
		}
		g.Catchup(ctx, initiatorAddress)
		fmt.Println("cought up")
	}

	// if initiatorAddress == "" {
	// 	if os.Getenv("ONBOARDING_TYPE") == "MULTICASTING" {
	// 		udpAddress := "224.1.1.1:5008"
	// 		StartReceiver(address, udpAddress)
	// 		_, err := GetInitiator(ctx, address, udpAddress)
	// 		if err != nil {
	// 			panic(err)
	// 		}
	// 	} else {
	// 		panic("INITIATOR cannot be null")
	// 	}
	// }

	g.Start(ctx, 1*time.Second)
	<-done
}
