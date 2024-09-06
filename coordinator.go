package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	pbcoor "github.com/ehsanfa/nimbus/coordinator"
	pbds "github.com/ehsanfa/nimbus/datastore"
	"github.com/ehsanfa/nimbus/partition"
	"github.com/ehsanfa/nimbus/protocol"
)

type coordinator struct {
	context context.Context
	cluster cluster
	self    *node
	timeout time.Duration
}
type coordinatorServer struct {
	pbcoor.UnimplementedCoordinatorServiceServer
	c coordinator
}

func (c coordinator) prepare(parentCtx context.Context, nodes []*node, key string, proposal int64) bool {
	ctx, cancel := context.WithTimeout(parentCtx, c.timeout)
	defer cancel()
	var wg sync.WaitGroup
	consensus := false
	for _, node := range nodes {
		client, err := getClient(node.address)
		if err != nil {
			fmt.Println(err)
			return false
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := pbds.NewDataStoreServiceClient(client)
			serverResp, err := c.Prepare(ctx, &pbds.PrepareRequest{Key: key, Proposal: proposal})
			if err == nil && serverResp.Promised {
				consensus = true
			}
		}()
	}
	wg.Wait()
	return consensus
}

func (c coordinator) accept(parentCtx context.Context, nodes []*node, key, value string, proposal int64) bool {
	ctx, cancel := context.WithTimeout(parentCtx, c.timeout)
	defer cancel()
	var wg sync.WaitGroup
	consensus := false
	for _, node := range nodes {
		client, err := getClient(node.address)
		if err != nil {
			fmt.Println(err)
			return false
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := pbds.NewDataStoreServiceClient(client)
			serverResp, err := c.Accept(ctx, &pbds.AcceptRequest{Key: key, Proposal: proposal, Value: value})
			if err == nil && serverResp.Accepted {
				consensus = true
			}
		}()
	}
	wg.Wait()
	return consensus
}

func (c coordinator) commit(parentCtx context.Context, nodes []*node, key string, proposal int64) bool {
	ctx, cancel := context.WithTimeout(parentCtx, c.timeout)
	defer cancel()
	var wg sync.WaitGroup
	consensus := false
	for _, node := range nodes {
		client, err := getClient(node.address)
		if err != nil {
			fmt.Println(err)
			return false
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := pbds.NewDataStoreServiceClient(client)
			serverResp, err := c.Commit(ctx, &pbds.CommitRequest{Key: key, Proposal: proposal})
			if err == nil && serverResp.Committed {
				consensus = true
			}
		}()
	}
	wg.Wait()
	return consensus
}

func (s *coordinatorServer) Get(ctx context.Context, in *pbcoor.CoordinateRequestGet) (*pbcoor.CoordinateResponseGet, error) {
	resp := &pbcoor.CoordinateResponseGet{}
	if !s.c.cluster.isHealthy() {
		resp.Ok = false
		resp.Error = "Not enough replicas"
		return resp, nil
	}
	token := partition.GetToken(in.Key)
	nodes, err := s.c.cluster.getResponsibleNodes(token)
	if err != nil {
		return resp, err
	}
	for _, node := range nodes {
		client, err := getClient(node.address)
		if err != nil {
			panic(err) // for now
		}
		c := pbds.NewDataStoreServiceClient(client)
		serverResp, err := c.Get(ctx, &pbds.GetRequest{Key: in.Key})
		if !serverResp.Ok || err != nil {
			fmt.Println(serverResp.Error)
			resp.Error = serverResp.Error
			resp.Ok = false
			return resp, nil
		}
		resp.Value = serverResp.Value
	}
	resp.Ok = true
	return resp, nil
}

func (s *coordinatorServer) Set(ctx context.Context, in *pbcoor.CoordinateRequestSet) (*pbcoor.CoordinateResponseSet, error) {
	tcpClient, err := connectTcp("node1:8080")
	if err != nil {
		panic(err)
	}
	m := protocol.DataStoreMessage{Cmd: 1, Args: []string{"hasan", "hooshang"}}
	if err = m.Encode(tcpClient); err != nil {
		fmt.Println("errrrr", err)
	} else {
		fmt.Println("sent message")
	}
	resp := &pbcoor.CoordinateResponseSet{}
	if !s.c.cluster.isHealthy() {
		resp.Ok = false
		resp.Error = "Not enough replicas"
		return resp, nil
	}
	token := partition.GetToken(in.Key)
	nodes, err := s.c.cluster.getResponsibleNodes(token)
	if err != nil {
		return resp, err
	}
	proposal := time.Now().UnixMicro()
	prepared := s.c.prepare(ctx, nodes, in.Key, proposal)
	if !prepared {
		fmt.Println("not prepared", in)
		return resp, errors.New("not prepared")
	}
	accepted := s.c.accept(ctx, nodes, in.Key, in.Value, proposal)
	if !accepted {
		fmt.Println("not accepted", in)
		return resp, errors.New("not accepted")
	}
	committed := s.c.commit(ctx, nodes, in.Key, proposal)
	if !committed {
		fmt.Println("not committed", in)
		return resp, errors.New("not committed")
	}

	resp.Ok = true
	return resp, nil
}

func NewCoordinatorServer(ctx context.Context, self *node, c cluster, lis net.Listener) *coordinatorServer {
	coor := coordinator{ctx, c, self, 5 * time.Second}
	return &coordinatorServer{c: coor}
}
