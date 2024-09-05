package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"

	pb "github.com/ehsanfa/nimbus/coordinator"
	"github.com/ehsanfa/nimbus/partition"
	"google.golang.org/grpc"
)

type Coordinator struct {
	context context.Context
	cluster cluster
	self    *node
	timeout time.Duration
}

type CoordinateRequestSet struct {
	Key   string
	Value string
}

type CoordinateResponseSet struct {
	Ok    bool
	Error string
}
type CoordinateRequestGet GetRequest
type CoordinateResponseGet GetResponse

func (c Coordinator) Set(req *CoordinateRequestSet, resp *CoordinateResponseSet) error {
	if !c.cluster.isHealthy() {
		resp.Ok = false
		resp.Error = "Not enough replicas"
		return nil
	}
	token := partition.GetToken(req.Key)
	nodes, err := c.cluster.getResponsibleNodes(token)
	if err != nil {
		return err
	}
	proposal := time.Now().UnixMicro()
	prepared := c.prepare(nodes, req.Key, proposal)
	if !prepared {
		fmt.Println("not prepared", req)
		return errors.New("not prepared")
	}
	accepted := c.accept(nodes, req.Key, req.Value, proposal)
	if !accepted {
		fmt.Println("not accepted", req)
		return errors.New("not accepted")
	}
	committed := c.commit(nodes, req.Key, proposal)
	if !committed {
		fmt.Println("not committed", req)
		return errors.New("not committed")
	}

	resp.Ok = true
	return nil
}

func (c Coordinator) prepare(nodes []*node, key string, propose int64) bool {
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
			var resp PrepareResponse
			done := make(chan *rpc.Call, 1)
			call := client.Go("DataStore.Prepare", PrepareRequest{key, propose}, &resp, done)
			select {
			case <-call.Done:
				if resp.Promised {
					consensus = true
				}
			case <-c.context.Done():
				consensus = false
				fmt.Println("cancelled")
			case <-time.After(c.timeout):
				consensus = false
				fmt.Println("prepare timed out")
			}
		}()
	}
	wg.Wait()
	return consensus
}

func (c Coordinator) accept(nodes []*node, key, value string, proposal int64) bool {
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
			var resp AcceptResponse
			done := make(chan *rpc.Call, 1)
			call := client.Go("DataStore.Accept", AcceptRequest{key, proposal, value}, &resp, done)
			select {
			case <-call.Done:
				if resp.Accepted {
					consensus = true
				}
			case <-c.context.Done():
				consensus = false
				fmt.Println("cancelld")
			case <-time.After(c.timeout):
				consensus = false
				// client.Close()
				fmt.Println("accept timed out")
			}
		}()
	}
	wg.Wait()
	return consensus
}

func (c Coordinator) commit(nodes []*node, key string, proposal int64) bool {
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
			var resp CommitResponse
			done := make(chan *rpc.Call, 1)
			call := client.Go("DataStore.Commit", CommitRequest{key, proposal}, &resp, done)
			select {
			case <-call.Done:
				if resp.Committed {
					consensus = true
				}
			case <-c.context.Done():
				fmt.Println("cancelld")
			case <-time.After(c.timeout):
				// client.Close()
				fmt.Println("commit timed out")
			}
		}()
	}
	wg.Wait()
	return consensus
}

func (c Coordinator) Get(req *CoordinateRequestGet, resp *CoordinateResponseGet) error {
	if !c.cluster.isHealthy() {
		resp.Ok = false
		resp.Error = "Not enough replicas"
		return nil
	}
	token := partition.GetToken(req.Key)
	nodes, err := c.cluster.getResponsibleNodes(token)
	if err != nil {
		return err
	}
	for _, node := range nodes {
		client, err := getClient(node.address)
		if err != nil {
			panic(err) // for now
		}
		var serverResp CoordinateResponseGet
		client.Call("DataStore.Get", CoordinateRequestGet{req.Key}, &serverResp)
		if !serverResp.Ok {
			fmt.Println(serverResp.Error)
			resp.Error = serverResp.Error
			resp.Ok = false
			return nil
		}
		resp.Value = serverResp.Value
	}
	resp.Ok = true
	return nil
}

func NewCoordinator(ctx context.Context, self *node, c cluster, lis net.Listener) Coordinator {
	coor := Coordinator{ctx, c, self, 5 * time.Second}
	grpcServer := &server{c: coor}
	s := grpc.NewServer()
	pb.RegisterCoordinatorServiceServer(s, grpcServer)
	go s.Serve(lis)
	return coor
}
