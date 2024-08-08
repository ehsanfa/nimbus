package main

import (
	"context"
	"errors"
	"fmt"
	"net/rpc"
	"sync"
	"time"

	"github.com/ehsanfa/nimbus/partition"
)

type Coordinator struct {
	context context.Context
	cluster cluster
	self    *node
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
		return errors.New("not prepared")
	}
	accepted := c.accept(nodes, req.Key, req.Value, proposal)
	if !accepted {
		return errors.New("not accepted")
	}
	committed := c.commit(nodes, req.Key, proposal)
	if !committed {
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
					// panic("could not promise")
				}
			case <-c.context.Done():
				fmt.Println("cancelled")
			case <-time.After(6 * time.Second):
				// client.Close()
				fmt.Println("timed out")
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
				fmt.Println("cancelld")
			case <-time.After(6 * time.Second):
				// client.Close()
				fmt.Println("timed out")
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
			case <-time.After(6 * time.Second):
				// client.Close()
				fmt.Println("timed out")
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
			// fmt.Println(serverResp.Error)
			resp.Error = serverResp.Error
			resp.Ok = false
			return nil
		}
		resp.Value = serverResp.Value
	}
	resp.Ok = true
	return nil
}

func NewCoordinator(ctx context.Context, self *node, c cluster) Coordinator {
	return Coordinator{ctx, c, self}
}
