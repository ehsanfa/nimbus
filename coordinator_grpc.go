package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	pb "github.com/ehsanfa/nimbus/coordinator"
	"github.com/ehsanfa/nimbus/partition"
)

type server struct {
	pb.UnimplementedCoordinatorServiceServer
	c Coordinator
}

func (s *server) Get(_ context.Context, in *pb.CoordinateRequestGet) (*pb.CoordinateResponseGet, error) {
	resp := &pb.CoordinateResponseGet{}
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
		var serverResp CoordinateResponseGet
		client.Call("DataStore.Get", CoordinateRequestGet{in.Key}, &serverResp)
		if !serverResp.Ok {
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

func (s *server) Set(_ context.Context, in *pb.CoordinateRequestSet) (*pb.CoordinateResponseSet, error) {
	resp := &pb.CoordinateResponseSet{}
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
	prepared := s.c.prepare(nodes, in.Key, proposal)
	if !prepared {
		fmt.Println("not prepared", in)
		return resp, errors.New("not prepared")
	}
	accepted := s.c.accept(nodes, in.Key, in.Value, proposal)
	if !accepted {
		fmt.Println("not accepted", in)
		return resp, errors.New("not accepted")
	}
	committed := s.c.commit(nodes, in.Key, proposal)
	if !committed {
		fmt.Println("not committed", in)
		return resp, errors.New("not committed")
	}

	resp.Ok = true
	return resp, nil
}
