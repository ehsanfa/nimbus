package main

import (
	"log"
	"net"
	"net/http"
	"net/rpc"

	"github.com/ehsanfa/nimbus/storage"
)

type Server struct {
	cacher  *Cacher
	address address
}

type Cacher struct {
	storage storage.Storage
	cluster cluster
}

type GetRequest struct {
	Key string
}

type GetResponse struct {
	Value string
	Ok    bool
	Error string
}

func (c *Cacher) Get(req *GetRequest, resp *GetResponse) error {
	val, err := c.storage.Get(req.Key)
	if err != nil {
		resp.Ok = false
		resp.Error = err.Error()
	} else {
		resp.Ok = true
	}
	resp.Value = val
	return nil
}

type SetRequest struct {
	Key   string
	Value string
}

type SetResponse struct {
	Ok    bool
	Error string
}

func (c *Cacher) Set(req *SetRequest, resp *SetResponse) error {
	err := c.storage.Set(req.Key, req.Value)
	if err != nil {
		resp.Ok = false
		resp.Error = err.Error()
	} else {
		resp.Ok = true
	}
	return nil
}

func (s Server) Serve() {
	rpc.Register(s.cacher)
	rpc.HandleHTTP()
	l, err := net.Listen("tcp", s.address.String())
	if err != nil {
		log.Fatal("listen error:", err)
	}
	go http.Serve(l, nil)
}

func NewServer(address address, storage storage.Storage, cluster cluster) Server {
	return Server{
		address: address,
		cacher: &Cacher{
			storage: storage,
			cluster: cluster,
		},
	}
}
