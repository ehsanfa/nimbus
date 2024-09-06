package main

import (
	"fmt"
	"io"
	"log"
	"net"

	"github.com/ehsanfa/nimbus/protocol"
)

type dataStoreBinary struct {
}

func (d *dataStoreBinary) serve() {
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal("sdadasd", err)
	}
	defer ln.Close()

	log.Println("dataStoreBinary Server listening on port 8080...")

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println("accept", err)
			continue
		}

		go d.handleConnection(conn)
	}
}

func (d *dataStoreBinary) handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		msg, err := protocol.Decode(conn)
		if err != nil {
			if err != io.EOF {
				fmt.Println(err)
			}
			return
		}

		fmt.Println("received message", len(msg.Args))
	}
}

func newDataStoreBinary() *dataStoreBinary {
	return &dataStoreBinary{}
}
