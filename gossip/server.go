package gossip

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
)

func (g *Gossip) serve(ctx context.Context, address string) {
	l, err := net.Listen("tcp", address)
	if err != nil {
		panic(err)
	}
	fmt.Println("serving for gossip on", l.Addr().String())

	go func() {
		<-ctx.Done()
		fmt.Println("context closed. gracefully shutting the gossip server down")
		l.Close()
	}()

	for {
		conn, err := l.Accept()
		if err != nil {
			if ctx.Err() != nil {
				fmt.Println("server stopped accepting new conntections")
				return
			}
			fmt.Println("listen error", err)
		}

		go func() {
			for {
				err := g.handleConnection(ctx, conn)
				if err != nil {
					fmt.Printf("error handling connection: %v", err)
					return
				}
			}
		}()
	}
}

func (g *Gossip) handleConnection(ctx context.Context, conn net.Conn) error {
	var l uint32
	if err := binary.Read(conn, binary.BigEndian, &l); err != nil {
		return err
	}
	b := make([]byte, l)
	if _, err := conn.Read(b); err != nil {
		return err
	}
	buff := bytes.NewBuffer(b)
	var identifier byte
	if err := binary.Read(buff, binary.BigEndian, &identifier); err != nil {
		return err
	}
	return g.handleIncoming(ctx, buff, conn, identifier)
}
