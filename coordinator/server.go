package coordinator

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"runtime"
)

func (c *Coordinator) serve(ctx context.Context, address string) {
	l, err := net.Listen("tcp", address)
	if err != nil {
		panic(err)
	}
	defer l.Close()
	fmt.Println("serving for coordinator on", l.Addr().String())

	go func() {
		<-ctx.Done()
		fmt.Println("context closed. gracefully shutting the coordinator server down")
		l.Close()
	}()

	// rateLimiter := rate.NewLimiter(rate.Limit(10), 1)

	for {
		// rateLimiter.Wait(ctx)
		conn, err := l.Accept()
		if err != nil {
			if ctx.Err() != nil {
				fmt.Println("server stopped accepting new conntections")
				return
			}
			fmt.Println("listen error", err)
		}
		defer conn.Close()

		go func() {
			for {
				rl := make(chan struct{}, runtime.NumCPU())
				err := c.handleConnection(ctx, conn, rl)
				if err == io.EOF {
					conn.Close()
					return
				}
				<-rl
				// if err != nil {
				// 	fmt.Printf("error handling connection: %v", err)
				// 	return
				// }
			}
		}()
	}
}

func (c *Coordinator) handleConnection(ctx context.Context, conn net.Conn, rl chan struct{}) error {
	var l uint32
	if err := binary.Read(conn, binary.BigEndian, &l); err != nil {
		rl <- struct{}{}
		return err
	}
	b := make([]byte, l)
	if _, err := conn.Read(b); err != nil {
		rl <- struct{}{}
		return err
	}
	buff := bytes.NewBuffer(b)
	var identifier byte
	if err := binary.Read(buff, binary.BigEndian, &identifier); err != nil {
		rl <- struct{}{}
		return err
	}
	go func() {
		err := c.handleIncoming(ctx, buff, conn, identifier)
		if err != nil {
			fmt.Println("error while handling incoming req", err)
		}
		rl <- struct{}{}
	}()
	return nil
}
