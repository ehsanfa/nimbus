package gossip

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"time"
)

func (g *Gossip) catchUp(ctx context.Context, initiatorAddress string, done chan bool) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			client, err := g.cp.GetClient(initiatorAddress)
			if err != nil {
				fmt.Println(err)
				return
			}
			cr := &catchupRequest{identifier: 33}
			err = cr.encode(ctx, client)
			if err != nil {
				fmt.Println(err)
				done <- false
				return
			}
			var l uint32
			if err := binary.Read(client, binary.BigEndian, &l); err != nil {
				fmt.Println(err)
				done <- false
				return
			}
			b := make([]byte, l)
			client.Read(b)
			buff := bytes.NewBuffer(b)
			var identifier byte
			if err := binary.Read(buff, binary.BigEndian, &identifier); err != nil {
				fmt.Println(err)
				done <- false
				return
			}
			resp, err := decodeCatchupResponse(buff)
			if err != nil {
				fmt.Println(err)
				done <- false
				return
			}
			if len(resp) == 0 {
				continue
			}
			for _, n := range resp {
				g.syncWithCluster(n)
			}
			done <- true
			return

		case <-ctx.Done():
			done <- false
			return
		}
	}
}

func (g *Gossip) handleCatchups(ctx context.Context) {
	defer close(g.catchupChan)

	for {
		select {
		case cr := <-g.catchupChan:
			cr.replyTo <- catchupResponse{identifier: 34, nodes: g.convertToNodes()}
		case <-ctx.Done():
			return
		}
	}
}
