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
			client, err := g.cp.GetClient(initiatorAddress, "test")
			if err != nil {
				fmt.Println(err, "err ticker catchup")
				return
			}
			cr := &catchupRequest{identifier: IDENTIFIER_GOSSIP_CATCHUP_REQUEST}
			err = cr.encode(ctx, client)
			if err != nil {
				fmt.Println(err, "err ticker catchup2")
				done <- false
				return
			}
			var l uint32
			if err := binary.Read(client, binary.BigEndian, &l); err != nil {
				fmt.Println(err, "err ticker catchup3")
				done <- false
				return
			}
			b := make([]byte, l)
			client.Read(b)
			buff := bytes.NewBuffer(b)
			var identifier byte
			if err := binary.Read(buff, binary.BigEndian, &identifier); err != nil {
				fmt.Println(err, "a")
				done <- false
				return
			}
			resp, err := decodeCatchupResponse(buff)
			if err != nil {
				fmt.Println(err, "b")
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
			cr.replyTo <- catchupResponse{identifier: IDENTIFIER_GOSSIP_CATCHUP_RESPONSE, nodes: g.cluster.info}
		case <-ctx.Done():
			return
		}
	}
}

func (g *Gossip) Catchup(ctx context.Context, initiatorAddress string) {
	catchupDone := make(chan bool)
	go g.catchUp(ctx, initiatorAddress, catchupDone)
	<-catchupDone
}
