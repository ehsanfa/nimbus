package coordinator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	cluster "github.com/ehsanfa/nimbus/cluster"
	datastore "github.com/ehsanfa/nimbus/data_store"
	"github.com/ehsanfa/nimbus/partition"
	"github.com/ehsanfa/nimbus/storage"
	"golang.org/x/sync/errgroup"
)

const (
	IDENTIFIER_COORDINATOR_GET_REQUEST byte = iota + 21
	IDENTIFIER_COORDINATOR_GET_RESPONSE
	IDENTIFIER_COORDINATOR_SET_REQUEST
	IDENTIFIER_COORDINATOR_SET_RESPONSE
)

type Coordinator struct {
	datastore      *datastore.DataStore
	cluster        *cluster.Cluster
	timeout        time.Duration
	coreDataStore  *storage.DataStore
	setRequestChan chan setRequest
}

func (c *Coordinator) prepare(parentCtx context.Context, nodes []*cluster.Node, key []byte, proposal uint64) bool {
	ctx, cancel := context.WithTimeout(parentCtx, c.timeout)
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)
	for _, node := range nodes {
		g.Go(func() error {
			return c.datastore.Prepare(ctx, node, key, proposal)
		})
	}
	if err := g.Wait(); err != nil {
		fmt.Printf("prepare error: %v", err)
		return false
	}
	return true
}

func (c *Coordinator) accept(parentCtx context.Context, nodes []*cluster.Node, key, value []byte, proposal uint64) bool {
	ctx, cancel := context.WithTimeout(parentCtx, c.timeout)
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)
	for _, node := range nodes {
		g.Go(func() error {
			return c.datastore.Accept(ctx, node, key, proposal, value)
		})
	}
	if err := g.Wait(); err != nil {
		fmt.Printf("accept error: %v", err)
		return false
	}
	return true
}

func (c *Coordinator) commit(parentCtx context.Context, nodes []*cluster.Node, key []byte, proposal uint64) bool {
	ctx, cancel := context.WithTimeout(parentCtx, c.timeout)
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)
	for _, node := range nodes {
		g.Go(func() error {
			return c.datastore.Commit(ctx, node, key, proposal)
		})
	}
	if err := g.Wait(); err != nil {
		fmt.Printf("accept error: %v", err)
		return false
	}
	return true
}

// func (c *Coordinator) get(ctx context.Context, key string) (string, bool, error) {
// 	if !c.cluster.isHealthy() {
// 		return "", false, errors.New("Not enough healthy replicas")
// 	}
// 	token := partition.GetToken(key)
// 	nodes, err := c.cluster.getResponsibleNodes(token)
// 	if err != nil {
// 		return "", false, err
// 	}
// 	for _, node := range nodes {
// 		c, err := getClient(node.address)
// 		if err != nil {
// 			return "", false, err
// 		}
// 		if !serverResp.Ok || err != nil {
// 			fmt.Println(serverResp.Error)
// 			resp.Error = serverResp.Error
// 			resp.Ok = false
// 			return resp, nil
// 		}
// 		resp.Value = serverResp.Value
// 	}
// 	resp.Ok = true
// 	return resp, nil
// }

func (c *Coordinator) set(ctx context.Context, key []byte, value []byte) (bool, error) {
	if !c.cluster.IsHealthy() {
		return false, errors.New("not enough healthy replicas")
	}
	token := partition.GetToken(key)
	nodes, err := c.cluster.GetResponsibleNodes(token)
	if err != nil {
		return false, err
	}
	proposal := uint64(time.Now().UnixMicro())
	prepared := c.prepare(ctx, nodes, key, proposal)
	if !prepared {
		return false, errors.New("not prepared")
	}
	accepted := c.accept(ctx, nodes, key, value, proposal)
	if !accepted {
		return false, errors.New("not accepted")
	}
	committed := c.commit(ctx, nodes, key, proposal)
	if !committed {
		return false, errors.New("not committed")
	}

	return true, nil
}

// func (c *Coordinator) handleSet(ctx context.Context, ch chan setRequest) {
// 	for {
// 		select {
// 		case sr := <-ch:
// 			ok, err := c.set(ctx, sr.key, sr.value)
// 			errString := make([]byte, 0)
// 			if err != nil {
// 				errString = []byte(err.Error())
// 			}
// 			resp := setResponse{identifier: IDENTIFIER_COORDINATOR_SET_RESPONSE, ok: ok, err: errString}
// 			sr.replyTo <- resp
// 		case <-ctx.Done():
// 			return
// 		}
// 	}
// }

func (c *Coordinator) handleIncoming(ctx context.Context, r io.Reader, w io.Writer, identifier byte) error {
	switch identifier {
	case IDENTIFIER_COORDINATOR_GET_REQUEST:
		// gr, err := decodeGetRequest(r)
	case IDENTIFIER_COORDINATOR_SET_REQUEST:
		sr, err := decodeSetRequest(r)
		if err != nil {
			return err
		}
		ok, err := c.set(ctx, sr.key, sr.value)
		errString := make([]byte, 0)
		if err != nil {
			errString = []byte(err.Error())
		}
		resp := setResponse{identifier: IDENTIFIER_COORDINATOR_SET_RESPONSE, ok: ok, err: errString}
		// sr.replyTo = make(chan setResponse)
		// defer close(sr.replyTo)
		// c.setRequestChan <- *sr
		// resp := <-sr.replyTo
		return resp.encode(ctx, w)
	}
	return nil
}

func NewCoordinator(ctx context.Context, clstr *cluster.Cluster, ds *datastore.DataStore, cds *storage.DataStore, address string) *Coordinator {
	setReqChan := make(chan setRequest)
	c := &Coordinator{
		ds,
		clstr,
		5 * time.Second,
		cds,
		setReqChan,
	}
	go c.serve(ctx, address)
	// go c.handleSet(ctx, setReqChan)
	return c
}
