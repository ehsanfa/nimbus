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
	datastore     *datastore.DataStore
	cluster       *cluster.Cluster
	timeout       time.Duration
	coreDataStore *storage.DataStore
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

func (c *Coordinator) get(parentCtx context.Context, key []byte) ([]byte, error) {
	if !c.cluster.IsHealthy() {
		return make([]byte, 0), errors.New("not enough healthy replicas")
	}
	token := partition.GetToken(key)
	nodes, err := c.cluster.GetResponsibleNodes(token)
	if err != nil {
		return make([]byte, 0), err
	}
	values := make([][]byte, len(nodes))
	ctx, cancel := context.WithTimeout(parentCtx, c.timeout)
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)
	for i, node := range nodes {
		fmt.Println("node", node.DataStoreAddress)
		g.Go(func() error {
			v, err := c.datastore.Get(ctx, node, key)
			if err != nil {
				return err
			}
			fmt.Println(v)
			values[i] = v
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		fmt.Printf("get error: %v", err)
		return make([]byte, 0), err
	}
	return values[0], nil
}

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

func (c *Coordinator) handleIncoming(ctx context.Context, r io.Reader, w io.Writer, identifier byte) error {
	switch identifier {
	case IDENTIFIER_COORDINATOR_GET_REQUEST:
		gr, err := decodeGetRequest(r)
		if err != nil {
			return err
		}
		fmt.Println("get request decoded", gr.key)
		val, err := c.get(ctx, gr.key)
		errString := make([]byte, 0)
		if err != nil {
			errString = []byte(err.Error())
		}
		fmt.Println("received request for ", gr.key, val, err)
		resp := getResponse{value: val, err: errString}
		return resp.encode(ctx, w)
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
		return resp.encode(ctx, w)
	}
	return nil
}

func NewCoordinator(ctx context.Context, clstr *cluster.Cluster, ds *datastore.DataStore, cds *storage.DataStore, address string) *Coordinator {
	c := &Coordinator{
		ds,
		clstr,
		5 * time.Second,
		cds,
	}
	go c.serve(ctx, address)
	return c
}
