package gossip

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"

	butils "github.com/ehsanfa/nimbus/binary"
	"github.com/ehsanfa/nimbus/partition"
)

type catchupRequest struct {
	identifier byte
	replyTo    chan catchupResponse
}

func (cr *catchupRequest) encode(ctx context.Context, w io.Writer) error {
	var b bytes.Buffer

	if err := binary.Write(&b, binary.BigEndian, cr.identifier); err != nil {
		return err
	}

	return butils.ContextfulWrite(ctx, w, b)
}

type catchupResponse struct {
	identifier byte
	nodes      []*gossipNode
}

func (cr *catchupResponse) encode(ctx context.Context, w io.Writer) error {
	var b bytes.Buffer

	if err := binary.Write(&b, binary.BigEndian, cr.identifier); err != nil {
		return err
	}
	if err := binary.Write(&b, binary.BigEndian, uint32(len(cr.nodes))); err != nil {
		return err
	}
	for _, n := range cr.nodes {
		if err := n.encode(&b); err != nil {
			return err
		}
	}
	return butils.ContextfulWrite(ctx, w, b)
}

func decodeCatchupResponse(r io.Reader) ([]*gossipNode, error) {
	var len uint32
	if err := binary.Read(r, binary.BigEndian, &len); err != nil {
		return nil, err
	}
	nodes := make([]*gossipNode, len)
	for i := range len {
		if gn, err := decodeGossipNode(r); err != nil {
			return nil, err
		} else {
			nodes[i] = gn
		}
	}
	return nodes, nil
}

func (g *gossipNode) encode(w io.Writer) error {
	if err := binary.Write(w, binary.BigEndian, g.Id); err != nil {
		return err
	}

	if err := butils.EncodeString(g.GossipAddress, w); err != nil {
		return err
	}

	if err := butils.EncodeString(g.DataStoreAddress, w); err != nil {
		return err
	}

	if err := binary.Write(w, binary.BigEndian, uint32(len(g.Tokens))); err != nil {
		return err
	}
	for _, token := range g.Tokens {
		if err := binary.Write(w, binary.BigEndian, token); err != nil {
			return err
		}
	}

	if err := binary.Write(w, binary.BigEndian, g.Status); err != nil {
		return err
	}

	return binary.Write(w, binary.BigEndian, g.Version)
}

func decodeGossipNode(r io.Reader) (*gossipNode, error) {
	gn := &gossipNode{}

	if err := binary.Read(r, binary.BigEndian, &gn.Id); err != nil {
		return nil, err
	}

	ga, err := butils.DecodeString(r)
	if ga == nil || err != nil {
		return nil, err
	}
	gn.GossipAddress = *ga

	dsa, err := butils.DecodeString(r)
	if dsa == nil || err != nil {
		return nil, err
	}
	gn.DataStoreAddress = *dsa

	var tokensLen uint32
	if err := binary.Read(r, binary.BigEndian, &tokensLen); err != nil {
		return nil, err
	}
	for range tokensLen {
		var token int64
		if err := binary.Read(r, binary.BigEndian, &token); err != nil {
			return nil, err
		}
		gn.Tokens = append(gn.Tokens, partition.Token(token))
	}

	if err := binary.Read(r, binary.BigEndian, &gn.Status); err != nil {
		return nil, err
	}

	if err := binary.Read(r, binary.BigEndian, &gn.Version); err != nil {
		return nil, err
	}

	return gn, nil
}

type nodeInfoRequest struct {
	identifier byte
	nodeId     uint64
	replyTo    chan nodeInfoResponse
}

func (ni *nodeInfoRequest) encode(ctx context.Context, w io.Writer) error {
	var b bytes.Buffer

	if err := binary.Write(&b, binary.BigEndian, ni.identifier); err != nil {
		return err
	}

	if err := binary.Write(&b, binary.BigEndian, ni.nodeId); err != nil {
		return err
	}

	return butils.ContextfulWrite(ctx, w, b)
}

func decodeNodeInfoRequest(r io.Reader) (*nodeInfoRequest, error) {
	var nodeId uint64
	if err := binary.Read(r, binary.BigEndian, &nodeId); err != nil {
		return nil, err
	}

	return &nodeInfoRequest{nodeId: nodeId}, nil
}

type nodeInfoResponse struct {
	identifier byte
	node       *gossipNode
	notFound   bool
}

func (nir *nodeInfoResponse) encode(ctx context.Context, w io.Writer) error {
	var b bytes.Buffer

	if err := binary.Write(&b, binary.BigEndian, nir.identifier); err != nil {
		return err
	}

	if err := binary.Write(&b, binary.BigEndian, nir.node == nil); err != nil {
		return err
	}

	if nir.node != nil {
		if err := nir.node.encode(&b); err != nil {
			return err
		}
	}

	return butils.ContextfulWrite(ctx, w, b)
}

func decodeNodeInfoResponse(r io.Reader) (*nodeInfoResponse, error) {
	notFound, err := butils.DecodeBool(r)
	if err != nil {
		return nil, err
	}

	var gn *gossipNode
	if !*notFound {
		gn, err = decodeGossipNode(r)
		if err != nil {
			return nil, err
		}
	}

	nir := &nodeInfoResponse{node: gn, notFound: *notFound}
	return nir, err
}

type spreadRequest struct {
	identifier       byte
	versions         map[uint64]uint64
	announcerAddress string
	replyTo          chan spreadResponse
}

func (sr spreadRequest) encode(ctx context.Context, w io.Writer) error {
	var b bytes.Buffer

	if err := binary.Write(&b, binary.BigEndian, sr.identifier); err != nil {
		return err
	}

	if err := butils.EncodeString(sr.announcerAddress, &b); err != nil {
		return err
	}

	if err := binary.Write(&b, binary.BigEndian, uint32(len(sr.versions))); err != nil {
		return err
	}
	for id, ver := range sr.versions {
		if err := binary.Write(&b, binary.BigEndian, id); err != nil {
			return err
		}
		if err := binary.Write(&b, binary.BigEndian, ver); err != nil {
			return err
		}
	}

	return butils.ContextfulWrite(ctx, w, b)
}

func decodeSpreadRequest(r io.Reader) (*spreadRequest, error) {
	announcerAddress, err := butils.DecodeString(r)
	if err != nil {
		return nil, err
	}

	var len uint32
	if err := binary.Read(r, binary.BigEndian, &len); err != nil {
		return nil, err
	}
	versions := make(map[uint64]uint64, len)
	for range len {
		var id uint64
		if err := binary.Read(r, binary.BigEndian, &id); err != nil {
			return nil, err
		}
		var ver uint64
		if err := binary.Read(r, binary.BigEndian, &ver); err != nil {
			return nil, err
		}
		versions[id] = ver
	}
	return &spreadRequest{versions: versions, announcerAddress: *announcerAddress}, nil
}

type spreadResponse struct {
	identifier byte
	ok         bool
}

func (e *spreadResponse) encode(ctx context.Context, w io.Writer) error {
	var b bytes.Buffer
	if err := binary.Write(&b, binary.BigEndian, e.identifier); err != nil {
		return err
	}
	if err := binary.Write(&b, binary.BigEndian, e.ok); err != nil {
		return err
	}
	return butils.ContextfulWrite(ctx, w, b)
}

func decodeSpreadResponse(r io.Reader) (*spreadResponse, error) {
	sr := &spreadResponse{}

	if err := binary.Read(r, binary.BigEndian, &sr.ok); err != nil {
		return nil, err
	}

	return sr, nil
}
