package gossip

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"

	butils "github.com/ehsanfa/nimbus/binary"
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
	nodes      map[uint64]*gossipNode
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
	var b bytes.Buffer

	if err := binary.Write(&b, binary.BigEndian, g.id); err != nil {
		return err
	}

	if err := butils.EncodeString(g.gossipAddress, &b); err != nil {
		return err
	}

	if err := butils.EncodeBytes(g.metadata, &b); err != nil {
		return err
	}

	if err := binary.Write(&b, binary.BigEndian, g.status); err != nil {
		return err
	}

	if err := binary.Write(&b, binary.BigEndian, g.version); err != nil {
		return err
	}

	_, err := b.WriteTo(w)
	return err
}

func decodeGossipNode(r io.Reader) (*gossipNode, error) {
	gn := &gossipNode{}

	if err := binary.Read(r, binary.BigEndian, &gn.id); err != nil {
		return nil, err
	}

	ga, err := butils.DecodeString(r)
	if ga == nil || err != nil {
		return nil, err
	}
	gn.gossipAddress = *ga

	metadata, err := butils.DecodeBytes(r)
	if metadata == nil || err != nil {
		return nil, err
	}
	gn.metadata = metadata

	if err := binary.Read(r, binary.BigEndian, &gn.status); err != nil {
		return nil, err
	}

	if err := binary.Read(r, binary.BigEndian, &gn.version); err != nil {
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
}

func (nir *nodeInfoResponse) encode(ctx context.Context, w io.Writer) error {
	var b bytes.Buffer

	if err := binary.Write(&b, binary.BigEndian, nir.identifier); err != nil {
		return err
	}

	found := nir.node == nil

	if found {
		if err := nir.node.encode(&b); err != nil {
			return err
		}
	}

	return butils.ContextfulWrite(ctx, w, b)
}

func decodeNodeInfoResponse(r io.Reader) (*nodeInfoResponse, error) {
	gn, err := decodeGossipNode(r)
	if err != nil {
		return nil, err
	}
	fmt.Println("decoded gossip node address", []byte(gn.gossipAddress))

	nir := &nodeInfoResponse{node: gn}
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
