package datastore

import (
	"bytes"
	"context"
	"fmt"
	"io"

	butils "github.com/ehsanfa/nimbus/binary"
)

type prepareRequest struct {
	identifier byte
	key        []byte
	proposal   uint64
	replyTo    chan prepareResponse
}

func (pr *prepareRequest) encode(ctx context.Context, w io.Writer) error {
	var b bytes.Buffer

	if err := butils.EncodeIdentifier(pr.identifier, &b); err != nil {
		return err
	}
	if err := butils.EncodeBytes(pr.key, &b); err != nil {
		return err
	}
	if err := butils.EncodeProposal(pr.proposal, &b); err != nil {
		return err
	}
	return butils.ContextfulWrite(ctx, w, b)
}

func decodePrepareRequest(r io.Reader) (*prepareRequest, error) {
	pr := &prepareRequest{identifier: IDENTIFIER_DATA_STORE_PREPARE_REQUEST}

	key, err := butils.DecodeStringToBytes(r)
	if err != nil {
		return nil, err
	}
	pr.key = key

	proposal, err := butils.DecodeProposal(r)
	if err != nil {
		return nil, err
	}
	pr.proposal = *proposal

	return pr, nil
}

type prepareResponse struct {
	identifier byte
	promised   bool
	err        []byte
}

func (pr *prepareResponse) encode(ctx context.Context, w io.Writer) error {
	var b bytes.Buffer

	if err := butils.EncodeIdentifier(pr.identifier, &b); err != nil {
		return err
	}
	if err := butils.EncodeBool(pr.promised, &b); err != nil {
		return err
	}
	if err := butils.EncodeBytes(pr.err, &b); err != nil {
		return err
	}
	return butils.ContextfulWrite(ctx, w, b)
}

func decodePrepareResponse(r io.Reader) (*prepareResponse, error) {
	pr := &prepareResponse{}

	_, err := butils.DecodeUInt32(r)
	if err != nil {
		return nil, err
	}
	// var l uint32
	// if err := binary.Read(r, binary.BigEndian, &l); err != nil {
	// 	return nil, err
	// }

	_, err = butils.DecodeIdentifier(r)
	if err != nil {
		return nil, err
	}
	// var identifier byte
	// if err := binary.Read(r, binary.BigEndian, &identifier); err != nil {
	// 	return nil, err
	// }

	promised, err := butils.DecodeBool(r)
	if err != nil {
		return nil, err
	}
	pr.promised = *promised

	errorString, err := butils.DecodeStringToBytes(r)
	if err != nil {
		return nil, err
	}
	if errorString != nil {
		pr.err = errorString
	}

	return pr, nil
}

type acceptRequest struct {
	identifier byte
	key        []byte
	proposal   uint64
	value      []byte
	replyTo    chan acceptResponse
}

func (ar *acceptRequest) encode(ctx context.Context, w io.Writer) error {
	var b bytes.Buffer
	if err := butils.EncodeIdentifier(ar.identifier, &b); err != nil {
		fmt.Println(err)
		return err
	}
	if err := butils.EncodeBytes(ar.key, &b); err != nil {
		fmt.Println(err)
		return err
	}
	if err := butils.EncodeProposal(ar.proposal, &b); err != nil {
		fmt.Println(err)
		return err
	}
	if err := butils.EncodeBytes(ar.value, &b); err != nil {
		return err
	}
	return butils.ContextfulWrite(ctx, w, b)
}

func decodeAcceptRequest(r io.Reader) (*acceptRequest, error) {
	ar := &acceptRequest{}

	key, err := butils.DecodeStringToBytes(r)
	if err != nil {
		return nil, err
	}
	ar.key = key

	proposal, err := butils.DecodeProposal(r)
	if err != nil {
		return nil, err
	}
	ar.proposal = *proposal

	value, err := butils.DecodeStringToBytes(r)
	if err != nil {
		return nil, err
	}
	ar.value = value

	return ar, nil
}

type acceptResponse struct {
	identifier byte
	accepted   bool
	err        []byte
}

func (pr *acceptResponse) encode(ctx context.Context, w io.Writer) error {
	var b bytes.Buffer

	if err := butils.EncodeIdentifier(pr.identifier, &b); err != nil {
		return err
	}
	if err := butils.EncodeBool(pr.accepted, &b); err != nil {
		return err
	}
	if err := butils.EncodeBytes(pr.err, &b); err != nil {
		return err
	}
	return butils.ContextfulWrite(ctx, w, b)
}

func decodeAcceptResponse(r io.Reader) (*acceptResponse, error) {
	pr := &acceptResponse{}

	_, err := butils.DecodeUInt32(r)
	if err != nil {
		return nil, err
	}
	// var l uint32
	// if err := binary.Read(r, binary.BigEndian, &l); err != nil {
	// 	return nil, err
	// }

	_, err = butils.DecodeIdentifier(r)
	if err != nil {
		return nil, err
	}
	// var identifier byte
	// if err := binary.Read(r, binary.BigEndian, &identifier); err != nil {
	// 	return nil, err
	// }

	accepted, err := butils.DecodeBool(r)
	if err != nil {
		return nil, err
	}
	pr.accepted = *accepted

	errorString, err := butils.DecodeStringToBytes(r)
	if err != nil {
		return nil, err
	}
	if errorString != nil {
		pr.err = errorString
	}

	return pr, nil
}

type commitRequest struct {
	identifier byte
	key        []byte
	proposal   uint64
	replyTo    chan commitResponse
}

func (cr *commitRequest) encode(ctx context.Context, w io.Writer) error {
	var b bytes.Buffer
	if err := butils.EncodeIdentifier(cr.identifier, &b); err != nil {
		return err
	}
	if err := butils.EncodeBytes(cr.key, &b); err != nil {
		return err
	}
	if err := butils.EncodeProposal(cr.proposal, &b); err != nil {
		return err
	}
	return butils.ContextfulWrite(ctx, w, b)
}

func decodeCommitRequest(r io.Reader) (*commitRequest, error) {
	cr := &commitRequest{}

	key, err := butils.DecodeStringToBytes(r)
	if err != nil {
		return nil, err
	}
	cr.key = key

	proposal, err := butils.DecodeProposal(r)
	if err != nil {
		return nil, err
	}
	cr.proposal = *proposal

	return cr, nil
}

type commitResponse struct {
	identifier byte
	committed  bool
	err        []byte
}

func (pr *commitResponse) encode(ctx context.Context, w io.Writer) error {
	var b bytes.Buffer

	if err := butils.EncodeIdentifier(pr.identifier, &b); err != nil {
		return err
	}
	if err := butils.EncodeBool(pr.committed, &b); err != nil {
		return err
	}
	if err := butils.EncodeBytes(pr.err, &b); err != nil {
		return err
	}
	return butils.ContextfulWrite(ctx, w, b)
}

func decodeCommitResponse(r io.Reader) (*commitResponse, error) {
	pr := &commitResponse{}

	_, err := butils.DecodeUInt32(r)
	if err != nil {
		return nil, err
	}
	// var l uint32
	// if err := binary.Read(r, binary.BigEndian, &l); err != nil {
	// 	return nil, err
	// }

	_, err = butils.DecodeIdentifier(r)
	if err != nil {
		return nil, err
	}
	// var identifier byte
	// if err := binary.Read(r, binary.BigEndian, &identifier); err != nil {
	// 	return nil, err
	// }

	committed, err := butils.DecodeBool(r)
	if err != nil {
		return nil, err
	}
	pr.committed = *committed

	errorString, err := butils.DecodeStringToBytes(r)
	if err != nil {
		return nil, err
	}
	if errorString != nil {
		pr.err = errorString
	}

	return pr, nil
}
