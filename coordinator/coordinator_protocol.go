package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"io"

	butils "github.com/ehsanfa/nimbus/binary"
)

type getRequest struct {
	identifier byte
	key        []byte
}

type setRequest struct {
	identifier byte
	key        []byte
	value      []byte
}

func (sr *setRequest) encode(ctx context.Context, w io.Writer) error {
	var b bytes.Buffer

	if err := butils.EncodeIdentifier(sr.identifier, &b); err != nil {
		return err
	}
	if err := butils.EncodeBytes(sr.key, &b); err != nil {
		fmt.Println(err)
		return err
	}
	if err := butils.EncodeBytes(sr.value, &b); err != nil {
		fmt.Println(err)
		return err
	}
	return butils.ContextfulWrite(ctx, w, b)
}

func decodeSetRequest(r io.Reader) (*setRequest, error) {
	sr := &setRequest{}

	key, err := butils.DecodeStringToBytes(r)
	if err != nil {
		return nil, err
	}
	sr.key = key

	value, err := butils.DecodeStringToBytes(r)
	if err != nil {
		return nil, err
	}
	sr.value = value

	return sr, nil
}

type setResponse struct {
	identifier byte
	ok         bool
	err        []byte
}

func (sr *setResponse) encode(ctx context.Context, w io.Writer) error {
	var b bytes.Buffer

	if err := butils.EncodeIdentifier(sr.identifier, &b); err != nil {
		return err
	}
	if err := butils.EncodeBool(sr.ok, &b); err != nil {
		return err
	}
	if err := butils.EncodeBytes(sr.err, &b); err != nil {
		return err
	}
	return butils.ContextfulWrite(ctx, w, b)
}

func decodeSetResponse(r io.Reader) (*setResponse, error) {
	sr := &setResponse{}

	ok, err := butils.DecodeBool(r)
	if err != nil {
		return nil, err
	}
	sr.ok = *ok

	errorString, err := butils.DecodeStringToBytes(r)
	if err != nil {
		return nil, err
	}
	if errorString != nil {
		sr.err = errorString
	}

	return sr, nil
}
