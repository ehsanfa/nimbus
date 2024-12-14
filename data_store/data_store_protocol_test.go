package datastore

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"
	"time"

	butils "github.com/ehsanfa/nimbus/binary"
)

type laggyBuffer struct {
	bytes.Buffer
}

func (l *laggyBuffer) Write(p []byte) (n int, err error) {
	time.Sleep(10 * time.Second)
	return 0, nil
}

func TestIdentifier(t *testing.T) {
	var b bytes.Buffer
	butils.EncodeIdentifier(IDENTIFIER_DATA_STORE_ACCEPT_REQUEST, &b)

	var identifier byte
	binary.Read(&b, binary.BigEndian, &identifier)
	if identifier != IDENTIFIER_DATA_STORE_ACCEPT_REQUEST {
		t.Error("identifiers expected to match")
	}
}

func TestProposal(t *testing.T) {
	var b bytes.Buffer
	butils.EncodeProposal(2123223, &b)

	p, err := butils.DecodeProposal(&b)
	if err != nil {
		t.Error(err)
	}

	if *p != uint64(2123223) {
		t.Error("proposals expected to match")
	}
}

func TestBool(t *testing.T) {
	var b bytes.Buffer
	butils.EncodeBool(true, &b)

	p, err := butils.DecodeBool(&b)
	if err != nil {
		t.Error(err)
	}

	if *p != true {
		t.Error("bools expected to match")
	}
}

func TestString(t *testing.T) {
	var b bytes.Buffer
	butils.EncodeString("test", &b)

	p, err := butils.DecodeString(&b)
	if err != nil {
		t.Error(err)
	}

	if *p != "test" {
		t.Error("proposals expected to match")
	}
}

func TestPrepareRequestEncoding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var b bytes.Buffer
	pr := prepareRequest{identifier: IDENTIFIER_DATA_STORE_PREPARE_REQUEST, key: []byte("key"), proposal: 22423}
	err := pr.encode(ctx, &b)
	if err != nil {
		t.Error(err)
	}

	var size uint32
	binary.Read(&b, binary.BigEndian, &size)

	var identifier byte
	binary.Read(&b, binary.BigEndian, &identifier)

	prDecoded, err := decodePrepareRequest(&b)
	if err != nil {
		t.Error(err)
	}
	if string(prDecoded.key) != "key" {
		t.Error("expected to match the keys")
	}
	if prDecoded.proposal != 22423 {
		t.Error("proposals don't match")
	}
}

func TestPrepareRequestTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var b laggyBuffer
	pr := prepareRequest{identifier: IDENTIFIER_DATA_STORE_PREPARE_REQUEST, key: []byte("key"), proposal: 22423}
	err := pr.encode(ctx, &b)
	if err == nil {
		t.Error("expected to see errors")
	}
}

func TestPrepareResponseEncoding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var b bytes.Buffer
	pr := prepareResponse{identifier: IDENTIFIER_DATA_STORE_PREPARE_RESPONSE, promised: false, err: []byte("error preparing")}
	err := pr.encode(ctx, &b)
	if err != nil {
		t.Error(err)
	}

	var size uint32
	binary.Read(&b, binary.BigEndian, &size)

	var identifier byte
	binary.Read(&b, binary.BigEndian, &identifier)

	prDecoded, err := decodePrepareResponse(&b)
	if err != nil {
		t.Error(err)
	}
	if prDecoded.promised {
		t.Error("expected to match the promised")
	}
	if prDecoded.err != nil {
		t.Error("errors don't match")
	}
}

func TestPrepareResponseTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var b laggyBuffer
	pr := prepareResponse{identifier: IDENTIFIER_DATA_STORE_PREPARE_REQUEST, promised: true}
	err := pr.encode(ctx, &b)
	if err == nil {
		t.Error("expected to see errors")
	}
}

func TestAcceptRequestEncoding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var b bytes.Buffer
	ar := acceptRequest{identifier: IDENTIFIER_DATA_STORE_ACCEPT_REQUEST, key: []byte("key"), proposal: 22423, value: []byte("value")}
	err := ar.encode(ctx, &b)
	if err != nil {
		t.Error(err)
	}

	var size uint32
	binary.Read(&b, binary.BigEndian, &size)

	var identifier byte
	binary.Read(&b, binary.BigEndian, &identifier)

	arDecoded, err := decodeAcceptRequest(&b)
	if err != nil {
		t.Error(err)
	}
	if string(arDecoded.key) != "key" {
		t.Error("expected to match the keys")
	}
	if arDecoded.proposal != 22423 {
		t.Error("proposals don't match")
	}
	if string(arDecoded.value) != "value" {
		t.Error("values don't match")
	}
}

func TestAcceptRequestTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var b laggyBuffer
	pr := prepareRequest{identifier: IDENTIFIER_DATA_STORE_ACCEPT_REQUEST, key: []byte("key"), proposal: 22423}
	err := pr.encode(ctx, &b)
	if err == nil {
		t.Error("expected to see errors")
	}
}

func TestAcceptResponseEncoding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var b bytes.Buffer
	ar := acceptResponse{identifier: IDENTIFIER_DATA_STORE_ACCEPT_RESPONSE, accepted: false, err: []byte("error accepting")}
	err := ar.encode(ctx, &b)
	if err != nil {
		t.Error(err)
	}

	var size uint32
	binary.Read(&b, binary.BigEndian, &size)

	var identifier byte
	binary.Read(&b, binary.BigEndian, &identifier)

	arDecoded, err := decodeAcceptResponse(&b)
	if err != nil {
		t.Error(err)
	}
	if arDecoded.accepted {
		t.Error("expected to match the accepted")
	}
	if string(arDecoded.err) != "error accepting" {
		t.Error("errors don't match")
	}
}

func TestAcceptResponseTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var b laggyBuffer
	ar := acceptResponse{identifier: IDENTIFIER_DATA_STORE_ACCEPT_RESPONSE, accepted: true}
	err := ar.encode(ctx, &b)
	if err == nil {
		t.Error("expected to see errors")
	}
}

func TestCommitRequestEncoding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var b bytes.Buffer
	cr := commitRequest{identifier: IDENTIFIER_DATA_STORE_COMMIT_REQUEST, key: []byte("key"), proposal: 22423}
	err := cr.encode(ctx, &b)
	if err != nil {
		t.Error(err)
	}

	var size uint32
	binary.Read(&b, binary.BigEndian, &size)

	var identifier byte
	binary.Read(&b, binary.BigEndian, &identifier)

	crDecoded, err := decodeCommitRequest(&b)
	if err != nil {
		t.Error(err)
	}
	if string(crDecoded.key) != "key" {
		t.Error("expected to match the keys")
	}
	if crDecoded.proposal != 22423 {
		t.Error("proposals don't match")
	}
}

func TestCommitRequestTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var b laggyBuffer
	pr := commitRequest{identifier: IDENTIFIER_DATA_STORE_ACCEPT_REQUEST, key: []byte("key"), proposal: 22423}
	err := pr.encode(ctx, &b)
	if err == nil {
		t.Error("expected to see errors")
	}
}

func TestCommitResponseEncoding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var b bytes.Buffer
	ar := commitResponse{identifier: IDENTIFIER_DATA_STORE_COMMIT_RESPONSE, committed: false, err: []byte("error committing")}
	err := ar.encode(ctx, &b)
	if err != nil {
		t.Error(err)
	}

	var size uint32
	binary.Read(&b, binary.BigEndian, &size)

	var identifier byte
	binary.Read(&b, binary.BigEndian, &identifier)

	crDecoded, err := decodeCommitResponse(&b)
	if err != nil {
		t.Error(err)
	}
	if crDecoded.committed {
		t.Error("expected to match the committed")
	}
	if string(crDecoded.err) != "error committing" {
		t.Error("errors don't match", crDecoded.err)
	}
}

func TestCommitResponseTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var b laggyBuffer
	ar := commitResponse{identifier: IDENTIFIER_DATA_STORE_COMMIT_RESPONSE, committed: true}
	err := ar.encode(ctx, &b)
	if err == nil {
		t.Error("expected to see errors")
	}
}

func TestGetRequestEncoding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var b bytes.Buffer
	gr := getRequest{identifier: IDENTIFIER_DATA_STORE_GET_REQUEST, key: []byte("test")}
	err := gr.encode(ctx, &b)
	if err != nil {
		t.Error(err)
	}

	var size uint32
	binary.Read(&b, binary.BigEndian, &size)

	var identifier byte
	binary.Read(&b, binary.BigEndian, &identifier)

	grDecoded, err := decodeGetRequest(&b)
	if err != nil {
		t.Error(err)
	}
	if string(grDecoded.key) != "test" {
		t.Error("expected to match the committed")
	}
}

func TestGetResponseEncoding(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var b bytes.Buffer
	gr := getResponse{value: []byte("test"), err: make([]byte, 0)}
	err := gr.encode(ctx, &b)
	if err != nil {
		t.Error(err)
	}

	var size uint32
	binary.Read(&b, binary.BigEndian, &size)

	grDecoded, err := decodeGetResponse(&b)
	if err != nil {
		t.Error(err)
	}
	if string(grDecoded.value) != "test" {
		t.Error("expected to match the committed")
	}
}
