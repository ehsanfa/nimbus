package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	butils "github.com/ehsanfa/nimbus/binary"
	"github.com/ehsanfa/nimbus/partition"
)

type nodeType byte

const (
	NODE_TYPE_WORKER = 1
	NODE_TYPE_PROXY  = 2
)

type metadata struct {
	nodeType      nodeType
	tokens        []partition.Token
	serverAddress string
}

func (m *metadata) encode() ([]byte, error) {
	var b bytes.Buffer
	if err := binary.Write(&b, binary.BigEndian, byte(m.nodeType)); err != nil {
		return make([]byte, 0), err
	}

	if err := binary.Write(&b, binary.BigEndian, uint32(len(m.tokens))); err != nil {
		return make([]byte, 0), err
	}

	for _, token := range m.tokens {
		if err := binary.Write(&b, binary.BigEndian, token); err != nil {
			return make([]byte, 0), err
		}
	}

	if err := butils.EncodeString(m.serverAddress, &b); err != nil {
		return make([]byte, 0), err
	}

	fmt.Println("encoded metadata", b.Bytes())
	return b.Bytes(), nil
}

func decodeMetadata(data []byte) (*metadata, error) {
	fmt.Println("decoding metadata", data)
	m := &metadata{}

	b := bytes.NewBuffer(data)

	var nt byte
	if err := binary.Read(b, binary.BigEndian, &nt); err != nil {
		return m, err
	}
	m.nodeType = nodeType(nt)

	var tokensLen uint32
	if err := binary.Read(b, binary.BigEndian, &tokensLen); err != nil {
		return m, err
	}

	for range tokensLen {
		var token int64
		if err := binary.Read(b, binary.BigEndian, &token); err != nil {
			return m, err
		}
		m.tokens = append(m.tokens, partition.Token(token))
	}

	sa, err := butils.DecodeString(b)
	if err != nil {
		return m, err
	}
	if sa == nil {
		fmt.Println(data)
		return m, errors.New("server address is nil")
	}
	m.serverAddress = *sa

	return m, err
}
