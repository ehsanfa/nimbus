package main

import (
	"fmt"
	"testing"
)

func TestMetadata(t *testing.T) {
	m := metadata{
		nodeType:      NODE_TYPE_WORKER,
		tokens:        getToken(1024),
		serverAddress: "localhost:9001",
	}
	encoded, err := m.encode()
	if err != nil {
		t.Error(err)
	}
	fmt.Println(encoded)

	decoded, err := decodeMetadata(encoded)
	if err != nil {
		t.Error(err)
	}

	if m.nodeType != decoded.nodeType {
		t.Error("node types don't match")
	}
	if len(m.tokens) != len(decoded.tokens) {
		t.Error("token sizes don't match")
	}
	if m.serverAddress != decoded.serverAddress {
		t.Error("server addresses don't match")
	}
}
