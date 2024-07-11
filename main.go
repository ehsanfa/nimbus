package main

import (
	"os"
	"strconv"

	"github.com/ehsanfa/nimbus/storage"
)

func main() {
	strg := storage.NewStorage()
	n1 := NewNode("localhost", 9022, "120")
	n2 := NewNode("localhost", 9023, "240")
	n3 := NewNode("localhost", 9024, "360")
	cluster := NewCluster([]node{n1, n2, n3}, 1, CONSISTENCY_LEVEL_ALL)
	port, err := strconv.Atoi(os.Getenv("PORT"))
	if err != nil {
		panic(err)
	}
	s := NewServer(address{"localhost", int16(port)}, strg, cluster)
	s.Serve()
	for {
	}
}
