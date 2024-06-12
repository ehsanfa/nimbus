package main

type node struct {
	token string
}

type consistencyLevel int
type replicationFactor int

const (
	ONE consistencyLevel = iota
	QUORUM
	ALL
)

type cluster struct {
	ring *ring
	replicationFactor
	consistencyLevel
}

func minNodesRequired(cl consistencyLevel, rf replicationFactor) int {
	switch cl {
	case ONE:
		return 1
	case QUORUM:
		return (int(rf) / 2) + 1
	case ALL:
		return int(rf)
	default:
		panic("invalid replication factor")
	}
}

func (c cluster) isHealthy() bool {
	return c.ring.length >= minNodesRequired(c.consistencyLevel, c.replicationFactor)
}

func NewCluster(nodes []node, rf replicationFactor, cl consistencyLevel) cluster {
	ring := NewRing()
	return cluster{ring, rf, cl}
}
