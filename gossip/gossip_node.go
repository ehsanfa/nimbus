package gossip

type nodeStatus byte

const (
	NODE_STATUS_OK          nodeStatus = 1
	NODE_STATUS_UNREACHABLE nodeStatus = 2
	NODE_STATUS_REMOVED     nodeStatus = 3
)

type gossipNode struct {
	id            uint64
	gossipAddress string
	metadata      []byte
	status        nodeStatus
	version       uint64
}

func (n *gossipNode) isOk() bool {
	return n.status == NODE_STATUS_OK
}

func (n *gossipNode) markAsUnreachable() {
	n.status = NODE_STATUS_UNREACHABLE
}

func (n *gossipNode) markAsOk() {
	n.status = NODE_STATUS_OK
}
