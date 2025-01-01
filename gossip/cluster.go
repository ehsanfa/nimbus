package gossip

import (
	"sync"
	"time"
)

type cluster struct {
	info          map[uint64]*gossipNode
	infoMutex     sync.RWMutex
	versions      map[uint64]uint64
	versionsMutex sync.RWMutex
	queue         *queue
	currentNode   *gossipNode
}

func (c *cluster) getNode(id uint64) *gossipNode {
	c.infoMutex.RLock()
	defer c.infoMutex.RUnlock()
	return c.info[id]
}

func (c *cluster) exists(gn *gossipNode) bool {
	return c.getNode(gn.id) != nil
}

func (g *cluster) randomNode() *gossipNode {
	// TDB
	return nil
}

func (c *cluster) nextNode(nps nodePickingStrategy) *gossipNode {
	switch nps {
	case NODE_PICK_RANDOM:
		return c.randomNode()
	case NODE_PICK_NEXT:
		pn := c.queue.pop()
		return pn
	default:
		panic("invalid node picking strategy")
	}
}

func (c *cluster) getVersion(id uint64) (uint64, bool) {
	c.versionsMutex.RLock()
	v, ok := c.versions[id]
	c.versionsMutex.RUnlock()
	return v, ok
}

func (c *cluster) updateVersion(n *gossipNode, version uint64) {
	c.versionsMutex.Lock()
	c.versions[uint64(n.id)] = version
	c.versionsMutex.Unlock()
}

func latestVersion() uint64 {
	return uint64(time.Now().UnixMicro())
}

func (c *cluster) add(node *gossipNode) {
	c.infoMutex.Lock()
	c.info[node.id] = node
	c.infoMutex.Unlock()
	c.updateVersion(node, node.version)
	c.queue.add(node)
}

func newCluster(currentNode Node, gossipAddress string) *cluster {
	cn := &gossipNode{
		id:            currentNode.Id,
		gossipAddress: gossipAddress,
		metadata:      currentNode.Metadata,
		status:        NODE_STATUS_OK,
		version:       latestVersion(),
	}
	c := &cluster{
		info: map[uint64]*gossipNode{
			cn.id: cn,
		},
		versions: map[uint64]uint64{
			cn.id: cn.version,
		},
		queue:       newQueue(),
		currentNode: cn,
	}
	return c
}
