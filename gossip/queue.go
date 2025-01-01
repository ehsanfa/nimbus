package gossip

type elem struct {
	node *gossipNode
	prev *elem
	next *elem
}

type queue struct {
	first   *elem
	pointer *elem
	last    *elem
}

func (q *queue) add(gn *gossipNode) {
	e := &elem{
		node: gn,
	}
	if q.first == nil {
		q.first = e
		q.last = e
		return
	}
	e.prev = q.last
	q.last.next = e
	q.last = e
}

func (q *queue) pop() *gossipNode {
	if q.first == nil {
		return nil
	}
	if q.pointer == nil {
		q.pointer = q.first
	}
	n := q.pointer.node
	nxt := q.pointer.next
	if nxt == nil {
		nxt = q.first
	}
	q.pointer = nxt
	return n
}

func newQueue() *queue {
	return &queue{}
}
