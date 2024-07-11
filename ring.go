package main

import "errors"

type element struct {
	prev *element
	node
	next *element
}

type ring struct {
	length       int
	first        *element
	last         *element
	pos          *element
	nodesById    map[string]*element
	nodesByToken map[string]*element
}

func (r *ring) push(node node) error {
	elem := element{node: node}
	if _, ok := r.nodesById[node.id()]; ok {
		return errors.New("node already exists in the ring")
	}

	if r.length == 0 {
		elem.next = &elem
		elem.prev = &elem
		r.first = &elem
		r.last = &elem
		r.length += 1
		r.nodesById[node.id()] = &elem
		r.nodesByToken[node.token] = &elem
		return nil
	}

	elem.next = r.first
	elem.prev = r.last
	r.last.next = &elem
	r.first.prev = &elem
	r.last = &elem
	r.length += 1
	r.nodesById[node.id()] = &elem
	r.nodesByToken[node.token] = &elem
	return nil
}

func (r *ring) unlink(node node) error {
	elem, ok := r.nodesById[node.id()]
	if !ok {
		return errors.New("node doesn't exist in the ring")
	}

	if r.length == 1 {
		r.reset()
		return nil
	}

	if r.first == elem {
		r.first = elem.next
	}
	if r.last == elem {
		r.last = elem.prev
	}

	// node's previous node will be the next node's previous
	elem.next.prev = elem.prev
	// node's next node will be the previous node's next
	elem.prev.next = elem.next
	// detach the node
	elem.next = nil
	elem.prev = nil

	delete(r.nodesById, node.id())
	delete(r.nodesByToken, node.token)
	r.length -= 1
	return nil
}

func (r *ring) next() node {
	if r.pos == nil {
		r.pos = r.first.prev
	}
	r.pos = r.pos.next
	return r.pos.node
}

func (r *ring) rewind() {
	r.pos = nil
}

func (r *ring) reset() {
	r.first = nil
	r.last = nil
	r.pos = nil
	r.nodesById = make(map[string]*element)
	r.length = 0
}

func (r *ring) getByNode(node node) (*element, error) {
	elem, ok := r.nodesById[node.id()]
	if !ok {
		return nil, errors.New("provided node doesn't exist in the ring")
	}

	return elem, nil
}

func (r *ring) getNodeByToken(token string) (*element, error) {
	elem, ok := r.nodesByToken[token]
	if !ok {
		return nil, errors.New("provided node doesn't exist in the ring")
	}

	return elem, nil
}

func NewRing(nodes ...node) *ring {
	r := &ring{pos: nil, nodesById: make(map[string]*element), nodesByToken: make(map[string]*element)}
	for _, n := range nodes {
		r.push(n)
	}
	return r
}
