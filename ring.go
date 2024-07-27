package main

import (
	"errors"

	"github.com/ehsanfa/nimbus/partition"
)

type element struct {
	prev *element
	*node
	next *element
}

type ring struct {
	length       int
	first        *element
	last         *element
	pos          *element
	elemsByToken map[partition.Token]*element
}

func (r *ring) push(node *node) error {
	if _, ok := r.elemsByToken[node.token]; ok {
		return errors.New("node already exists in the ring")
	}

	elem := element{node: node}
	if r.length == 0 {
		elem.next = &elem
		elem.prev = &elem
		r.first = &elem
		r.last = &elem
		r.length += 1
		r.elemsByToken[node.token] = &elem
		return nil
	}

	closestElemBefore := r.getClosestElemBefore(node.token)

	elem.next = closestElemBefore.next
	elem.prev = closestElemBefore
	closestElemBefore.next.prev = &elem
	closestElemBefore.next = &elem
	if closestElemBefore == r.last {
		if closestElemBefore.node.token > elem.node.token {
			r.first = &elem
		} else {
			r.last = &elem
		}
	}
	r.length += 1
	r.elemsByToken[node.token] = &elem
	return nil
}

func (r *ring) unlink(node *node) error {
	elem, ok := r.elemsByToken[node.token]
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

	delete(r.elemsByToken, node.token)
	r.length -= 1
	return nil
}

func (r *ring) next() *element {
	if r.pos == nil {
		r.pos = r.first.prev
	}
	r.pos = r.pos.next
	return r.pos
}

func (r *ring) reset() {
	r.first = nil
	r.last = nil
	r.pos = nil
	r.elemsByToken = make(map[partition.Token]*element)
	r.length = 0
}

func (r *ring) getClosestElemBefore(token partition.Token) *element {
	if r.length == 0 {
		panic("empty ring")
	}
	if r.length == 1 {
		return r.first
	}
	elem := r.first
	for {
		if token < elem.node.token {
			return elem.prev
		}
		if elem == r.last {
			return r.last
		}
		elem = elem.next
	}
}

func (r *ring) rewind() {
	r.pos = nil
}

func (r *ring) nthElem(n int) *element {
	if r.length == 0 || r.length < n {
		return nil
	}
	defer r.rewind()
	c := 0
	elem := r.next()
	for c < n {
		elem = r.next()
		c++
	}
	return elem
}

func NewRing(nodes ...*node) *ring {
	r := &ring{pos: nil, elemsByToken: make(map[partition.Token]*element)}
	for _, n := range nodes {
		r.push(n)
	}
	return r
}
