package main

import "fmt"

type element struct {
	prev *element
	node
	next *element
}

type ring struct {
	length int
	first  *element
	last   *element
	pos    *element
}

func (r *ring) incrLength() {
	r.length += 1
}

func (r *ring) push(node node) {
	elem := &element{node: node}
	defer r.incrLength()

	if r.length == 0 {
		elem.next = elem
		elem.prev = elem
		r.first = elem
		r.last = elem
		return
	}

	elem.next = r.first
	elem.prev = r.last
	r.last.next = elem
	r.last = elem
}

func (r *ring) next() node {
	if r.pos == nil {
		r.pos = r.first.prev
	}
	r.pos = r.pos.next
	return r.pos.node
}

func (r *ring) show() {
	i := 0
	for i < 10 {
		l := r.next()
		fmt.Println(l)
		i += 1
	}
}

func NewRing() *ring {
	return &ring{pos: nil}
}
