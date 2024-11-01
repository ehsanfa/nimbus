package storage

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestCommit(t *testing.T) {
	ctx := context.Background()
	ds := NewDataStore(ctx)
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			p := Promise(time.Now().UnixMicro())
			ds.Promise(p, "key")
			ds.Accept(p, "key", []byte("value"))
			ds.Commit("key", p)
		}()
	}
}

func BenchmarkCommit(b *testing.B) {
	ctx := context.Background()
	ds := NewDataStore(ctx)
	p := Promise(time.Now().UnixMicro())
	ds.Promise(p, "key")
	ds.Accept(p, "key", []byte("value"))
	for n := 0; n < b.N; n++ {
		ds.Commit("key", p)
	}
}

func TestGet(t *testing.T) {
	ctx := context.Background()
	ds := NewDataStore(ctx)
	var wg sync.WaitGroup
	p := Promise(time.Now().UnixMicro())
	ds.Promise(p, "key")
	ds.Accept(p, "key", []byte("value"))
	ds.Commit("key", p)
	for range 100 {
		wg.Add(1)
		go func() {
			ds.Get("key")
		}()
	}
}

func BenchmarkGet(b *testing.B) {
	ctx := context.Background()
	ds := NewDataStore(ctx)
	p := Promise(time.Now().UnixMicro())
	ds.Promise(p, "key")
	ds.Accept(p, "key", []byte("value"))
	ds.Commit("key", p)
	for n := 0; n < b.N; n++ {
		ds.Get("key")
	}
}
