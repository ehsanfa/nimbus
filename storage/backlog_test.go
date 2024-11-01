package storage

import (
	"sync"
	"testing"
	"time"
)

func TestPrepare(t *testing.T) {
	s := newBacklog()
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			s.promise(Promise(time.Now().UnixMilli()), "key")
			wg.Done()
		}()
	}
	wg.Wait()
}

func BenchmarkPrepare(b *testing.B) {
	s := newBacklog()
	for n := 0; n <= b.N; n++ {
		err := s.promise(Promise(time.Now().UnixMilli()), "key")
		if err != nil {
			b.Error(err)
		}
	}
}

func TestPrepareAndAccept(t *testing.T) {
	s := newBacklog()
	var wg sync.WaitGroup
	for range 100 {
		promise := Promise(time.Now().UnixMilli())
		wg.Add(1)
		go func() {
			s.promise(promise, "key")
			s.accept(promise, "key", []byte("value"))
			wg.Done()
		}()
	}
	wg.Wait()
}

func BenchmarkAccept(b *testing.B) {
	s := newBacklog()
	promise := Promise(time.Now().UnixMilli())
	err := s.promise(promise, "key")
	if err != nil {
		b.Error(err)
	}
	for n := 0; n <= b.N; n++ {
		err = s.accept(promise, "key", []byte("value"))
		if err != nil {
			b.Error(err)
		}
	}
}
