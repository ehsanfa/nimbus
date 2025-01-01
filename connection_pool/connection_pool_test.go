package connectionpool

import (
	"fmt"
	"net"
	"sync"
	"testing"
)

func TestGetClient(t *testing.T) {
	c, _ := net.Pipe()
	cp := NewConnectionPool(NewMockConnector(c))
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func() {
			cp.GetClient(fmt.Sprintf("address_%d", i), "test")
			wg.Done()
		}()
	}
	wg.Wait()
	for i := range 100 {
		wg.Add(1)
		go func() {
			cp.GetClient(fmt.Sprintf("address_%d", i), "test")
			wg.Done()
		}()
	}
	wg.Wait()
}

func BenchmarkGetClientNonExisting(b *testing.B) {
	c, _ := net.Pipe()
	cp := NewConnectionPool(NewMockConnector(c))
	for n := 0; n < b.N; n++ {
		cp.GetClient(fmt.Sprint(n), "test")
	}
}
