package connectionpool

import (
	"fmt"
	"net"
	"sync"
)

type ConnectionPool struct {
	connector Connector
	pool      map[string]net.Conn
	poolMutex sync.RWMutex
}

func (c *ConnectionPool) GetClient(address, identifier string) (net.Conn, error) {
	key := fmt.Sprintf("%s_%s", address, identifier)
	c.poolMutex.RLock()
	conn, ok := c.pool[key]
	c.poolMutex.RUnlock()
	if ok {
		return conn, nil
	}
	connection, err := c.connector.Connect(address)
	if err != nil {
		return nil, err
	}
	c.poolMutex.Lock()
	defer c.poolMutex.Unlock()
	c.pool[key] = connection
	return connection, nil
}

func (c *ConnectionPool) Invalidate(address string) {
	delete(c.pool, address)
}

func NewConnectionPool(connector Connector) *ConnectionPool {
	return &ConnectionPool{connector: connector, pool: make(map[string]net.Conn)}
}
