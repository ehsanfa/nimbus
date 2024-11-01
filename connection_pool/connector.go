package connectionpool

import (
	"fmt"
	"net"
)

type Connector interface {
	Connect(address string) (net.Conn, error)
}

type TcpConnector struct {
}

func (t TcpConnector) Connect(address string) (net.Conn, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	fmt.Println("dialing node to connect", address)
	return conn, err
}

func NewTcpConnector() TcpConnector {
	return TcpConnector{}
}

type MockConnector struct {
	conn net.Conn
}

func (m MockConnector) Connect(address string) (net.Conn, error) {
	return m.conn, nil
}

func NewMockConnector(conn net.Conn) MockConnector {
	return MockConnector{conn}
}
