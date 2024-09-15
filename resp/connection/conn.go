package connection

import (
	"myredis/lib/sync/wait"
	"net"
	"sync"
	"time"
)

// Connection represents a connection with a redis-cli
type Connection struct {
	conn net.Conn

	// lock while handler sending response
	mu sync.Mutex

	// waiting until reply finished
	waitingReply wait.Wait

	// selected db
	selectedDB int
}

// RemoteAddr returns the remote network address
func (c *Connection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// NewConn creates Connection instance
func NewConn(conn net.Conn) *Connection {
	return &Connection{
		conn: conn,
	}
}

func (c *Connection) Write(b []byte) error {
	if len(b) == 0 {
		return nil
	}
	c.mu.Lock()
	c.waitingReply.Add(1)
	defer func() {
		c.waitingReply.Done()
		c.mu.Unlock()
	}()

	_, err := c.conn.Write(b)
	return err
}

func (c *Connection) GetDBIndex() int {
	return c.selectedDB
}

func (c *Connection) SelectDB(dbNum int) {
	c.selectedDB = dbNum
}

// Close disconnect with the client
func (c *Connection) Close() error {
	c.waitingReply.WaitWithTimeout(10 * time.Second)
	_ = c.conn.Close()
	return nil
}
