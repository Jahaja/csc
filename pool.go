package csc

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/gomodule/redigo/redis"
)

type ClientPool struct {
	MaxActive       int
	MaxIdle         int
	Wait            bool
	MaxCacheEntries int

	rpool       *redis.Pool
	mu          sync.Mutex
	numOpen     uint32
	freeClients []*Client
}

func NewClientPool(rpool *redis.Pool, maxCacheEntries int) *ClientPool {
	return &ClientPool{
		MaxCacheEntries: maxCacheEntries,
		rpool:           rpool,
	}
}

// todo(jhamren): other configuration options
// NewDefaultClientPool create a new client pool with default configuration
func NewDefaultClientPool(addr string) *ClientPool {
	rpool := &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", addr) },
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}

	return NewClientPool(rpool, 10000)
}

// getFree returns nil if there is no free client
func (p *ClientPool) getFree() *Client {
	p.mu.Lock()
	defer p.mu.Unlock()

	numFree := len(p.freeClients)
	if numFree > 0 {
		c := p.freeClients[0]
		copy(p.freeClients, p.freeClients[1:])
		p.freeClients = p.freeClients[:numFree-1]
		return c
	}

	return nil
}

func (p *ClientPool) putFree(c *Client) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.freeClients = append(p.freeClients, c)
}

func (p *ClientPool) GetClient() (*Client, error) {
	c := p.getFree()
	if c != nil {
		return c, nil
	}

	c = &Client{
		pool:  p,
		dconn: p.rpool.Get(),
		iconn: p.rpool.Get(),
		cache: newCache(p.MaxCacheEntries),
	}

	cid, err := redis.Int(c.iconn.Do("CLIENT", "ID"))
	if err != nil {
		return nil, err
	}

	if _, err := c.dconn.Do("CLIENT", "TRACKING", "ON", "REDIRECT", cid, "NOLOOP"); err != nil {
		return nil, err
	}

	go c.invalidationsReceiver()
	go c.expireWatcher()

	atomic.AddUint32(&p.numOpen, 1)
	return c, nil
}
