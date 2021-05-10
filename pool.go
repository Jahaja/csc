package csc

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gomodule/redigo/redis"
)

var ErrTooManyActiveClients = errors.New("too many active clients")

type ClientPoolOptions struct {
	RedisAddress  string
	RedisDatabase int
	MaxActive     int
	Wait          bool
	MaxEntries    int
}

type ClientPool struct {
	options ClientPoolOptions
	// number of active clients, used or in the free list
	active uint32
	// number of times a Get had to wait to receive a connection
	waited uint64
	// buffered channel representing available slots when there's a max active clients limit
	ch chan struct{}
	mu sync.Mutex
	// available clients to reuse
	free []*Client
}

func NewClientPool(opts ClientPoolOptions) *ClientPool {
	p := &ClientPool{
		options: opts,
	}

	if p.options.Wait && p.options.MaxActive > 0 {
		// setup slots
		p.ch = make(chan struct{}, p.options.MaxActive)
		for i := 0; i < p.options.MaxActive; i++ {
			p.ch <- struct{}{}
		}
	}

	return p
}

func (p *ClientPool) Get() (*Client, error) {
	// grab a slot, there're MaxActive slots available when waiting
	if p.options.Wait && p.options.MaxActive > 0 {
		// assume that we're waiting if there's no slot
		waiting := len(p.ch) == 0
		var start time.Time
		if waiting {
			atomic.AddUint64(&p.waited, 1)
			if debugMode {
				start = nowFunc()
				debugLogger.Printf("pool.waiting: %p", p)
			}
		}

		select {
		case <-p.ch:
		}

		if debugMode && waiting {
			debugLogger.Printf("client.waited: %p, t=%dus", p, time.Since(start).Microseconds())
		}
	} else if p.options.MaxActive > 0 && int(atomic.LoadUint32(&p.active)) >= p.options.MaxActive {
		return nil, ErrTooManyActiveClients
	}

	c := p.getFree()
	if c != nil {
		return c, nil
	}

	dconn, err := redis.Dial("tcp", p.options.RedisAddress, redis.DialDatabase(p.options.RedisDatabase))
	if err != nil {
		return nil, err
	}

	iconn, err := redis.Dial("tcp", p.options.RedisAddress, redis.DialDatabase(p.options.RedisDatabase))
	if err != nil {
		return nil, err
	}

	c = &Client{
		pool:  p,
		dconn: dconn,
		iconn: iconn,
		cache: newCache(p.options.MaxEntries),
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

	atomic.AddUint32(&p.active, 1)
	return c, nil
}

// closes connections of all clients in the free list
func (p *ClientPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, c := range p.free {
		c.reset()
	}

	return nil
}

// getFree returns nil if there is no free client
func (p *ClientPool) getFree() *Client {
	p.mu.Lock()
	defer p.mu.Unlock()
	numFree := len(p.free)
	if numFree > 0 {
		c := p.free[0]
		copy(p.free, p.free[1:])
		p.free = p.free[:numFree-1]
		return c
	}

	return nil
}

func (p *ClientPool) putFree(c *Client) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.free = append(p.free, c)

	// notify that a slot has become available
	if p.ch != nil {
		p.ch <- struct{}{}
	}
}
