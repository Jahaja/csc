package csc

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gomodule/redigo/redis"
)

var ErrTooManyActiveClients = errors.New("too many active clients")

type PoolOptions struct {
	RedisAddress  string
	RedisDatabase int
	MaxActive     int
	MaxIdle       int
	IdleTimeout   time.Duration
	Wait          bool
	MaxEntries    int
}

type TrackingPool struct {
	options PoolOptions
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

func NewTrackingPool(opts PoolOptions) *TrackingPool {
	p := &TrackingPool{
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

func (p *TrackingPool) Get() (*Client, error) {
	// grab a slot, there're MaxActive slots available when waiting
	if p.options.Wait && p.options.MaxActive > 0 {
		// assume that we're waiting if there's no slot
		waiting := len(p.ch) == 0
		var start time.Time
		if waiting {
			atomic.AddUint64(&p.waited, 1)
			if debugMode {
				start = nowFunc()
				debugLogger.Printf("tpool.waiting: %p", p)
			}
		}

		select {
		case <-p.ch:
		}

		if debugMode && waiting {
			debugLogger.Printf("tclient.waited: %p, t=%dus", p, time.Since(start).Microseconds())
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
		conn:  dconn,
		cache: newCache(p.options.MaxEntries),
		iconn: iconn,
	}

	cid, err := redis.Int(c.iconn.Do("CLIENT", "ID"))
	if err != nil {
		return nil, err
	}

	if _, err := c.conn.Do("CLIENT", "TRACKING", "ON", "REDIRECT", cid, "NOLOOP"); err != nil {
		return nil, err
	}

	go func() {
		if err := invalidationsReceiver(c.iconn, c.cache); err != nil {
			c.setClosed()
		}
	}()
	go expireWatcher(context.Background(), c.cache)

	atomic.AddUint32(&p.active, 1)
	return c, nil
}

// closes connections of all clients in the free list
func (p *TrackingPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if debugMode {
		debugLogger.Printf("tpool.close: %p\n", p)
	}

	for _, c := range p.free {
		c.setClosed()
		c.Close()
	}

	return nil
}

// getFree returns nil if there is no free client
func (p *TrackingPool) getFree() *Client {
	p.mu.Lock()
	defer p.mu.Unlock()

	numFree := len(p.free)
	if debugMode {
		debugLogger.Printf("tpool.getfree: %p n=%d\n", p, numFree)
	}

	if numFree > 0 {
		c := p.free[0]
		copy(p.free, p.free[1:])
		p.free = p.free[:numFree-1]
		return c
	}

	return nil
}

func (p *TrackingPool) putFree(c *Client) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if debugMode {
		debugLogger.Printf("tpool.putfree: %p n=%d\n", p, len(p.free))
	}

	p.free = append(p.free, c)

	// notify that a slot has become available
	if p.ch != nil {
		p.ch <- struct{}{}
	}
}

type BroadcastingPool struct {
	rpool     *redis.Pool
	iconn     redis.Conn
	conn      redis.Conn
	cache     *cache
	outOfSync uint32
	closed    uint32
}

// creates a new broadcasting pool and starts the background jobs
func NewBroadcastingPool(rpool *redis.Pool, opts PoolOptions) (*BroadcastingPool, error) {
	p := &BroadcastingPool{
		rpool: rpool,
		cache: newCache(opts.MaxEntries),
	}

	if err := p.setupConnections(); err != nil {
		return nil, err
	}

	go expireWatcher(context.Background(), p.cache)
	go func() {
		for !p.isClosed() {
			time.Sleep(time.Second)
			if p.isOutOfSync() {
				if debugMode {
					debugLogger.Printf("bpool.conn.outofsync: %p\n", p)
				}

				p.conn.Close()
				p.iconn.Close()
				p.cache.flush()

				if err := p.setupConnections(); err != nil {
					Logger.Println("failed to reopen broadcasting pool connections:", err.Error())
					continue
				}

				p.setOutofSync(false)
			}
		}
	}()

	return p, nil
}

func (p *BroadcastingPool) setupConnections() error {
	if debugMode {
		debugLogger.Printf("bpool.conn.setup: %p\n", p)
	}

	p.conn = p.rpool.Get()
	p.iconn = p.rpool.Get()

	cid, err := redis.Int(p.iconn.Do("CLIENT", "ID"))
	if err != nil {
		return err
	}

	if debugMode {
		debugLogger.Printf("bpool.conn.iconn: %p cid=%d\n", p, cid)
	}

	if _, err := p.conn.Do("CLIENT", "TRACKING", "ON", "REDIRECT", cid, "BCAST"); err != nil {
		return err
	}

	// ping the redirecting data conn periodically as a healthcheck
	go func(conn redis.Conn) {
		ticker := time.NewTicker(time.Second * 5)
		fails := 0
		for !p.isClosed() {
			if fails >= 5 {
				p.setOutofSync(true)
				Logger.Println("broadcasting data connection failed, connections out-of-sync")
				return
			}

			select {
			case <-ticker.C:
				_, err := conn.Do("PING")
				if err != nil {
					fails++
					if debugMode {
						debugLogger.Printf("bpool.conn.pingfail: %p err=%s\n", p, err.Error())
					}
					continue
				}

				fails = 0
			}
		}
	}(p.conn)

	go func() {
		if err := invalidationsReceiver(p.iconn, p.cache); err != nil {
			Logger.Println("invalidation data connection failed, connections out-of-sync")
			p.setOutofSync(true)
		}
	}()

	return nil
}

func NewDefaultBroadcastingPool(opts PoolOptions) (*BroadcastingPool, error) {
	return NewBroadcastingPool(
		&redis.Pool{
			Dial: func() (redis.Conn, error) {
				return redis.Dial("tcp", opts.RedisAddress, redis.DialDatabase(opts.RedisDatabase))
			},
			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				if time.Since(t) < time.Second {
					return nil
				}

				_, err := c.Do("PING")
				return err
			},
		},
		opts,
	)
}

func (p *BroadcastingPool) Get() (*Client, error) {
	c := &Client{
		conn:  p.rpool.Get(),
		cache: p.cache,
	}

	return c, nil
}

func (p *BroadcastingPool) Close() error {
	if debugMode {
		debugLogger.Printf("bpool.close: %p\n", p)
	}

	atomic.StoreUint32(&p.closed, 1)
	p.conn.Close()
	p.iconn.Close()
	p.cache.flush()
	return p.rpool.Close()
}

func (p *BroadcastingPool) isOutOfSync() bool {
	return atomic.LoadUint32(&p.outOfSync) == 1
}

func (p *BroadcastingPool) isClosed() bool {
	return atomic.LoadUint32(&p.closed) == 1
}

func (p *BroadcastingPool) setOutofSync(b bool) {
	if b {
		atomic.StoreUint32(&p.outOfSync, 1)
	} else {
		atomic.StoreUint32(&p.outOfSync, 0)
	}
}

func (p *BroadcastingPool) Stats() Stats {
	return p.cache.stats()
}

func (p *BroadcastingPool) Flush() {
	p.cache.flush()
}
