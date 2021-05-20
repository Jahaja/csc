package csc

import (
	"errors"
	"log"
	"os"
	"sync/atomic"
	"time"

	"github.com/gomodule/redigo/redis"
)

const cacheInProgressSentinel = "__csc:cip__"

// in milliseconds
const defaultExpireCheckInterval = 3000

var debugMode = os.Getenv("__CSC_DEBUG") != ""
var debugLogger = log.New(os.Stdout, "csc-debug ", log.Ldate|log.Lmicroseconds)

func dlog(format string, v ...interface{}) {
	if debugMode {
		debugLogger.Printf(format, v...)
	}
}

var Logger = log.New(os.Stdout, "csc ", log.Ldate|log.Lmicroseconds)

var ErrClosed = errors.New("client is closed")

// this is used so it can be overridden in testing
var nowFunc = time.Now

type Client struct {
	pool   Pool
	conn   redis.Conn
	closed uint32
	cache  *cache
	iconn  redis.Conn
}

type Entry struct {
	Data    []byte
	Expires time.Time
	Hit     bool
}

func (c *Client) getEntry(key string) (Entry, error) {
	var empty Entry

	if c.isClosed() {
		return empty, ErrClosed
	}

	key = c.prefixKey(key)
	dlog("client.get: %p k=%s\n", c, key)

	if ce, ok := c.cache.getEntry(key); ok && ce.data != nil && string(ce.data) != cacheInProgressSentinel {
		return Entry{
			Data:    ce.data,
			Expires: ce.expires,
			Hit:     true,
		}, nil
	}

	cleanup := func() {
		c.cache.delete(key)
		c.conn.Do("DEL", key)
	}

	c.cache.set(key, []byte(cacheInProgressSentinel), 30)

	rpl, err := c.conn.Do("GET", key)
	if err != nil {
		cleanup()
		return empty, err
	}

	data, err := redis.Bytes(rpl, err)
	if err != nil {
		cleanup()
		return empty, err
	}

	expire, err := redis.Int(c.conn.Do("TTL", key))
	if err != nil {
		cleanup()
		return empty, err
	}

	// only set to cache if we see the sentinel, if not, the key has been invalidated during processing
	if string(c.cache.get(key)) == cacheInProgressSentinel {
		c.cache.set(key, data, expire)
	}

	return Entry{
		Data:    data,
		Expires: nowFunc().Add(time.Second * time.Duration(expire)),
		Hit:     false,
	}, nil
}

func (c *Client) GetEntry(key string) (Entry, error) {
	return c.getEntry(key)
}

func (c *Client) Get(key string) ([]byte, error) {
	e, err := c.getEntry(key)
	if err != nil {
		return nil, err
	}

	return e.Data, err
}

// todo(jhamren): if perf needs it, implement this properly with MGET
func (c *Client) GetMulti(keys []string) ([][]byte, error) {
	var results [][]byte

	for _, k := range keys {
		k = c.prefixKey(k)
		d, _ := c.Get(k)
		results = append(results, d)
	}

	return results, nil
}

func (c *Client) Set(key string, value []byte, expires int) error {
	if c.isClosed() {
		return ErrClosed
	}

	key = c.prefixKey(key)
	dlog("client.set: %p k=%s v=%s\n", c, key, value)

	if _, err := c.conn.Do("SETEX", key, expires, value); err != nil {
		return err
	}

	return nil
}

func (c *Client) Delete(keys ...string) error {
	if c.isClosed() {
		return ErrClosed
	}

	if c.pool.Options().KeyPrefix != "" {
		var ks = make([]string, 0, len(keys))
		for _, k := range keys {
			ks = append(ks, c.prefixKey(k))
		}

		keys = ks
	}

	dlog("client.delete: %p k=%s\n", c, keys)
	if _, err := c.conn.Do("DEL", redis.Args{}.AddFlat(keys)...); err != nil {
		return err
	}

	c.cache.delete(keys...)
	return nil
}

func (c *Client) Flush() {
	c.cache.flush()
}

func (c *Client) Stats() Stats {
	return c.cache.stats()
}

func (c *Client) setClosed() {
	atomic.StoreUint32(&c.closed, 1)
}

func (c *Client) isClosed() bool {
	return atomic.LoadUint32(&c.closed) == 1
}

func (c *Client) Conn() redis.Conn {
	return c.conn
}

func (c *Client) Close() error {
	// if the client is closed/done, close the connections, otherwise put it back into the pool
	if c.isClosed() {
		if err := c.conn.Close(); err != nil {
			return err
		}

		if err := c.iconn.Close(); err != nil {
			return err
		}

		return nil
	}

	c.pool.put(c)
	return nil
}

func (c *Client) prefixKey(k string) string {
	opts := c.pool.Options()
	if opts.KeyPrefix != "" {
		return opts.KeyPrefix + k
	}

	return k
}
