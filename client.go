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
var Logger = log.New(os.Stdout, "csc ", log.Ldate|log.Lmicroseconds)

var ErrClosed = errors.New("client is closed")

// this is used so it can be overridden in testing
var nowFunc = time.Now

type client struct {
	conn   redis.Conn
	cache  *cache
	closed uint32
}

type TrackingClient struct {
	client

	pool *TrackingPool
	// invalidation connection
	iconn redis.Conn
}

type BroadcastingClient struct {
	client
}

func (c *client) Get(key string) ([]byte, error) {
	if debugMode {
		debugLogger.Printf("client.get: %p k=%s\n", c, key)
	}

	if c.isClosed() {
		return nil, ErrClosed
	}

	if data := c.cache.get(key); data != nil {
		return data, nil
	}

	c.cache.set(key, []byte(cacheInProgressSentinel), 30)

	rpl, err := c.conn.Do("GET", key)
	if err != nil {
		c.cache.delete(key)
		return nil, err
	}

	data, err := redis.Bytes(rpl, err)
	if err != nil {
		c.cache.delete(key)
		return nil, err
	}

	expire, err := redis.Int(c.conn.Do("TTL", key))
	if err != nil {
		c.cache.delete(key)
		return nil, err
	}

	// only set to cache if we see the sentinel, if not, the key has been invalidated during processing
	if string(c.cache.get(key)) == cacheInProgressSentinel {
		c.cache.set(key, data, expire)
	}

	return data, nil
}

// todo(jhamren): if perf needs it, implement this properly with MGET
func (c *client) GetMulti(keys []string) ([][]byte, error) {
	var results [][]byte

	for _, k := range keys {
		d, err := c.Get(k)
		if err != nil {
			return nil, err
		}

		results = append(results, d)
	}

	return results, nil
}

func (c *client) Set(key string, value []byte, expires int) error {
	if debugMode {
		debugLogger.Printf("client.set: %p k=%s v=%s\n", c, key, value)
	}

	if c.isClosed() {
		return ErrClosed
	}

	if _, err := c.conn.Do("SETEX", key, expires, value); err != nil {
		return err
	}

	// set in cache using a Get call which turns on tracking for the key
	_, err := c.Get(key)
	return err
}

func (c *client) Delete(keys ...string) error {
	if debugMode {
		debugLogger.Printf("client.delete: %p k=%s\n", c, keys)
	}

	if c.isClosed() {
		return ErrClosed
	}

	if _, err := c.conn.Do("DEL", redis.Args{}.AddFlat(keys)...); err != nil {
		return err
	}

	c.cache.delete(keys...)
	return nil
}

func (c *client) Stats() Stats {
	return c.cache.stats()
}

func (c *client) setClosed() {
	atomic.StoreUint32(&c.closed, 1)
}

func (c *client) isClosed() bool {
	return atomic.LoadUint32(&c.closed) == 1
}

func (c *client) Close() error {
	c.setClosed()
	return nil
}

func (c *client) Conn() redis.Conn {
	return c.conn
}

func (c *TrackingClient) Close() error {
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

	c.pool.putFree(c)
	return nil
}

func (c *BroadcastingClient) Close() error {
	c.client.Close()
	return c.conn.Close()
}
