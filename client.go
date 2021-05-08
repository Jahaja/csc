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

var debugMode = os.Getenv("__CSC_DEBUG") != ""
var debugLogger = log.New(os.Stdout, "csc-debug ", log.Ldate|log.Lmicroseconds)
var Logger = log.New(os.Stdout, "csc ", log.Ldate|log.Lmicroseconds)

var ErrClientFailed = errors.New("client has failed")

type Client struct {
	pool *ClientPool
	// data connection
	dconn redis.Conn
	// invalidation connection
	iconn redis.Conn
	cache *cache
	// used as bool with atomic store/load
	failed int32
	// stats
	hits   int64
	misses int64
}

type ClientStats struct {
	Hits       int64 `json:"hits"`
	Misses     int64 `json:"misses"`
	NumEntries int   `json:"num_entries"`
}

func (c *Client) Get(key string) ([]byte, error) {
	if debugMode {
		debugLogger.Printf("client.get: %p, %s\n", c, key)
	}

	if c.hasFailed() {
		return nil, ErrClientFailed
	}

	if data := c.cache.get(key); data != nil {
		c.hit()
		return data, nil
	}

	c.miss()
	c.cache.set(key, []byte(cacheInProgressSentinel), 30)

	rpl, err := c.dconn.Do("GET", key)
	if err != nil {
		c.cache.delete(key)
		return nil, err
	}

	data, err := redis.Bytes(rpl, err)
	if err != nil {
		c.cache.delete(key)
		return nil, err
	}

	expire, err := redis.Int(c.dconn.Do("TTL", key))
	if err != nil {
		c.cache.delete(key)
		return nil, err
	}

	if string(c.cache.get(key)) == cacheInProgressSentinel {
		c.cache.set(key, data, expire)
	}

	return data, nil
}

func (c *Client) GetMulti(keys []string) ([]byte, error) {
	return nil, nil
}

func (c *Client) Set(key string, value []byte, expires int) error {
	if debugMode {
		debugLogger.Printf("client.set: %p, %s, %s\n", c, key, value)
	}

	if c.hasFailed() {
		return ErrClientFailed
	}

	if _, err := c.dconn.Do("SETEX", key, expires, value); err != nil {
		return err
	}

	// set in cache using a Get call which turns on tracking for the key
	_, err := c.Get(key)
	return err
}

func (c *Client) Delete(key string) error {
	if debugMode {
		debugLogger.Printf("client.delete: %p, %s\n", c, key)
	}

	if c.hasFailed() {
		return ErrClientFailed
	}

	if _, err := c.dconn.Do("DEL", key); err != nil {
		return err
	}

	c.cache.delete(key)
	return nil
}

func (c *Client) Stats() ClientStats {
	c.cache.RLock()
	defer c.cache.RUnlock()

	return ClientStats{
		Hits:       atomic.LoadInt64(&c.hits),
		Misses:     atomic.LoadInt64(&c.misses),
		NumEntries: len(c.cache.entries),
	}
}

func (c *Client) hit() {
	if debugMode {
		debugLogger.Printf("client.hit: %p\n", c)
	}
	atomic.AddInt64(&c.hits, 1)
}

func (c *Client) miss() {
	if debugMode {
		debugLogger.Printf("client.miss: %p\n", c)
	}
	atomic.AddInt64(&c.misses, 1)
}

func (c *Client) markFailed() {
	atomic.StoreInt32(&c.failed, 1)
}

func (c *Client) reset() error {
	if _, err := c.dconn.Do("RESET"); err != nil {
		return err
	}

	if err := c.dconn.Close(); err != nil {
		return err
	}

	if _, err := c.iconn.Do("RESET"); err != nil {
		return err
	}

	if err := c.iconn.Close(); err != nil {
		return err
	}

	return nil
}

func (c *Client) hasFailed() bool {
	return atomic.LoadInt32(&c.failed) == 1
}

func (c *Client) invalidationsReceiver() {
	if _, err := c.iconn.Do("SUBSCRIBE", "__redis__:invalidate"); err != nil {
		Logger.Println("failed to setup invalidation subscription:", err.Error())
		return
	}

	fails := 0
	for {
		// todo(jhamren): handle failure
		if fails > 5 {
			Logger.Println("invalidation receive failed after 5 retries")
			c.markFailed()
			return
		}

		values, err := redis.Values(c.iconn.Receive())
		if err != nil {
			fails++
			Logger.Println("failed to parse invalidation reply")
			continue
		}

		replyType, _ := redis.String(values[0], nil)
		if replyType != "message" {
			Logger.Println("subscription reply is not a message")
			continue
		}

		// values[1] is channel name, skip for now
		keys, err := redis.Strings(values[2], nil)
		if err != nil && err != redis.ErrNil {
			Logger.Println("failed to parse subscription reply keys:", err.Error())
			continue
		}

		if debugMode {
			debugLogger.Printf("client.invalidating: %p, %s\n", c, keys)
		}

		c.cache.delete(keys...)
	}
}

func (c *Client) expireWatcher() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for !c.hasFailed() {
		select {
		// todo(jhamren): cancel context case
		case <-ticker.C:
			var keys []string

			now := time.Now()
			c.cache.RLock()
			for k, entry := range c.cache.entries {
				if !entry.expires.After(now) {
					keys = append(keys, k)
				}
			}
			c.cache.RUnlock()

			if len(keys) > 0 {
				for _, k := range keys {
					c.cache.delete(k)
				}
			}
		}
	}
}
