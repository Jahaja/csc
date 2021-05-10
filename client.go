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

var ErrClientFailed = errors.New("client has failed")

// this is used so it can be overridden in testing
var nowFunc = time.Now

type Client struct {
	pool *ClientPool
	// data connection
	dconn redis.Conn
	// invalidation connection
	iconn redis.Conn
	cache *cache
	// used as bool with atomic store/load
	failed int32
}

type ClientStats struct {
	Hits       uint64 `json:"hits"`
	Misses     uint64 `json:"misses"`
	Evictions  uint64 `json:"evictions"`
	Expired    uint64 `json:"expired"`
	NumEntries int    `json:"num_entries"`
}

func (c *Client) Get(key string) ([]byte, error) {
	if debugMode {
		debugLogger.Printf("client.get: %p k=%s\n", c, key)
	}

	if c.hasFailed() {
		return nil, ErrClientFailed
	}

	if data := c.cache.get(key); data != nil {
		return data, nil
	}

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

	// only set to cache if we see the sentinel, if not, the key has been invalidated during processing
	if string(c.cache.get(key)) == cacheInProgressSentinel {
		c.cache.set(key, data, expire)
	}

	return data, nil
}

// todo(jhamren): if perf needs it, implement this properly with MGET
func (c *Client) GetMulti(keys []string) ([][]byte, error) {
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

func (c *Client) Set(key string, value []byte, expires int) error {
	if debugMode {
		debugLogger.Printf("client.set: %p k=%s v=%s\n", c, key, value)
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
		debugLogger.Printf("client.delete: %p k=%s\n", c, key)
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
	c.cache.Lock()
	num := len(c.cache.entries)
	c.cache.Unlock()

	return ClientStats{
		Hits:       atomic.LoadUint64(&c.cache.hits),
		Misses:     atomic.LoadUint64(&c.cache.misses),
		Expired:    atomic.LoadUint64(&c.cache.expired),
		Evictions:  atomic.LoadUint64(&c.cache.evictions),
		NumEntries: num,
	}
}

func (c *Client) markFailed() {
	atomic.StoreInt32(&c.failed, 1)
}

func (c *Client) reset() error {
	if err := c.dconn.Close(); err != nil {
		return err
	}

	if err := c.iconn.Close(); err != nil {
		return err
	}

	return nil
}

func (c *Client) Close() error {
	if c.hasFailed() {
		return c.reset()
	}

	c.pool.putFree(c)
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
		if fails >= 5 {
			Logger.Println("invalidation receive failed after 5 retries")
			c.markFailed()
			return
		}

		reply, err := c.iconn.Receive()
		if err != nil {
			fails++
			Logger.Println("failed to receive from subscription")
			continue
		}

		if s, ok := reply.(string); ok && s == "RESET" {
			return
		}

		values, err := redis.Values(reply, err)
		if err != nil {
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
			debugLogger.Printf("client.invalidating: %p k=%s\n", c, keys)
		}

		c.cache.delete(keys...)
	}
}

func (c *Client) expireWatcher() {
	ticker := time.NewTicker(time.Millisecond * defaultExpireCheckInterval)
	defer ticker.Stop()

	for !c.hasFailed() {
		select {
		// todo(jhamren): cancel context case
		case <-ticker.C:
			c.cache.evictExpired()
		}
	}
}
