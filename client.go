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
	Data     []byte
	Expires  time.Time
	LocalHit bool
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
			Data:     ce.data,
			Expires:  ce.expires,
			LocalHit: true,
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
		Data:     data,
		Expires:  nowFunc().Add(time.Second * time.Duration(expire)),
		LocalHit: false,
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

func (c *Client) GetEntries(keys []string) ([]Entry, error) {
	dlog("client.getentries: %p k=%s\n", c, keys)
	entries := make([]Entry, len(keys))

	// fetch all that's in local cache and keep track of the missing ones
	idxMap := make(map[string]int)
	missing := make([]string, 0, len(keys))
	results := c.cache.getm(keys...)
	for i, ce := range results {
		if ce.data == nil {
			k := keys[i]
			idxMap[k] = i
			missing = append(missing, k)
			continue
		}

		entries[i] = Entry{
			Data:     ce.data,
			Expires:  ce.expires,
			LocalHit: true,
		}
	}

	dlog("client.getentries.missing: %p k=%s\n", c, missing)

	// fetch the missing keys in a single MGET
	for _, k := range missing {
		c.cache.set(k, []byte(cacheInProgressSentinel), 30)
	}

	cleanup := func() {
		c.cache.delete(missing...)
		c.conn.Do("DEL", redis.Args{}.AddFlat(missing)...)
	}

	rpl, err := c.conn.Do("MGET", redis.Args{}.AddFlat(missing)...)
	if err != nil {
		cleanup()
		return nil, err
	}

	mres, err := redis.ByteSlices(rpl, err)
	if err != nil {
		cleanup()
		return nil, err
	}

	c.conn.Send("MULTI")
	for _, k := range missing {
		c.conn.Send("TTL", k)
	}

	ttls, err := redis.Ints(c.conn.Do("EXEC"))
	if err != nil {
		cleanup()
		return nil, err
	}

	for i, k := range missing {
		d := mres[i]
		ttl := ttls[i]

		if d == nil {
			c.cache.delete(k)
			continue
		}

		// only set to cache if we see the sentinel, if not, the key has been invalidated during processing
		if string(c.cache.get(k)) == cacheInProgressSentinel {
			c.cache.set(k, d, ttl)
		}

		idx := idxMap[k]
		entries[idx] = Entry{
			Data:     d,
			Expires:  nowFunc().Add(time.Second * time.Duration(ttl)),
			LocalHit: false,
		}
	}

	return entries, nil
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

func (e Entry) Miss() bool {
	return e.Data == nil
}
