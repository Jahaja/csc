package csc

import (
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type Stats struct {
	Hits       uint64 `json:"hits"`
	Misses     uint64 `json:"misses"`
	Evictions  uint64 `json:"evictions"`
	Expired    uint64 `json:"expired"`
	NumEntries int    `json:"num_entries"`
}

type cacheEntry struct {
	data    []byte
	expires time.Time
}

type cache struct {
	sync.Mutex
	maxEntries int
	hits       uint64
	misses     uint64
	evictions  uint64
	expired    uint64
	entries    map[string]cacheEntry
}

const initialCacheSize = 128
const evictSizeFactor = 0.05

func newCache(maxEntries int) *cache {
	if maxEntries <= 0 {
		panic("max entries must not be 0")
	}

	return &cache{
		maxEntries: maxEntries,
		entries:    make(map[string]cacheEntry, initialCacheSize),
	}
}

func (c *cache) delete(keys ...string) {
	if debugMode {
		debugLogger.Printf("cache.delete: %p k=%s\n", c, keys)
	}

	c.Lock()
	defer c.Unlock()
	for _, k := range keys {
		delete(c.entries, k)
	}
}

// lock is held
func (c *cache) evictSize() int {
	s := int(math.Ceil(evictSizeFactor * float64(len(c.entries))))
	if s == 0 {
		s = 1
	}

	return s
}

// crude eviction if the cache is full, deletes a percentage of the keys from a random offset
// lock is held
// todo(jhamren): make this smarter, like LRU
func (c *cache) evictKeys() {
	size := c.evictSize()

	rand.Seed(nowFunc().UnixNano())
	start := rand.Intn(len(c.entries) - size)

	if debugMode {
		debugLogger.Printf("cache.evict: %p st=%d si=%d l=%ds\n", c, start, size, len(c.entries))
	}

	i := 0
	for k := range c.entries {
		if i < start {
			i++
			continue
		}

		if debugMode {
			debugLogger.Printf("cache.evict: %p k=%s\n", c, k)
		}

		delete(c.entries, k)
		c.evictions++

		size--
		if size == 0 {
			break
		}

		i++
	}
}

func (c *cache) set(key string, value []byte, expires int) {
	if debugMode {
		debugLogger.Printf("cache.set: %p k=%s v=%s ex=%d\n", c, key, value, expires)
	}

	c.Lock()
	defer c.Unlock()

	num := len(c.entries)
	if num >= c.maxEntries {
		c.evictKeys()
	}

	c.entries[key] = cacheEntry{
		data:    value,
		expires: nowFunc().Add(time.Second * time.Duration(expires)),
	}
}

func (c *cache) get(key string) []byte {
	if debugMode {
		debugLogger.Printf("cache.get: %p k=%s\n", c, key)
	}

	c.Lock()
	ce, ok := c.entries[key]
	c.Unlock()

	if ok {
		atomic.AddUint64(&c.hits, 1)
		return ce.data
	}

	atomic.AddUint64(&c.misses, 1)
	return nil
}

func (c *cache) getm(keys ...string) [][]byte {
	if debugMode {
		debugLogger.Printf("cache.getm: %p k=%s\n", c, keys)
	}

	var results [][]byte

	c.Lock()
	defer c.Unlock()

	for _, k := range keys {
		e, _ := c.entries[k]
		results = append(results, e.data)
	}

	return results
}

func (c *cache) evictExpired() {
	var keys []string

	now := nowFunc()
	c.Lock()
	for k, entry := range c.entries {
		if entry.expires.Before(now) {
			keys = append(keys, k)
		}
	}
	c.Unlock()

	if len(keys) > 0 {
		if debugMode {
			debugLogger.Printf("client.expire: %p k=%s\n", c, keys)
		}

		atomic.AddUint64(&c.expired, uint64(len(keys)))
		c.delete(keys...)
	}
}

func (c *cache) stats() Stats {
	c.Lock()
	num := len(c.entries)
	c.Unlock()

	return Stats{
		Hits:       atomic.LoadUint64(&c.hits),
		Misses:     atomic.LoadUint64(&c.misses),
		Expired:    atomic.LoadUint64(&c.expired),
		Evictions:  atomic.LoadUint64(&c.evictions),
		NumEntries: num,
	}
}
