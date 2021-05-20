package csc

import (
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const NoExpire = 0

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

	c := &cache{
		maxEntries: maxEntries,
		entries:    make(map[string]cacheEntry, initialCacheSize),
	}

	dlog("cache.new: %p n=%d\n", c, maxEntries)
	return c
}

func (c *cache) delete(keys ...string) {
	dlog("cache.delete: %p k=%s\n", c, keys)

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

	dlog("cache.evict: %p st=%d si=%d l=%ds\n", c, start, size, len(c.entries))

	i := 0
	for k := range c.entries {
		if i < start {
			i++
			continue
		}

		dlog("cache.evict: %p k=%s\n", c, k)

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
	dlog("cache.set: %p k=%s v=%s ex=%d\n", c, key, value, expires)

	c.Lock()
	defer c.Unlock()

	num := len(c.entries)
	if num >= c.maxEntries {
		c.evictKeys()
	}

	ce := cacheEntry{
		data: value,
	}

	if expires > NoExpire {
		ce.expires = nowFunc().Add(time.Second * time.Duration(expires))
	}

	c.entries[key] = ce
}

func (c *cache) getEntry(key string) (cacheEntry, bool) {
	c.Lock()
	ce, ok := c.entries[key]
	c.Unlock()

	if ok {
		dlog("cache.get.hit: %p k=%s\n", c, key)

		atomic.AddUint64(&c.hits, 1)
		return ce, ok
	}

	dlog("cache.get.miss: %p k=%s\n", c, key)
	atomic.AddUint64(&c.misses, 1)

	return cacheEntry{}, ok
}

func (c *cache) get(key string) []byte {
	ce, _ := c.getEntry(key)
	return ce.data
}

func (c *cache) getm(keys ...string) [][]byte {
	var results [][]byte

	c.Lock()
	defer c.Unlock()

	for _, k := range keys {
		e, ok := c.entries[k]
		if ok {
			atomic.AddUint64(&c.hits, 1)
			dlog("cache.getm.hit: %p k=%s\n", c, keys)
		} else {
			atomic.AddUint64(&c.misses, 1)
			dlog("cache.getm.miss: %p k=%s\n", c, keys)
		}

		results = append(results, e.data)
	}

	return results
}

func (c *cache) evictExpired() {
	var keys []string

	now := nowFunc()
	c.Lock()
	for k, entry := range c.entries {
		if !entry.expires.IsZero() && entry.expires.Before(now) {
			keys = append(keys, k)
		}
	}
	c.Unlock()

	if len(keys) > 0 {
		dlog("cache.expire: %p k=%s\n", c, keys)

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

func (c *cache) flush() {
	c.Lock()
	defer c.Unlock()

	dlog("cache.flush: %p\n", c)

	c.hits = 0
	c.misses = 0
	c.expired = 0
	c.evictions = 0
	c.entries = map[string]cacheEntry{}
}
