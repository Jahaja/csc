package csc

import (
	"sync"
	"time"
)

type cacheEntry struct {
	data    []byte
	expires time.Time
}

type cache struct {
	sync.RWMutex
	maxEntries int
	entries    map[string]cacheEntry
}

const initialCacheSize = 128
const deleteOnFullFactor = 0.05

func newCache(maxEntries int) *cache {
	return &cache{
		maxEntries: maxEntries,
		entries:    make(map[string]cacheEntry, initialCacheSize),
	}
}

func (c *cache) delete(keys ...string) {
	if debugMode {
		debugLogger.Printf("cache.delete: %p, %s\n", c, keys)
	}

	c.Lock()
	defer c.Unlock()
	for _, k := range keys {
		delete(c.entries, k)
	}
}

func (c *cache) set(key string, data []byte, expires int) {
	if debugMode {
		debugLogger.Printf("cache.set: %p, k: %s, d: %s, ex: %d\n", c, key, data, expires)
	}

	c.Lock()
	defer c.Unlock()

	// crude deletion if the cache is full, deletes a percentage of the keys in iteration order
	// todo(jhamren): make this smarter, like LRU
	num := len(c.entries)
	if num >= c.maxEntries {
		i := int(deleteOnFullFactor * float32(num))
		if i == 0 {
			i = 1
		}

		for k := range c.entries {
			Logger.Printf("evicting cache key: %p, %s\n", c, k)
			delete(c.entries, k)

			i--
			if i == 0 {
				break
			}
		}
	}

	c.entries[key] = cacheEntry{
		data:    data,
		expires: time.Now().Add(time.Second * time.Duration(expires)),
	}
}

func (c *cache) get(key string) []byte {
	if debugMode {
		debugLogger.Printf("cache.get: %p, %s\n", c, key)
	}

	c.RLock()
	defer c.RUnlock()

	ce, ok := c.entries[key]
	if ok {
		return ce.data
	}

	return nil
}

func (c *cache) getm(keys ...string) [][]byte {
	if debugMode {
		debugLogger.Printf("cache.getm: %p, %s\n", c, keys)
	}

	var results [][]byte

	c.RLock()
	defer c.RUnlock()

	for _, k := range keys {
		e, _ := c.entries[k]
		results = append(results, e.data)
	}

	return results
}
