package csc

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestCache_evict(t *testing.T) {
	c := newCache(10)
	for i := 0; i < 100; i++ {
		c.set(fmt.Sprintf("key:%d", i), []byte("fooobar"), 60)
	}

	if c.evictions == 0 {
		t.Fatalf("evictions: %d", c.evictions)
	}

	c2 := newCache(100)
	for i := 0; i < 101; i++ {
		c2.set(fmt.Sprintf("key:%d", i), []byte("fooobar"), 60)
	}

	if int(c2.evictions) != c2.evictSize() {
		t.Fatalf("evictions: %d, evict size: %d", c2.evictions, c2.evictSize())
	}
}

func TestCache_expired(t *testing.T) {
	c := newCache(10)

	pastNowFunc := func() time.Time {
		return time.Now().Add(-time.Hour)
	}

	nowFunc = pastNowFunc
	for i := 0; i < 10; i++ {
		c.set(fmt.Sprintf("key:%d", i), []byte("fooobar"), 60)
	}

	nowFunc = time.Now
	c.evictExpired()

	if c.expired != 10 {
		t.Fatalf("expired: %d", c.expired)
	}
}

func TestCache_getset(t *testing.T) {
	c := newCache(1000)

	key := "somekey"
	value := "value123"
	bvalue := []byte(value)

	c.set(key, bvalue, 3600)
	d := c.get(key)
	if string(d) != value {
		t.FailNow()
	}

	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("key:%d", i)
		c.set(k, bvalue, 3600)
		d := c.get(k)
		if string(d) != value {
			t.FailNow()
		}
	}

	if c.stats().NumEntries != 101 {
		t.FailNow()
	}

	wg := sync.WaitGroup{}
	for i := 100; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			c.set(fmt.Sprintf("key:%d", i), bvalue, 3600)
		}(i)
	}
	wg.Wait()

	if c.stats().NumEntries != 201 {
		t.FailNow()
	}
}

func TestCache_flush(t *testing.T) {
	c := newCache(100)

	value := "value123"
	bvalue := []byte(value)

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key:%d", i)
		c.set(key, bvalue, 3600)
		d := c.get(key)
		if string(d) != value {
			t.FailNow()
		}
	}

	if c.stats().NumEntries != 100 {
		t.FailNow()
	}

	c.flush()

	if c.stats().NumEntries != 0 {
		t.FailNow()
	}
}

func TestCache_delete(t *testing.T) {
	c := newCache(100)

	value := "value123"
	bvalue := []byte(value)

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key:%d", i)
		c.set(key, bvalue, 3600)
		d := c.get(key)
		if string(d) != value {
			t.FailNow()
		}
	}

	if c.stats().NumEntries != 100 {
		t.FailNow()
	}

	c.delete("key:0", "key:1", "key:2")

	if c.stats().NumEntries != 97 {
		t.FailNow()
	}
}
