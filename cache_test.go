package csc

import (
	"fmt"
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
