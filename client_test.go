package csc

import (
	"fmt"
	"testing"
	"time"
)

func TestClient(t *testing.T) {
	key := "foo"

	pool := NewTrackingPool(PoolOptions{RedisAddress: ":6379", MaxEntries: 10000})
	c, err := pool.Get()
	if err != nil {
		t.Fatalf("failed to get client from pool: %v", err)
	}

	_, err = c.Get(key)

	c2, err := pool.Get()
	if err != nil {
		t.Fatalf("failed to get client from pool: %v", err)
	}

	if err := c2.Set(key, []byte("123"), 60); err != nil {
		t.Fatalf("failed to set: %v", err)
	}

	if err := c.Delete(key); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	time.Sleep(time.Millisecond * 100)
}

func TestClient_Set(t *testing.T) {
	key := "foo"
	value := "123456"

	pool := NewTrackingPool(PoolOptions{RedisAddress: ":6379", MaxEntries: 10000})
	c1, err := pool.Get()
	if err != nil {
		t.Fatalf("failed to get client from pool: %v", err)
	}

	if err := c1.Set(key, []byte(value), 60); err != nil {
		t.Fatalf("failed to set: %v", err)
	}

	c2, err := pool.Get()
	if err != nil {
		t.Fatalf("failed to get client from pool: %v", err)
	}

	res, err := c2.Get(key)
	if err != nil {
		t.Fatalf("failed to get: %v", err)
	}

	if string(res) != value {
		t.FailNow()
	}

	stats := c2.Stats()
	if stats.NumEntries != 1 {
		t.FailNow()
	}

	if stats.Misses != 1 {
		t.FailNow()
	}

	hitsPre := c2.Stats().Hits
	if _, err := c2.Get(key); err != nil {
		t.Fatalf("failed to get: %v", err)
	}

	if int(c2.Stats().Hits-hitsPre) != 1 {
		t.Fatalf("stats.Hits: %d", stats.Hits)
	}

	if err := c1.Delete(key); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	time.Sleep(time.Millisecond * 100)
}

func TestBroadcastingClient(t *testing.T) {
	key := "foo"
	value := "123456"

	pool, _ := NewDefaultBroadcastingPool(PoolOptions{MaxEntries: 100, RedisAddress: ":6379"})

	time.Sleep(time.Millisecond * 100)

	_, err := pool.Get()
	if err != nil {
		t.FailNow()
	}
	c1, err := pool.Get()
	if err != nil {
		t.Fatalf("failed to get client from pool: %v", err)
	}

	if err := c1.Set(key, []byte(value), 60); err != nil {
		t.Fatalf("failed to set: %v", err)
	}

	c2, err := pool.Get()
	if err != nil {
		t.Fatalf("failed to get client from pool: %v", err)
	}

	res, err := c2.Get(key)
	if err != nil {
		t.Fatalf("failed to get: %v", err)
	}

	if string(res) != value {
		t.FailNow()
	}

	stats := c2.Stats()
	if stats.NumEntries != 1 {
		t.FailNow()
	}

	if stats.Misses != 1 {
		t.FailNow()
	}

	hitsPre := c2.Stats().Hits
	if _, err := c2.Get(key); err != nil {
		t.Fatalf("failed to get: %v", err)
	}

	if int(c2.Stats().Hits-hitsPre) != 1 {
		t.Fatalf("stats.Hits: %d", stats.Hits)
	}

	if err := c1.Delete(key); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	time.Sleep(time.Millisecond * 100)
}

func TestBroadcastingClient_prefix(t *testing.T) {
	key := "foo"
	value := "123456"

	pool, _ := NewDefaultBroadcastingPool(PoolOptions{KeyPrefix: "__csc:", MaxEntries: 100, RedisAddress: ":6379"})

	time.Sleep(time.Millisecond * 100)

	_, err := pool.Get()
	if err != nil {
		t.FailNow()
	}
	c1, err := pool.Get()
	if err != nil {
		t.Fatalf("failed to get client from pool: %v", err)
	}

	if err := c1.Set(key, []byte(value), 60); err != nil {
		t.Fatalf("failed to set: %v", err)
	}

	c2, err := pool.Get()
	if err != nil {
		t.Fatalf("failed to get client from pool: %v", err)
	}

	res, err := c2.Get(key)
	if err != nil {
		t.Fatalf("failed to get: %v", err)
	}

	if string(res) != value {
		t.FailNow()
	}

	stats := c2.Stats()
	if stats.NumEntries != 1 {
		t.FailNow()
	}

	if stats.Misses != 1 {
		t.FailNow()
	}

	hitsPre := c2.Stats().Hits
	if _, err := c2.Get(key); err != nil {
		t.Fatalf("failed to get: %v", err)
	}

	if int(c2.Stats().Hits-hitsPre) != 1 {
		t.Fatalf("stats.Hits: %d", stats.Hits)
	}

	if err := c1.Delete(key); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	time.Sleep(time.Millisecond * 100)
}

func TestBroadcastingClient_GetEntries(t *testing.T) {
	key := "foo"
	value := "123456"

	pool, _ := NewDefaultBroadcastingPool(PoolOptions{MaxEntries: 100, RedisAddress: ":6379"})

	time.Sleep(time.Millisecond * 100)

	c, err := pool.Get()
	if err != nil {
		t.Fatalf("failed to get client from pool: %v", err)
	}

	keys := []string{}
	for i := 0; i < 10; i++ {
		k := fmt.Sprintf("%s-%d", key, i)
		if err := c.Set(k, []byte(value), 60); err != nil {
			t.Fatalf("failed to set: %v", err)
		}

		keys = append(keys, k)
	}

	// add a few that doesn't exist
	keys = append(keys, "bar1")
	keys = append(keys, "bar2")
	keys = append(keys, "bar3")

	entries, err := c.GetEntries(keys)
	if len(entries) != 13 {
		t.FailNow()
	}

	if !entries[10].Miss() {
		t.FailNow()
	}

	if !entries[11].Miss() {
		t.FailNow()
	}

	if !entries[12].Miss() {
		t.FailNow()
	}
}
