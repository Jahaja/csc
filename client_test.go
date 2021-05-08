package csc

import (
	"testing"
	"time"
)

func TestClient(t *testing.T) {
	key := "foo"

	pool := NewDefaultClientPool(":6379")
	c, err := pool.GetClient()
	if err != nil {
		t.Fatalf("failed to get client from pool: %v", err)
	}

	_, err = c.Get(key)

	c2, err := pool.GetClient()
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

	pool := NewDefaultClientPool(":6379")
	c1, err := pool.GetClient()
	if err != nil {
		t.Fatalf("failed to get client from pool: %v", err)
	}

	if err := c1.Set(key, []byte(value), 60); err != nil {
		t.Fatalf("failed to set: %v", err)
	}

	c2, err := pool.GetClient()
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

	if c2.Stats().NumEntries != 1 {
		t.FailNow()
	}

	if c2.Stats().Misses != 1 {
		t.FailNow()
	}

	if _, err := c2.Get(key); err != nil {
		t.Fatalf("failed to get: %v", err)
	}

	if c2.Stats().Hits != 1 {
		t.FailNow()
	}

	if err := c1.Delete(key); err != nil {
		t.Fatalf("failed to delete: %v", err)
	}

	time.Sleep(time.Millisecond * 100)
}
