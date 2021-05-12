package csc

import (
	"testing"
	"time"
)

func TestClientPool(t *testing.T) {
	pool := NewClientPool(ClientPoolOptions{RedisAddress: ":6379", Wait: true, MaxActive: 1, MaxEntries: 100})
	c1, err := pool.Get()
	if err != nil {
		t.FailNow()
	}

	go func() {
		time.Sleep(time.Millisecond * 100)
		c1.Close()
	}()

	c2, err := pool.Get()
	if err != nil {
		t.FailNow()
	}

	go func() {
		time.Sleep(time.Millisecond * 100)
		c2.Close()
	}()

	_, err = pool.Get()
	if err != nil {
		t.FailNow()
	}

	if pool.waited != 2 {
		t.Fatal("waited is not 2")
	}
}

func TestBroadcastPool(t *testing.T) {
	pool := NewBroadcastClientPool(ClientPoolOptions{MaxEntries: 1000, RedisAddress: ":6379"})
	pool.Start()

	time.Sleep(time.Millisecond * 100)

	_, err := pool.Get()
	if err != nil {
		t.FailNow()
	}
}
