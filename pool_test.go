package csc

import (
	"testing"
	"time"
)

func TestClientPool(t *testing.T) {
	pool := NewTrackingPool(PoolOptions{RedisAddress: ":6379", Wait: true, MaxActive: 1, MaxEntries: 100})
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
	pool, err := NewDefaultBroadcastingPool(PoolOptions{MaxEntries: 1000, RedisAddress: ":6379"})
	if err != nil {
		t.FailNow()
	}

	time.Sleep(time.Millisecond * 100)

	if _, err := pool.Get(); err != nil {
		t.FailNow()
	}
}
