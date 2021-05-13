package csc

import (
	"context"
	"time"

	"github.com/gomodule/redigo/redis"
)

type cacher interface {
	getCache() *cache
}

func invalidationsReceiver(conn redis.Conn, c cacher) error {
	if _, err := conn.Do("SUBSCRIBE", "__redis__:invalidate"); err != nil {
		return err
	}

	fails := 0
	for {
		if fails >= 5 {
			return ErrClosed
		}

		reply, err := conn.Receive()
		if err != nil {
			fails++
			Logger.Println("failed to receive from subscription")
			continue
		}

		if s, ok := reply.(string); ok && s == "RESET" {
			return nil
		}

		values, err := redis.Values(reply, err)
		if err != nil {
			Logger.Println("failed to parse invalidation reply")
			continue
		}

		replyType, _ := redis.String(values[0], nil)
		if replyType != "message" {
			Logger.Println("subscription reply is not a message")
			continue
		}

		// values[1] is channel name, skip for now
		keys, err := redis.Strings(values[2], nil)
		if err != nil && err != redis.ErrNil {
			Logger.Println("failed to parse subscription reply keys:", err.Error())
			continue
		}

		if debugMode {
			debugLogger.Printf("client.invalidating: %p k=%s\n", c, keys)
		}

		c.getCache().delete(keys...)
	}
}

func expireWatcher(ctx context.Context, c cacher) {
	ticker := time.NewTicker(time.Millisecond * defaultExpireCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.getCache().evictExpired()
		}
	}
}
