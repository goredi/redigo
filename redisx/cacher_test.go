package redisx

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
)

func TestCacherTrackingStrategy(t *testing.T) {
	c, cleanup := setupCacher(t, dialGetter, Tracking)
	defer cleanup()

	matcher := MatcherFunc(func(key string) bool {
		return key != "nocache"
	})

	cached := hasBeenCachedHelper(t, c)

	assert.True(t, cached(matcher, "key"), "key should be cached")
	assert.False(t, cached(matcher, "nocache"), "key shouldn't be cached because it didn't Match")

	assert.True(t, cached(nil, "anything"), "all keys should be cached when nil matcher is supplied")
}

func TestCacherBroadcastStrategy(t *testing.T) {
	c, cleanup := setupCacher(t, dialGetter, Broadcast)
	defer cleanup()

	cached := hasBeenCachedHelper(t, c)

	t.Run("should panic when Matcher is not prefixMatcher", func(t *testing.T) {
		assert.PanicsWithValue(t, errPrefixMatcherExpected, func() {
			cached(MatcherFunc(func(_ string) bool { return true }), "key")
		})
		assert.PanicsWithValue(t, errPrefixMatcherExpected, func() {
			cached(nil, "key")
		})
	})

	matcher := NewPrefixMatcher([]string{"user:", "object:"})

	assert.True(t, cached(matcher, "user:123"), "key should be in cache")
	assert.True(t, cached(matcher, "object:"), "key should be in cache")

	assert.False(t, cached(matcher, "admin:blah"), "key without needed prefix shouldn't be in cache")
}

func TestCacherWithoutInvalidationProcess(t *testing.T) {
	c := &Cacher{
		Getter: ConnGetterFunc(func() redis.Conn { return nil }),
	}

	assert.PanicsWithValue(t, errNotRunning, func() {
		c.Get(nil)
	})

	assert.PanicsWithValue(t, errNotRunning, func() {
		c.Wrap(c.Get(nil), nil)
	})
}

func TestCacherNilGetter(t *testing.T) {
	c := &Cacher{
		Getter: nil,
	}

	assert.PanicsWithValue(t, errNilGetter, func() {
		c.Run(context.Background(), make(chan<- struct{}))
	})

	assert.PanicsWithValue(t, errNilGetter, func() {
		c.Get(nil)
	})
}

func TestCacherNoConnection(t *testing.T) {
	calls := 0
	getter := ConnGetterFunc(func() redis.Conn {
		if calls == 0 {
			calls++
			return dialGetter()
		}
		return errorConn{errors.New("sorry no network")}
	})

	pingerInterval = time.Millisecond

	c, cleanup := setupCacher(t, getter, Tracking)
	defer cleanup()

	conn := c.Wrap(dialGetter(), nil)

	// Cache some keys
	_, err := conn.Do("SET", "key", "value")
	assert.Nil(t, err)
	_, err = conn.Do("GET", "key")
	assert.Nil(t, err)

	_, err = conn.Do("SET", "foo", "bar")
	assert.Nil(t, err)
	_, err = conn.Do("GET", "foo")
	assert.Nil(t, err)

	assert.Equal(t, 2, c.Stats().Entries)

	time.Sleep(time.Millisecond * 4)

	assert.Equal(t, 0, c.Stats().Entries)
}

func TestCacherTTL(t *testing.T) {
	c, cleanup := setupCacher(t, dialGetter, Tracking)
	defer cleanup()

	conn := c.Get(nil)

	// Cache some keys
	_, err := conn.Do("SET", "foo", "value", "EX", 1)
	assert.Nil(t, err)
	_, err = conn.Do("GET", "foo")
	assert.Nil(t, err)

	_, err = conn.Do("SET", "bar", "value")
	assert.Nil(t, err)
	_, err = conn.Do("GET", "bar")
	assert.Nil(t, err)

	assert.Equal(t, 2, c.Stats().Entries)
	time.Sleep(time.Millisecond * 1100)

	// Cache will actually get invalidated by redis notification rather that
	// local purge. Nevertheless Redis docs suggest to store key's TTL locally
	// redis.io/topics/client-side-caching#other-hints-about-client-libraries-implementation
	//
	// Also related: https://github.com/redis/redis/issues/6833
	assert.Equal(t, 1, c.Stats().Entries)
}

var dialGetter = ConnGetterFunc(func() redis.Conn {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	conn, err := redis.DialContext(ctx, "tcp", ":6379")
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to redis: %v", err))
	}

	return conn
})

func setupCacher(t *testing.T, getter ConnGetter, strategy cachingStrategy) (c *Cacher, cleanup func()) {
	c = &Cacher{
		Getter:   getter,
		Strategy: strategy,
		MaxSize:  9999,
	}

	ctx, cleanup := context.WithCancel(context.Background())

	setupDone := make(chan struct{})
	go func() {
		t.Log("Starting invalidator")
		err := c.Run(ctx, setupDone)
		if err != context.Canceled {
			t.Errorf("unexpected error: %v", err)
		}
	}()
	<-setupDone

	return
}

func hasBeenCachedHelper(t *testing.T, c *Cacher) func(Matcher, string) bool {
	return func(m Matcher, key string) bool {
		conn := c.Get(m)
		defer conn.Close()

		beforeCount := c.Stats().Entries

		_, err := conn.Do("SET", key, "value")
		assert.Nil(t, err)

		_, err = conn.Do("GET", key)
		assert.Nil(t, err)

		// Key should be cached by now
		time.Sleep(10 * time.Millisecond)
		afterCount := c.Stats().Entries

		// Make sure stored value is correct
		value, err := redis.String(conn.Do("GET", key))
		assert.Equal(t, "value", value)
		assert.Nil(t, err)

		// Invalidate cache
		_, err = conn.Do("SET", key, "another value")
		assert.Nil(t, err)

		time.Sleep(10 * time.Millisecond)
		assert.Equal(t, beforeCount, c.Stats().Entries)

		return afterCount == beforeCount+1
	}
}

func TestPrefixMatcher(t *testing.T) {
	m := NewPrefixMatcher([]string{"user:", "object:"})
	assert.True(t, m.Match("user:123"))
	assert.True(t, m.Match("user:"))
	assert.True(t, m.Match("object:blah"))
	assert.False(t, m.Match("admin:666"))
}
