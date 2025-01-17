package redisx

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/gomodule/redigo/redisx/internal"
)

type cachingStrategy int

const (
	// Tracking strategy lets server keep track of keys request by client
	Tracking cachingStrategy = iota

	// Broadcast strategy allows client to manually subscribe
	// to invalidation broadcasts for interested keys
	// More efficient in terms of Redis memory & cpu usage but requires
	// client to know what keys need to be cached beforehand
	// See https://redis.io/topics/client-side-caching#broadcasting-mode
	Broadcast
)

// Cacher implements client-side caching first appeared in Redis 6.0
type Cacher struct {
	Getter   ConnGetter      // Interface used for obtaining connections
	Strategy cachingStrategy // Caching strategy (see docs)
	MaxSize  int             // Max number of keys in local cache. Must be positive

	mu    sync.Mutex      // Guard for fields below
	cid   int             // Connection ID of invalidator process
	cache *internal.Cache // Actual cache
}

// Matcher interface is used to determine whether key should be cached.
// Allows for custom caching logic.
type Matcher interface {
	Match(key string) bool
}

// MatcherFunc is an adapter type for Matcher interface
type MatcherFunc func(string) bool

// Match just calls underlying function
func (f MatcherFunc) Match(key string) bool {
	return f(key)
}

type prefixMatcher []string

// NewPrefixMatcher is a helper function that matches all keys with
// supplied prefixes
func NewPrefixMatcher(prefixes []string) Matcher {
	return prefixMatcher(prefixes)
}

func (p prefixMatcher) Match(key string) bool {
	for _, prefix := range p {
		if strings.HasPrefix(key, prefix) {
			return true
		}
	}

	return false
}

// ConnGetter is interface used to get redis connection for Cacher usage
type ConnGetter interface {
	Get() redis.Conn
}

// ConnGetterFunc is an adapter type for ConnGetter interface
type ConnGetterFunc func() redis.Conn

// Get just calls underlying function
func (f ConnGetterFunc) Get() redis.Conn {
	return f()
}

var errNilGetter = errors.New("redisx cacher: getter is nil")
var purgeInterval = time.Second

// Run starts cache invalidation process. Must be called before any other
// method. Closes provided channel when setup is done.
// Blocks caller.
func (c *Cacher) Run(ctx context.Context, setupDoneChan chan<- struct{}) error {
	if c.Getter == nil {
		panic(errNilGetter)
	}

	// Connection used for invalidation
	conn := c.Getter.Get()
	if conn == nil {
		close(setupDoneChan)
		return errors.New("getter returned nil connection")
	}

	id, err := redis.Int(conn.Do("CLIENT", "ID"))
	if err != nil {
		close(setupDoneChan)
		return err
	}

	c.mu.Lock()
	c.cid = id
	c.cache = internal.NewCache(c.MaxSize)
	c.mu.Unlock()

	// Subscribe to revocation channel
	if err = conn.Send("SUBSCRIBE", "__redis__:invalidate"); err != nil {
		close(setupDoneChan)
		return err
	}

	if err = conn.Flush(); err != nil {
		close(setupDoneChan)
		return err
	}

	pingConn := c.Getter.Get()
	if conn == nil {
		close(setupDoneChan)
		return errors.New("getter returned nil connection")
	}

	purgeTimer := time.NewTicker(purgeInterval)

	connDeadChan := make(chan struct{})
	go pinger(ctx, pingConn, connDeadChan)

	invalidateChan := make(chan []string)
	go invalidator(ctx, conn, invalidateChan)

	close(setupDoneChan)

outer:
	for {
		select {
		case <-ctx.Done():
			break outer

		case <-connDeadChan:
			// Flush local cache
			c.mu.Lock()
			c.cache.Clear()
			c.mu.Unlock()

		case <-purgeTimer.C:
			// Evict expired and infrequently used items
			c.mu.Lock()
			c.cache.Purge()
			c.mu.Unlock()

		case keys := <-invalidateChan:
			// Remove keys from local cache
			c.mu.Lock()
			for _, key := range keys {
				c.cache.Delete(key)
			}
			c.mu.Unlock()
		}
	}

	c.mu.Lock()
	c.cid = 0
	c.mu.Unlock()

	return ctx.Err()
}

// invalidator polls invalidate messages in separate goroutine since conn.Receive blocks
func invalidator(ctx context.Context, conn redis.Conn, invalidateChan chan<- []string) {
outer:
	for {
		select {
		case <-ctx.Done():
			break outer
		default:
			event, err := redis.Values(conn.Receive())
			if err != nil {
				continue outer
			}

			etype, _ := redis.String(event[0], nil)

			if etype != "message" {
				continue outer
			}

			keys, err := redis.Strings(event[2], nil)
			if err != nil || err == redis.ErrNil {
				continue outer
			}

			invalidateChan <- keys
		}
	}

	close(invalidateChan)
}

// changed in tests to avoid sleeping insane amounts of time
var pingerInterval = time.Second

// pinger will try to ping server and will send to channel on multiple failed attempts
func pinger(ctx context.Context, conn redis.Conn, connDeadChan chan<- struct{}) {
	ticker := time.NewTicker(pingerInterval)
	failures := 0

outer:
	for {
		select {
		case <-ctx.Done():
			break outer
		case <-ticker.C:
			// Check connection is still active
			if _, err := conn.Do("PING"); err == nil {
				failures = 0
				continue
			}

			failures++

			if failures == 3 {
				connDeadChan <- struct{}{}
				failures = 0
			}
		}
	}

	ticker.Stop()
	close(connDeadChan)
}

// Get returns a connection with caching enabled for matching keys.
// If nil Matcher is passed all keys are subjected to cache.
// When using Broadcast strategy Matcher should be created with NewPrefixMatcher.
// Caller is responsible for closing connection.
func (c *Cacher) Get(m Matcher) redis.Conn {
	if c.Getter == nil {
		panic(errNilGetter)
	}
	return c.Wrap(c.Getter.Get(), m)
}

var errNotRunning = errors.New("redisx cacher: invalidation process is not running")
var errPrefixMatcherExpected = errors.New("redisx cacher: NewPrefixMatcher is expected when using Broadcast strategy")

// Wrap allows wrapping existing connection with caching layer
// When using Broadcast strategy Matcher should be created with NewPrefixMatcher.
// Caller is responsible for closing connection.
func (c *Cacher) Wrap(conn redis.Conn, m Matcher) redis.Conn {
	c.mu.Lock()
	cid := c.cid
	c.mu.Unlock()

	if cid == 0 {
		panic(errNotRunning)
	}

	args := []interface{}{"TRACKING", "on", "REDIRECT", cid}

	// When using Broadcast strategy we just pass list of key prefixes
	// we are interested in.
	if c.Strategy == Broadcast {
		prefixes, ok := m.(prefixMatcher)
		if !ok {
			panic(errPrefixMatcherExpected)
		}

		args = append(args, "BCAST")
		for _, prefix := range prefixes {
			args = append(args, "PREFIX", prefix)
		}
	}

	// If matcher if supplied we want to pass OPTIN flag so we can munually
	// decide whether we want to cache certain key
	if c.Strategy == Tracking && m != nil {
		args = append(args, "OPTIN")
	}

	_, err := conn.Do("CLIENT", args...)
	if err != nil {
		return errorConn{err}
	}

	return wrappedConn{c, conn, m}
}

// Stats returns aggregated stats about local cache
func (c *Cacher) Stats() CacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()

	size, hits, misses := c.cache.Stats()
	return CacheStats{size, hits, misses}
}

// CacheStats contain statistics about local cache
type CacheStats struct {
	Entries int // Number of keys in the cache
	Hits    int // Number of cache hits
	Misses  int // Number of cache misses
}

type wrappedConn struct {
	c       *Cacher
	conn    redis.Conn
	matcher Matcher
}

func (wc wrappedConn) Close() error                            { return wc.conn.Close() }
func (wc wrappedConn) Err() error                              { return wc.conn.Err() }
func (wc wrappedConn) Send(cmd string, a ...interface{}) error { return wc.conn.Send(cmd, a...) }
func (wc wrappedConn) Flush() error                            { return wc.conn.Flush() }
func (wc wrappedConn) Receive() (reply interface{}, err error) { return wc.conn.Receive() }

// Indicates that cache is curently being populated.
// See https://redis.io/topics/client-side-caching#avoiding-race-conditions
type cachingInProgressPlaceholder struct{}

func (wc wrappedConn) Do(cmd string, args ...interface{}) (interface{}, error) {
	if strings.ToLower(cmd) != "get" || len(args) == 0 {
		return wc.conn.Do(cmd, args...)
	}

	key, ok := args[0].(string)
	if !ok {
		return wc.conn.Do(cmd, args...)
	}

	wc.c.mu.Lock()
	entry := wc.c.cache.Get(key)
	wc.c.mu.Unlock()

	if entry != nil {
		return entry, nil
	}

	return wc.exec(key, cmd, args...)
}

const defaultTTL = 600 // 10 minutes

func (wc wrappedConn) exec(key string, cmd string, args ...interface{}) (interface{}, error) {
	// 1
	if wc.shouldTrack(key) {
		if err := wc.conn.Send("CLIENT", "CACHING", "yes"); err != nil {
			return nil, err
		}
	}

	// 2
	if err := wc.conn.Send(cmd, args...); err != nil {
		return nil, err
	}

	// 3
	if wc.shouldCache(key) {
		wc.c.mu.Lock()
		wc.c.cache.Set(key, cachingInProgressPlaceholder{}, defaultTTL)
		wc.c.mu.Unlock()

		if err := wc.conn.Send("TTL", key); err != nil {
			return nil, err
		}
	}

	// Flush command buffer
	if err := wc.conn.Flush(); err != nil {
		return nil, err
	}

	// 1
	if wc.shouldTrack(key) {
		if _, err := wc.conn.Receive(); err != nil {
			return nil, err
		}
	}

	// 2
	reply, err := wc.conn.Receive()
	if err != nil {
		return reply, err
	}

	// 3
	if wc.shouldCache(key) {
		ttl, err := redis.Int(wc.conn.Receive())
		if err != nil {
			return nil, err
		}

		if ttl < 0 {
			ttl = defaultTTL
		}

		wc.c.mu.Lock()
		// avoid caching stale responses
		if _, ok := wc.c.cache.Get(key).(cachingInProgressPlaceholder); ok {
			wc.c.cache.Set(key, reply, ttl)
		}
		wc.c.mu.Unlock()
	}

	return reply, nil
}

// whether opt-in caching is enabled
func (wc wrappedConn) shouldTrack(key string) bool {
	return wc.c.Strategy == Tracking && wc.matcher != nil && wc.matcher.Match(key)
}

// whether key should be stored in local cache
func (wc wrappedConn) shouldCache(key string) bool {
	return wc.matcher == nil || (wc.matcher != nil && wc.matcher.Match(key))
}

//
type errorConn struct{ err error }

func (ec errorConn) Do(string, ...interface{}) (interface{}, error) { return nil, ec.err }
func (ec errorConn) Send(string, ...interface{}) error              { return ec.err }
func (ec errorConn) Err() error                                     { return ec.err }
func (ec errorConn) Close() error                                   { return nil }
func (ec errorConn) Flush() error                                   { return ec.err }
func (ec errorConn) Receive() (interface{}, error)                  { return nil, ec.err }
