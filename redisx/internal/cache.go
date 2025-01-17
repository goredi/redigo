package internal

import (
	"container/list"
	"errors"
	"time"
)

// Cache implements yet-another not-so-effecient probably-O(1) in-memory lazy-expirable lru cache
// Why? Because dependency hell is real.
type Cache struct {
	maxSize int

	ll      *list.List
	entries map[string]entry

	hits   int
	misses int
}

type entry struct {
	value   interface{}
	ref     *list.Element
	expires time.Time
}

var errPositiveMaxSize = errors.New("redisx cacher: cache max size must be positive")

// NewCache returns initialized cache
func NewCache(maxSize int) *Cache {
	if maxSize < 1 {
		panic(errPositiveMaxSize)
	}

	return &Cache{
		maxSize: maxSize,
		ll:      list.New(),
		entries: make(map[string]entry),
	}
}

// Set does exactly what you would expect. TTL is specified in seconds
func (c *Cache) Set(key string, value interface{}, ttl int) {
	ref := c.ll.PushFront(key)
	expires := time.Now().Add(time.Second * time.Duration(ttl))

	c.entries[key] = entry{
		value:   value,
		ref:     ref,
		expires: expires,
	}
}

// Get does exactly what you would expect
func (c *Cache) Get(key string) interface{} {
	if entry, hit := c.entries[key]; hit {
		if time.Now().After(entry.expires) {
			c.Delete(key)
			return nil
		}

		c.ll.MoveToFront(entry.ref)
		c.hits++
		return entry.value
	}

	c.misses++
	return nil
}

// Delete deletes a key from cache
func (c *Cache) Delete(key string) {
	if entry, ok := c.entries[key]; ok {
		c.ll.Remove(entry.ref)
		delete(c.entries, key)
	}
}

// Stats returns stats about local cache
func (c *Cache) Stats() (size int, hits int, misses int) {
	size = len(c.entries)
	hits = c.hits
	misses = c.misses
	return
}

// Purge deletes all expired keys and trim cache to match
// Must be called manually
func (c *Cache) Purge() {
	size := 0
	for e := c.ll.Front(); e != nil; e = e.Next() {
		key := e.Value.(string)
		entry := c.entries[key]

		if time.Now().After(entry.expires) {
			c.ll.Remove(entry.ref)
			delete(c.entries, key)
			continue
		}

		size++
	}

	if size > c.maxSize {
		e := c.ll.Back()
		for i := size - c.maxSize; i > 0; i-- {
			delete(c.entries, e.Value.(string))
			c.ll.Remove(e)
			e = e.Prev()
		}
	}
}

// Clear removes all keys from cache
func (c *Cache) Clear() {
	c.entries = make(map[string]entry)
	c.ll = list.New()
}
