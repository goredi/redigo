package internal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCache(t *testing.T) {
	c := NewCache(100)
	c.Set("foo", "bar", 1)
	c.Set("baz", "qux", 1)

	c.Purge()

	assert.Equal(t, "bar", c.Get("foo"))
	assert.Equal(t, "qux", c.Get("baz"))
	assert.Equal(t, nil, c.Get("foobar"))

	size, hits, misses := c.Stats()
	assert.Equal(t, 2, size)
	assert.Equal(t, 2, hits)
	assert.Equal(t, 1, misses)

	time.Sleep(time.Second)
	c.Purge()

	assert.Nil(t, c.Get("foo"))
	assert.Nil(t, c.Get("qux"))
}

func TestCacheTrim(t *testing.T) {
	c := NewCache(2)
	c.Set("k1", "v1", 999)
	c.Set("k2", "v2", 999)
	c.Set("k3", "v3", 999)

	// Move key up on eviction list
	c.Get("k3")

	c.Purge()

	assert.NotNil(t, c.Get("k3"))
	assert.NotNil(t, c.Get("k2"))
	assert.Nil(t, c.Get("k1"))

	c.Clear()
	size, _, _ := c.Stats()
	assert.Equal(t, 0, size)
}

func TestCacheMaxSize(t *testing.T) {
	assert.PanicsWithValue(t, errPositiveMaxSize, func() {
		NewCache(0)
	})

	assert.PanicsWithValue(t, errPositiveMaxSize, func() {
		NewCache(-1)
	})
}
