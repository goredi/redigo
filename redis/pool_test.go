// Copyright 2011 Gary Burd
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package redis_test

import (
	"context"
	"errors"
	"io"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/require"
)

const (
	testGoRoutines = 10
)

type poolTestConn struct {
	d   *poolDialer
	err error
	redis.Conn
}

func (c *poolTestConn) Close() error {
	c.d.mu.Lock()
	c.d.open -= 1
	c.d.mu.Unlock()
	return c.Conn.Close()
}

func (c *poolTestConn) Err() error { return c.err }

func (c *poolTestConn) Do(commandName string, args ...interface{}) (interface{}, error) {
	return c.do(c.Conn.Do, commandName, args...)
}

func (c *poolTestConn) DoContext(ctx context.Context, commandName string, args ...interface{}) (interface{}, error) {
	cwc, ok := c.Conn.(redis.ConnWithContext)
	if !ok {
		return nil, errors.New("redis: connection does not support ConnWithContext")
	}
	return c.do(
		func(c string, a ...interface{}) (interface{}, error) {
			return cwc.DoContext(ctx, c, a...)
		},
		commandName, args)
}

func (c *poolTestConn) do(
	fn func(commandName string, args ...interface{}) (interface{}, error),
	commandName string, args ...interface{},
) (interface{}, error) {
	if commandName == "ERR" {
		c.err = args[0].(error)
		commandName = "PING"
	}
	if commandName != "" {
		c.d.commands = append(c.d.commands, commandName)
	}
	return fn(commandName, args...)
}

func (c *poolTestConn) Send(commandName string, args ...interface{}) error {
	c.d.commands = append(c.d.commands, commandName)
	return c.Conn.Send(commandName, args...)
}

func (c *poolTestConn) ReceiveContext(ctx context.Context) (reply interface{}, err error) {
	cwc, ok := c.Conn.(redis.ConnWithContext)
	if !ok {
		return nil, errors.New("redis: connection does not support ConnWithContext")
	}
	return cwc.ReceiveContext(ctx)
}

type poolDialer struct {
	mu       sync.Mutex
	t        *testing.T
	dialed   int
	open     int
	commands []string
	dialErr  error
}

func (d *poolDialer) dial() (redis.Conn, error) {
	return d.dialContext(context.Background())
}

func (d *poolDialer) dialContext(ctx context.Context) (redis.Conn, error) {
	d.mu.Lock()
	d.dialed += 1
	dialErr := d.dialErr
	d.mu.Unlock()
	if dialErr != nil {
		return nil, d.dialErr
	}
	c, err := redis.DialDefaultServerContext(ctx)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	d.open += 1
	d.mu.Unlock()
	return &poolTestConn{d: d, Conn: c}, nil
}

func (d *poolDialer) check(message string, p *redis.Pool, dialed, open, inuse int) {
	d.t.Helper()
	d.checkAll(message, p, dialed, open, inuse, 0, 0)
}

func (d *poolDialer) checkAll(message string, p *redis.Pool, dialed, open, inuse int, waitCountMax int64, waitDurationMax time.Duration) {
	d.t.Helper()

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.dialed != dialed {
		d.t.Errorf("%s: dialed=%d, want %d", message, d.dialed, dialed)
	}
	if d.open != open {
		d.t.Errorf("%s: open=%d, want %d", message, d.open, open)
	}

	stats := p.Stats()

	if stats.ActiveCount != open {
		d.t.Errorf("%s: active=%d, want %d", message, stats.ActiveCount, open)
	}
	if stats.IdleCount != open-inuse {
		d.t.Errorf("%s: idle=%d, want %d", message, stats.IdleCount, open-inuse)
	}

	if stats.WaitCount > waitCountMax {
		d.t.Errorf("%s: unexpected wait=%d want at most %d", message, stats.WaitCount, waitCountMax)
	}

	if waitCountMax == 0 {
		if stats.WaitDuration != 0 {
			d.t.Errorf("%s: unexpected waitDuration=%v want %v", message, stats.WaitDuration, 0)
		}
		return
	}

	if stats.WaitDuration > waitDurationMax {
		d.t.Errorf("%s: unexpected waitDuration=%v want < %v", message, stats.WaitDuration, waitDurationMax)
	}
}

func TestPoolReuse(t *testing.T) {
	d := poolDialer{t: t}
	p := &redis.Pool{
		MaxIdle: 2,
		Dial:    d.dial,
	}

	for i := 0; i < 10; i++ {
		c1 := p.Get()
		_, err := c1.Do("PING")
		require.NoError(t, err)
		c2 := p.Get()
		_, err = c2.Do("PING")
		require.NoError(t, err)
		require.NoError(t, c1.Close())
		require.NoError(t, c2.Close())
	}

	d.check("before close", p, 2, 2, 0)
	p.Close()
	d.check("after close", p, 2, 0, 0)
}

func TestPoolMaxIdle(t *testing.T) {
	d := poolDialer{t: t}
	p := &redis.Pool{
		MaxIdle: 2,
		Dial:    d.dial,
	}
	defer p.Close()

	for i := 0; i < 10; i++ {
		c1 := p.Get()
		_, err = c1.Do("PING")
		require.NoError(t, err)
		c2 := p.Get()
		_, err = c2.Do("PING")
		require.NoError(t, err)
		c3 := p.Get()
		_, err = c3.Do("PING")
		require.NoError(t, err)
		require.NoError(t, c1.Close())
		require.NoError(t, c2.Close())
		require.NoError(t, c3.Close())
	}
	d.check("before close", p, 12, 2, 0)
	p.Close()
	d.check("after close", p, 12, 0, 0)
}

func TestPoolError(t *testing.T) {
	d := poolDialer{t: t}
	p := &redis.Pool{
		MaxIdle: 2,
		Dial:    d.dial,
	}
	defer p.Close()

	c := p.Get()
	_, err := c.Do("ERR", io.EOF)
	require.NoError(t, err)
	if c.Err() == nil {
		t.Errorf("expected c.Err() != nil")
	}
	c.Close()

	c = p.Get()
	_, err = c.Do("ERR", io.EOF)
	require.NoError(t, err)
	c.Close()

	d.check(".", p, 2, 0, 0)
}

func TestPoolClose(t *testing.T) {
	d := poolDialer{t: t}
	p := &redis.Pool{
		MaxIdle: 2,
		Dial:    d.dial,
	}
	defer p.Close()

	c1 := p.Get()
	_, err := c1.Do("PING")
	require.NoError(t, err)
	c2 := p.Get()
	_, err = c2.Do("PING")
	require.NoError(t, err)
	c3 := p.Get()
	_, err = c3.Do("PING")
	require.NoError(t, err)

	c1.Close()
	if _, err := c1.Do("PING"); err == nil {
		t.Errorf("expected error after connection closed")
	}

	c2.Close()
	c2.Close()

	p.Close()

	d.check("after pool close", p, 3, 1, 1)

	if _, err := c1.Do("PING"); err == nil {
		t.Errorf("expected error after connection and pool closed")
	}

	c3.Close()

	d.check("after conn close", p, 3, 0, 0)

	c1 = p.Get()
	if _, err := c1.Do("PING"); err == nil {
		t.Errorf("expected error after pool closed")
	}
}

func TestPoolClosedConn(t *testing.T) {
	d := poolDialer{t: t}
	p := &redis.Pool{
		MaxIdle:     2,
		IdleTimeout: 300 * time.Second,
		Dial:        d.dial,
	}
	defer p.Close()
	c := p.Get()
	if c.Err() != nil {
		t.Fatal("get failed")
	}
	c.Close()
	if err := c.Err(); err == nil {
		t.Fatal("Err on closed connection did not return error")
	}
	if _, err := c.Do("PING"); err == nil {
		t.Fatal("Do on closed connection did not return error")
	}
	if err := c.Send("PING"); err == nil {
		t.Fatal("Send on closed connection did not return error")
	}
	if err := c.Flush(); err == nil {
		t.Fatal("Flush on closed connection did not return error")
	}
	if _, err := c.Receive(); err == nil {
		t.Fatal("Receive on closed connection did not return error")
	}
}

func TestPoolIdleTimeout(t *testing.T) {
	d := poolDialer{t: t}
	p := &redis.Pool{
		MaxIdle:     2,
		IdleTimeout: 300 * time.Second,
		Dial:        d.dial,
	}
	defer p.Close()

	now := time.Now()
	redis.SetNowFunc(func() time.Time { return now })
	defer redis.SetNowFunc(time.Now)

	c := p.Get()
	_, err := c.Do("PING")
	require.NoError(t, err)
	c.Close()

	d.check("1", p, 1, 1, 0)

	now = now.Add(p.IdleTimeout + 1)

	c = p.Get()
	_, err = c.Do("PING")
	require.NoError(t, err)
	c.Close()

	d.check("2", p, 2, 1, 0)
}

func TestPoolMaxLifetime(t *testing.T) {
	d := poolDialer{t: t}
	p := &redis.Pool{
		MaxIdle:         2,
		MaxConnLifetime: 300 * time.Second,
		Dial:            d.dial,
	}
	defer p.Close()

	now := time.Now()
	redis.SetNowFunc(func() time.Time { return now })
	defer redis.SetNowFunc(time.Now)

	c := p.Get()
	_, err := c.Do("PING")
	require.NoError(t, err)
	c.Close()

	d.check("1", p, 1, 1, 0)

	now = now.Add(p.MaxConnLifetime + 1)

	c = p.Get()
	_, err = c.Do("PING")
	require.NoError(t, err)
	c.Close()

	d.check("2", p, 2, 1, 0)
}

func TestPoolConcurrenSendReceive(t *testing.T) {
	p := &redis.Pool{
		Dial: func() (redis.Conn, error) { return redis.DialDefaultServer() },
	}
	defer p.Close()

	c := p.Get()
	done := make(chan error, 1)
	go func() {
		_, err := c.Receive()
		done <- err
	}()
	require.NoError(t, c.Send("PING"))
	c.Flush()
	err := <-done
	if err != nil {
		t.Fatalf("Receive() returned error %v", err)
	}
	_, err = c.Do("")
	if err != nil {
		t.Fatalf("Do() returned error %v", err)
	}
	c.Close()
}

func TestPoolBorrowCheck(t *testing.T) {
	pingN := func(ctx context.Context, p *redis.Pool, n int) {
		for i := 0; i < n; i++ {
			func() {
				c, err := p.GetContext(ctx)
				require.NoError(t, err)
				defer func() {
					require.NoError(t, c.Close())
				}()
				_, err = redis.DoContext(c, ctx, "PING")
				require.NoError(t, err)
			}()
		}
	}

	checkLastUsedTimes := func(lastUsedTimes []time.Time, startTime, endTime time.Time, wantLen int) {
		require.Len(t, lastUsedTimes, wantLen)
		for i, lastUsed := range lastUsedTimes {
			if i == 0 {
				require.True(t, lastUsed.After(startTime))
			} else {
				require.True(t, lastUsed.After(lastUsedTimes[i-1]))
			}
			require.True(t, lastUsed.Before(endTime))
		}
	}

	t.Run("TestOnBorrow-error", func(t *testing.T) {
		d := poolDialer{t: t}
		p := &redis.Pool{
			MaxIdle:      2,
			DialContext:  d.dialContext,
			TestOnBorrow: func(redis.Conn, time.Time) error { return redis.Error("BLAH") },
		}
		defer p.Close()
		pingN(context.Background(), p, 10)
		d.check("1", p, 10, 1, 0)
	})

	t.Run("TestOnBorrow-nil-error", func(t *testing.T) {
		d := poolDialer{t: t}
		var borrowErrs []error
		var lastUsedTimes []time.Time
		p := &redis.Pool{
			MaxIdle:     2,
			DialContext: d.dialContext,
			TestOnBorrow: func(c redis.Conn, lastUsed time.Time) error {
				lastUsedTimes = append(lastUsedTimes, lastUsed)
				_, err := c.Do("PING")
				if err != nil {
					borrowErrs = append(borrowErrs, err)
				}
				return err
			},
		}
		defer p.Close()

		startTime := time.Now()
		pingN(context.Background(), p, 10)
		endTime := time.Now()

		require.Empty(t, borrowErrs)
		checkLastUsedTimes(lastUsedTimes, startTime, endTime, 9)
		d.check("1", p, 1, 1, 0)
	})

	t.Run("TestOnBorrowContext-error", func(t *testing.T) {
		d := poolDialer{t: t}
		p := &redis.Pool{
			MaxIdle:             2,
			DialContext:         d.dialContext,
			TestOnBorrowContext: func(context.Context, redis.Conn, time.Time) error { return redis.Error("BLAH") },
		}
		defer p.Close()
		pingN(context.Background(), p, 10)
		d.check("1", p, 10, 1, 0)
	})

	t.Run("TestOnBorrowContext-nil-error", func(t *testing.T) {
		d := poolDialer{t: t}
		var borrowErrs []error
		var lastUsedTimes []time.Time
		p := &redis.Pool{
			MaxIdle:     2,
			DialContext: d.dialContext,
			TestOnBorrowContext: func(ctx context.Context, c redis.Conn, lastUsed time.Time) error {
				lastUsedTimes = append(lastUsedTimes, lastUsed)
				_, err := redis.DoContext(c, ctx, "PING")
				if err != nil {
					borrowErrs = append(borrowErrs, err)
				}
				return err
			},
		}
		defer p.Close()

		startTime := time.Now()
		pingN(context.Background(), p, 10)
		endTime := time.Now()

		require.Empty(t, borrowErrs)
		checkLastUsedTimes(lastUsedTimes, startTime, endTime, 9)
		d.check("1", p, 1, 1, 0)
	})

	t.Run("TestOnBorrowContext-context.Canceled", func(t *testing.T) {
		d := poolDialer{t: t}
		var borrowErrs []error
		p := &redis.Pool{
			MaxIdle:     2,
			DialContext: d.dialContext,
			TestOnBorrowContext: func(ctx context.Context, c redis.Conn, _ time.Time) error {
				_, err := redis.DoContext(c, ctx, "PING")
				if err != nil {
					borrowErrs = append(borrowErrs, err)
				}
				return err
			},
		}
		defer p.Close()

		ctx, ctxCancel := context.WithCancel(context.Background())
		defer ctxCancel()

		pingN(ctx, p, 2)
		d.check("1", p, 1, 1, 0)
		require.Empty(t, borrowErrs)

		ctxCancel()

		_, err := p.GetContext(ctx)
		require.ErrorIs(t, err, context.Canceled)

		d.check("1", p, 2, 0, 0)
		require.Len(t, borrowErrs, 1)
		require.ErrorIs(t, borrowErrs[0], context.Canceled)
	})
}

func TestPoolMaxActive(t *testing.T) {
	d := poolDialer{t: t}
	p := &redis.Pool{
		MaxIdle:   2,
		MaxActive: 2,
		Dial:      d.dial,
	}
	defer p.Close()

	c1 := p.Get()
	_, err := c1.Do("PING")
	require.NoError(t, err)
	c2 := p.Get()
	_, err = c2.Do("PING")
	require.NoError(t, err)

	d.check("1", p, 2, 2, 2)

	c3 := p.Get()
	if _, err := c3.Do("PING"); err != redis.ErrPoolExhausted {
		t.Errorf("expected pool exhausted")
	}

	c3.Close()
	d.check("2", p, 2, 2, 2)
	c2.Close()
	d.check("3", p, 2, 2, 1)

	c3 = p.Get()
	if _, err := c3.Do("PING"); err != nil {
		t.Errorf("expected good channel, err=%v", err)
	}
	c3.Close()

	d.check("4", p, 2, 2, 1)
}

func TestPoolWaitStats(t *testing.T) {
	d := poolDialer{t: t}
	p := &redis.Pool{
		Wait:      true,
		MaxIdle:   2,
		MaxActive: 2,
		Dial:      d.dial,
	}
	defer p.Close()

	c1 := p.Get()
	_, err := c1.Do("PING")
	require.NoError(t, err)
	c2 := p.Get()
	_, err = c2.Do("PING")
	require.NoError(t, err)

	d.checkAll("1", p, 2, 2, 2, 0, 0)

	start := time.Now()
	go func() {
		time.Sleep(time.Millisecond * 100)
		c1.Close()
	}()

	c3 := p.Get()
	d.checkAll("2", p, 2, 2, 2, 1, time.Since(start))

	if _, err := c3.Do("PING"); err != nil {
		t.Errorf("expected good channel, err=%v", err)
	}
}

func TestPoolMonitorCleanup(t *testing.T) {
	d := poolDialer{t: t}
	p := &redis.Pool{
		MaxIdle:   2,
		MaxActive: 2,
		Dial:      d.dial,
	}
	defer p.Close()

	c := p.Get()
	require.NoError(t, c.Send("MONITOR"))
	c.Close()

	d.check("", p, 1, 0, 0)
}

func TestPoolPubSubCleanup(t *testing.T) {
	d := poolDialer{t: t}
	p := &redis.Pool{
		MaxIdle:   2,
		MaxActive: 2,
		Dial:      d.dial,
	}
	defer p.Close()

	c := p.Get()
	require.NoError(t, c.Send("SUBSCRIBE", "x"))
	c.Close()

	want := []string{"SUBSCRIBE", "UNSUBSCRIBE", "ECHO"}
	if !reflect.DeepEqual(d.commands, want) {
		t.Errorf("got commands %v, want %v", d.commands, want)
	}
	d.commands = nil

	c = p.Get()
	require.NoError(t, c.Send("PSUBSCRIBE", "x*"))
	c.Close()

	want = []string{"PSUBSCRIBE", "PUNSUBSCRIBE", "ECHO"}
	if !reflect.DeepEqual(d.commands, want) {
		t.Errorf("got commands %v, want %v", d.commands, want)
	}
	d.commands = nil
}

func TestPoolTransactionCleanup(t *testing.T) {
	d := poolDialer{t: t}
	p := &redis.Pool{
		MaxIdle:   2,
		MaxActive: 2,
		Dial:      d.dial,
	}
	defer p.Close()

	c := p.Get()
	_, err := c.Do("WATCH", "key")
	require.NoError(t, err)
	_, err = c.Do("PING")
	require.NoError(t, err)
	c.Close()

	want := []string{"WATCH", "PING", "UNWATCH"}
	if !reflect.DeepEqual(d.commands, want) {
		t.Errorf("got commands %v, want %v", d.commands, want)
	}
	d.commands = nil

	c = p.Get()
	_, err = c.Do("WATCH", "key")
	require.NoError(t, err)
	_, err = c.Do("UNWATCH")
	require.NoError(t, err)
	_, err = c.Do("PING")
	require.NoError(t, err)
	c.Close()

	want = []string{"WATCH", "UNWATCH", "PING"}
	if !reflect.DeepEqual(d.commands, want) {
		t.Errorf("got commands %v, want %v", d.commands, want)
	}
	d.commands = nil

	c = p.Get()
	_, err = c.Do("WATCH", "key")
	require.NoError(t, err)
	_, err = c.Do("MULTI")
	require.NoError(t, err)
	_, err = c.Do("PING")
	require.NoError(t, err)
	c.Close()

	want = []string{"WATCH", "MULTI", "PING", "DISCARD"}
	if !reflect.DeepEqual(d.commands, want) {
		t.Errorf("got commands %v, want %v", d.commands, want)
	}
	d.commands = nil

	c = p.Get()
	_, err = c.Do("WATCH", "key")
	require.NoError(t, err)
	_, err = c.Do("MULTI")
	require.NoError(t, err)
	_, err = c.Do("DISCARD")
	require.NoError(t, err)
	_, err = c.Do("PING")
	require.NoError(t, err)
	c.Close()

	want = []string{"WATCH", "MULTI", "DISCARD", "PING"}
	if !reflect.DeepEqual(d.commands, want) {
		t.Errorf("got commands %v, want %v", d.commands, want)
	}
	d.commands = nil

	c = p.Get()
	_, err = c.Do("WATCH", "key")
	require.NoError(t, err)
	_, err = c.Do("MULTI")
	require.NoError(t, err)
	_, err = c.Do("EXEC")
	require.NoError(t, err)
	_, err = c.Do("PING")
	require.NoError(t, err)
	c.Close()

	want = []string{"WATCH", "MULTI", "EXEC", "PING"}
	if !reflect.DeepEqual(d.commands, want) {
		t.Errorf("got commands %v, want %v", d.commands, want)
	}
	d.commands = nil
}

func startGoroutines(p *redis.Pool, cmd string, args ...interface{}) chan error {
	errs := make(chan error, testGoRoutines)
	for i := 0; i < cap(errs); i++ {
		go func() {
			c := p.Get()
			_, err := c.Do(cmd, args...)
			c.Close()
			errs <- err
		}()
	}

	return errs
}

func TestWaitPool(t *testing.T) {
	d := poolDialer{t: t}
	p := &redis.Pool{
		MaxIdle:   1,
		MaxActive: 1,
		Dial:      d.dial,
		Wait:      true,
	}
	defer p.Close()

	c := p.Get()
	start := time.Now()
	errs := startGoroutines(p, "PING")
	d.check("before close", p, 1, 1, 1)
	c.Close()
	timeout := time.After(2 * time.Second)
	for i := 0; i < cap(errs); i++ {
		select {
		case err := <-errs:
			if err != nil {
				t.Fatal(err)
			}
		case <-timeout:
			t.Fatalf("timeout waiting for blocked goroutine %d", i)
		}
	}
	d.checkAll("done", p, 1, 1, 0, testGoRoutines, time.Since(start)*testGoRoutines)
}

func TestWaitPoolClose(t *testing.T) {
	d := poolDialer{t: t}
	p := &redis.Pool{
		MaxIdle:   1,
		MaxActive: 1,
		Dial:      d.dial,
		Wait:      true,
	}
	defer p.Close()

	c := p.Get()
	if _, err := c.Do("PING"); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	errs := startGoroutines(p, "PING")
	d.check("before close", p, 1, 1, 1)
	p.Close()
	timeout := time.After(2 * time.Second)
	for i := 0; i < cap(errs); i++ {
		select {
		case err := <-errs:
			switch err {
			case nil:
				t.Fatal("blocked goroutine did not get error")
			case redis.ErrPoolExhausted:
				t.Fatal("blocked goroutine got pool exhausted error")
			}
		case <-timeout:
			t.Fatal("timeout waiting for blocked goroutine")
		}
	}
	c.Close()
	d.checkAll("done", p, 1, 0, 0, testGoRoutines, time.Since(start)*testGoRoutines)
}

func TestWaitPoolCommandError(t *testing.T) {
	testErr := errors.New("test")
	d := poolDialer{t: t}
	p := &redis.Pool{
		MaxIdle:   1,
		MaxActive: 1,
		Dial:      d.dial,
		Wait:      true,
	}
	defer p.Close()

	c := p.Get()
	start := time.Now()
	errs := startGoroutines(p, "ERR", testErr)
	d.check("before close", p, 1, 1, 1)
	c.Close()
	timeout := time.After(2 * time.Second)
	for i := 0; i < cap(errs); i++ {
		select {
		case err := <-errs:
			if err != nil {
				t.Fatal(err)
			}
		case <-timeout:
			t.Fatalf("timeout waiting for blocked goroutine %d", i)
		}
	}
	d.checkAll("done", p, cap(errs), 0, 0, testGoRoutines, time.Since(start)*testGoRoutines)
}

func TestWaitPoolDialError(t *testing.T) {
	testErr := errors.New("test")
	d := poolDialer{t: t}
	p := &redis.Pool{
		MaxIdle:   1,
		MaxActive: 1,
		Dial:      d.dial,
		Wait:      true,
	}
	defer p.Close()

	c := p.Get()
	start := time.Now()
	errs := startGoroutines(p, "ERR", testErr)
	d.check("before close", p, 1, 1, 1)

	d.dialErr = errors.New("dial")
	c.Close()

	nilCount := 0
	errCount := 0
	timeout := time.After(2 * time.Second)
	for i := 0; i < cap(errs); i++ {
		select {
		case err := <-errs:
			switch err {
			case nil:
				nilCount++
			case d.dialErr:
				errCount++
			default:
				t.Fatalf("expected dial error or nil, got %v", err)
			}
		case <-timeout:
			t.Fatalf("timeout waiting for blocked goroutine %d", i)
		}
	}
	if nilCount != 1 {
		t.Errorf("expected one nil error, got %d", nilCount)
	}
	if errCount != cap(errs)-1 {
		t.Errorf("expected %d dial errors, got %d", cap(errs)-1, errCount)
	}
	d.checkAll("done", p, cap(errs), 0, 0, testGoRoutines, time.Since(start)*testGoRoutines)
}

// Borrowing requires us to iterate over the idle connections, unlock the pool,
// and perform a blocking operation to check the connection still works. If
// TestOnBorrow fails, we must reacquire the lock and continue iteration. This
// test ensures that iteration will work correctly if multiple threads are
// iterating simultaneously.
func TestLocking_TestOnBorrowFails_PoolDoesntCrash(t *testing.T) {
	const count = 100

	// First we'll Create a pool where the pilfering of idle connections fails.
	d := poolDialer{t: t}
	p := &redis.Pool{
		MaxIdle:   count,
		MaxActive: count,
		Dial:      d.dial,
		TestOnBorrow: func(redis.Conn, time.Time) error {
			return errors.New("No way back into the real world.")
		},
	}
	defer p.Close()

	// Fill the pool with idle connections.
	conns := make([]redis.Conn, count)
	for i := range conns {
		conns[i] = p.Get()
	}
	for i := range conns {
		conns[i].Close()
	}

	// Spawn a bunch of goroutines to thrash the pool.
	var wg sync.WaitGroup
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func() {
			c := p.Get()
			if c.Err() != nil {
				t.Errorf("pool get failed: %v", c.Err())
			}
			c.Close()
			wg.Done()
		}()
	}
	wg.Wait()
	if d.dialed != count*2 {
		t.Errorf("Expected %d dials, got %d", count*2, d.dialed)
	}
}

func BenchmarkPoolGet(b *testing.B) {
	b.StopTimer()
	p := redis.Pool{Dial: func() (redis.Conn, error) { return redis.DialDefaultServer() }, MaxIdle: 2}
	c := p.Get()
	if err := c.Err(); err != nil {
		b.Fatal(err)
	}
	c.Close()
	defer p.Close()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		c = p.Get()
		c.Close()
	}
}

func BenchmarkPoolDo(b *testing.B) {
	p := redis.Pool{
		MaxIdle: 2,
		Dial: func() (redis.Conn, error) {
			return redis.DialDefaultServer()
		},
	}

	func() {
		// Setup required data.
		c := p.Get()
		require.NoError(b, c.Err())
		defer func() {
			require.NoError(b, c.Close())
		}()

		_, err = c.Do("MSET", "foo", "bar", "bar", "baz")
		require.NoError(b, err)
		_, err = c.Do("HMSET", "hfoo", "bar", "baz", "qux", "quux", "thing", "bob")
		require.NoError(b, err)
	}()

	tests := map[string]struct {
		cmd  string
		args []interface{}
	}{
		"set": {
			cmd:  "SET",
			args: []interface{}{"foo", "bar"},
		},
		"get": {
			cmd:  "GET",
			args: []interface{}{"foo"},
		},
		"mget": {
			cmd:  "MGET",
			args: []interface{}{"foo", "bar"},
		},
		"hmset": {
			cmd:  "HMSET",
			args: []interface{}{"hfoo", "bar", "baz", "qux", "baz1", "qux1", "stuff"},
		},
		"hgetall": {
			cmd:  "HGETALL",
			args: []interface{}{"hfoo"},
		},
		"blank": {
			cmd: "",
		},
		"ping": {
			cmd: "PING",
		},
	}
	for name, tc := range tests {
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				c := p.Get()
				_, err := c.Do(tc.cmd, tc.args...)
				require.NoError(b, err)
				err = c.Close()
				require.NoError(b, err)
			}
		})
	}
}

func BenchmarkPoolGetErr(b *testing.B) {
	b.StopTimer()
	p := redis.Pool{Dial: func() (redis.Conn, error) { return redis.DialDefaultServer() }, MaxIdle: 2}
	c := p.Get()
	if err := c.Err(); err != nil {
		b.Fatal(err)
	}
	c.Close()
	defer p.Close()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		c = p.Get()
		if err := c.Err(); err != nil {
			b.Fatal(err)
		}
		c.Close()
	}
}

func BenchmarkPoolGetPing(b *testing.B) {
	b.StopTimer()
	p := redis.Pool{Dial: func() (redis.Conn, error) { return redis.DialDefaultServer() }, MaxIdle: 2}
	c := p.Get()
	if err := c.Err(); err != nil {
		b.Fatal(err)
	}
	c.Close()
	defer p.Close()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		c = p.Get()
		if _, err := c.Do("PING"); err != nil {
			b.Fatal(err)
		}
		c.Close()
	}
}

func TestWaitPoolGetContext(t *testing.T) {
	d := poolDialer{t: t}
	p := &redis.Pool{
		MaxIdle:   1,
		MaxActive: 1,
		Dial:      d.dial,
		Wait:      true,
	}
	defer p.Close()
	c, err := p.GetContext(context.Background())
	if err != nil {
		t.Fatalf("GetContext returned %v", err)
	}
	defer c.Close()
}

func TestWaitPoolGetContextIssue520(t *testing.T) {
	d := poolDialer{t: t}
	p := &redis.Pool{
		MaxIdle:   1,
		MaxActive: 1,
		Dial:      d.dial,
		Wait:      true,
	}
	defer p.Close()
	ctx1, cancel1 := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel1()
	c, err := p.GetContext(ctx1)
	if err != context.DeadlineExceeded {
		t.Fatalf("GetContext returned %v", err)
	}
	defer c.Close()

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	c2, err := p.GetContext(ctx2)
	if err != nil {
		t.Fatalf("Get context returned %v", err)
	}
	defer c2.Close()
}

func TestWaitPoolGetContextWithDialContext(t *testing.T) {
	d := poolDialer{t: t}
	p := &redis.Pool{
		MaxIdle:     1,
		MaxActive:   1,
		DialContext: d.dialContext,
		Wait:        true,
	}
	defer p.Close()
	c, err := p.GetContext(context.Background())
	if err != nil {
		t.Fatalf("GetContext returned %v", err)
	}
	defer c.Close()
}

func TestPoolGetContext_DialContext(t *testing.T) {
	var isPassed bool
	f := func(ctx context.Context, network, addr string) (net.Conn, error) {
		isPassed = true
		return &testConn{}, nil
	}

	p := &redis.Pool{
		DialContext: func(ctx context.Context) (redis.Conn, error) {
			return redis.DialContext(ctx, "", "", redis.DialContextFunc(f))
		},
	}
	defer p.Close()

	if _, err := p.GetContext(context.Background()); err != nil {
		t.Fatalf("GetContext returned %v", err)
	}

	if !isPassed {
		t.Fatal("DialContextFunc not passed")
	}
}

func TestPoolGetContext_DialContext_CanceledContext(t *testing.T) {
	addr, err := redis.DefaultServerAddr()
	if err != nil {
		t.Fatalf("redis.DefaultServerAddr returned %v", err)
	}

	p := &redis.Pool{
		DialContext: func(ctx context.Context) (redis.Conn, error) { return redis.DialContext(ctx, "tcp", addr) },
	}
	defer p.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := p.GetContext(ctx); err == nil {
		t.Fatalf("GetContext returned nil, expect error")
	}
}

func TestWaitPoolGetAfterClose(t *testing.T) {
	d := poolDialer{t: t}
	p := &redis.Pool{
		MaxIdle:   1,
		MaxActive: 1,
		Dial:      d.dial,
		Wait:      true,
	}
	p.Close()
	_, err := p.GetContext(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestWaitPoolGetCanceledContext(t *testing.T) {
	t.Run("without vacant connection in the pool", func(t *testing.T) {
		d := poolDialer{t: t}
		p := &redis.Pool{
			MaxIdle:   1,
			MaxActive: 1,
			Dial:      d.dial,
			Wait:      true,
		}
		defer p.Close()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		c := p.Get()
		defer c.Close()
		_, err := p.GetContext(ctx)
		if err != context.Canceled {
			t.Fatalf("got error %v, want %v", err, context.Canceled)
		}
	})
	t.Run("with vacant connection in the pool", func(t *testing.T) {
		d := poolDialer{t: t}
		p := &redis.Pool{
			MaxIdle:   1,
			MaxActive: 1,
			Dial:      d.dial,
			Wait:      true,
		}
		defer p.Close()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := p.GetContext(ctx)
		if err != context.Canceled {
			t.Fatalf("got error %v, want %v", err, context.Canceled)
		}
	})
}
