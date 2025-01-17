// Copyright 2012 Gary Burd
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
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/require"
)

type testConn struct {
	io.Reader
	io.Writer
	readDeadline  time.Time
	writeDeadline time.Time
}

func (*testConn) Close() error         { return nil }
func (*testConn) LocalAddr() net.Addr  { return nil }
func (*testConn) RemoteAddr() net.Addr { return nil }
func (c *testConn) SetDeadline(t time.Time) error {
	c.readDeadline = t
	c.writeDeadline = t
	return nil
}
func (c *testConn) SetReadDeadline(t time.Time) error  { c.readDeadline = t; return nil }
func (c *testConn) SetWriteDeadline(t time.Time) error { c.writeDeadline = t; return nil }

func dialTestConn(r string, w io.Writer) redis.DialOption {
	return redis.DialNetDial(func(network, addr string) (net.Conn, error) {
		return &testConn{Reader: strings.NewReader(r), Writer: w}, nil
	})
}

type tlsTestConn struct {
	net.Conn
	done chan struct{}
}

func (c *tlsTestConn) Close() error {
	c.Conn.Close()
	<-c.done
	return nil
}

func dialTestConnTLS(r string, w io.Writer) redis.DialOption {
	return redis.DialNetDial(func(network, addr string) (net.Conn, error) {
		client, server := net.Pipe()
		tlsServer := tls.Server(server, &serverTLSConfig)
		go io.Copy(tlsServer, strings.NewReader(r)) // nolint: errcheck
		done := make(chan struct{})
		go func() {
			io.Copy(w, tlsServer) // nolint: errcheck
			close(done)
		}()
		return &tlsTestConn{Conn: client, done: done}, nil
	})
}

type durationArg struct {
	time.Duration
}

func (t durationArg) RedisArg() interface{} {
	return t.Seconds()
}

type recursiveArg int

func (v recursiveArg) RedisArg() interface{} { return v }

var writeTests = []struct {
	args     []interface{}
	expected string
}{
	{
		[]interface{}{"SET", "key", "value"},
		"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
	},
	{
		[]interface{}{"SET", "key", "value"},
		"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n",
	},
	{
		[]interface{}{"SET", "key", byte(100)},
		"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\n100\r\n",
	},
	{
		[]interface{}{"SET", "key", 100},
		"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\n100\r\n",
	},
	{
		[]interface{}{"SET", "key", int64(math.MinInt64)},
		"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$20\r\n-9223372036854775808\r\n",
	},
	{
		[]interface{}{"SET", "key", float64(1349673917.939762)},
		"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$21\r\n1.349673917939762e+09\r\n",
	},
	{
		[]interface{}{"SET", "key", ""},
		"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$0\r\n\r\n",
	},
	{
		[]interface{}{"SET", "key", nil},
		"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$0\r\n\r\n",
	},
	{
		[]interface{}{"SET", "key", durationArg{time.Minute}},
		"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$2\r\n60\r\n",
	},
	{
		[]interface{}{"SET", "key", recursiveArg(123)},
		"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\n123\r\n",
	},
	{
		[]interface{}{"ECHO", true, false},
		"*3\r\n$4\r\nECHO\r\n$1\r\n1\r\n$1\r\n0\r\n",
	},
}

func TestWrite(t *testing.T) {
	for _, tt := range writeTests {
		var buf bytes.Buffer
		c, _ := redis.Dial("", "", dialTestConn("", &buf))
		err := c.Send(tt.args[0].(string), tt.args[1:]...)
		if err != nil {
			t.Errorf("Send(%v) returned error %v", tt.args, err)
			continue
		}
		c.Flush()
		actual := buf.String()
		if actual != tt.expected {
			t.Errorf("Send(%v) = %q, want %q", tt.args, actual, tt.expected)
		}
	}
}

var errorSentinel = &struct{}{}

var readTests = []struct {
	reply    string
	expected interface{}
}{
	{
		"+OK\r\n",
		"OK",
	},
	{
		"+PONG\r\n",
		"PONG",
	},
	{
		"+OK\n\n", // no \r
		errorSentinel,
	},
	{
		"@OK\r\n",
		errorSentinel,
	},
	{
		"$6\r\nfoobar\r\n",
		[]byte("foobar"),
	},
	{
		"$-1\r\n",
		nil,
	},
	{
		":1\r\n",
		int64(1),
	},
	{
		":-2\r\n",
		int64(-2),
	},
	{
		"*0\r\n",
		[]interface{}{},
	},
	{
		"*-1\r\n",
		nil,
	},
	{
		"*4\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$5\r\nHello\r\n$5\r\nWorld\r\n",
		[]interface{}{[]byte("foo"), []byte("bar"), []byte("Hello"), []byte("World")},
	},
	{
		"*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n",
		[]interface{}{[]byte("foo"), nil, []byte("bar")},
	},

	{
		// "" is not a valid length
		"$\r\nfoobar\r\n",
		errorSentinel,
	},
	{
		// "x" is not a valid length
		"$x\r\nfoobar\r\n",
		errorSentinel,
	},
	{
		// -2 is not a valid length
		"$-2\r\n",
		errorSentinel,
	},
	{
		// ""  is not a valid integer
		":\r\n",
		errorSentinel,
	},
	{
		// "x"  is not a valid integer
		":x\r\n",
		errorSentinel,
	},
	{
		// missing \r\n following value
		"$6\r\nfoobar",
		errorSentinel,
	},
	{
		// short value
		"$6\r\nxx",
		errorSentinel,
	},
	{
		// long value
		"$6\r\nfoobarx\r\n",
		errorSentinel,
	},
}

func TestRead(t *testing.T) {
	for _, tt := range readTests {
		c, _ := redis.Dial("", "", dialTestConn(tt.reply, nil))
		actual, err := c.Receive()
		if tt.expected == errorSentinel {
			if err == nil {
				t.Errorf("Receive(%q) did not return expected error", tt.reply)
			}
		} else {
			if err != nil {
				t.Errorf("Receive(%q) returned error %v", tt.reply, err)
				continue
			}
			if !reflect.DeepEqual(actual, tt.expected) {
				t.Errorf("Receive(%q) = %v, want %v", tt.reply, actual, tt.expected)
			}
		}
	}
}

func TestReadString(t *testing.T) {
	// n is value of bufio.defaultBufSize
	const n = 4096

	// Test read string lengths near bufio.Reader buffer boundaries.
	testRanges := [][2]int{{0, 64}, {n - 64, n + 64}, {2*n - 64, 2*n + 64}}

	p := make([]byte, 2*n+64)
	for i := range p {
		p[i] = byte('a' + i%26)
	}
	s := string(p)

	for _, r := range testRanges {
		for i := r[0]; i < r[1]; i++ {
			c, _ := redis.Dial("", "", dialTestConn("+"+s[:i]+"\r\n", nil))
			actual, err := c.Receive()
			if err != nil || actual != s[:i] {
				t.Fatalf("Receive(string len %d) -> err=%v, equal=%v", i, err, actual != s[:i])
			}
		}
	}
}

var testCommands = []struct {
	args     []interface{}
	expected interface{}
}{
	{
		[]interface{}{"PING"},
		"PONG",
	},
	{
		[]interface{}{"SET", "foo", "bar"},
		"OK",
	},
	{
		[]interface{}{"GET", "foo"},
		[]byte("bar"),
	},
	{
		[]interface{}{"GET", "nokey"},
		nil,
	},
	{
		[]interface{}{"MGET", "nokey", "foo"},
		[]interface{}{nil, []byte("bar")},
	},
	{
		[]interface{}{"INCR", "mycounter"},
		int64(1),
	},
	{
		[]interface{}{"LPUSH", "mylist", "foo"},
		int64(1),
	},
	{
		[]interface{}{"LPUSH", "mylist", "bar"},
		int64(2),
	},
	{
		[]interface{}{"LRANGE", "mylist", 0, -1},
		[]interface{}{[]byte("bar"), []byte("foo")},
	},
	{
		[]interface{}{"MULTI"},
		"OK",
	},
	{
		[]interface{}{"LRANGE", "mylist", 0, -1},
		"QUEUED",
	},
	{
		[]interface{}{"PING"},
		"QUEUED",
	},
	{
		[]interface{}{"EXEC"},
		[]interface{}{
			[]interface{}{[]byte("bar"), []byte("foo")},
			"PONG",
		},
	},
}

func TestDoCommands(t *testing.T) {
	c, err := redis.DialDefaultServer()
	if err != nil {
		t.Fatalf("error connection to database, %v", err)
	}
	defer c.Close()

	for _, cmd := range testCommands {
		actual, err := c.Do(cmd.args[0].(string), cmd.args[1:]...)
		if err != nil {
			t.Errorf("Do(%v) returned error %v", cmd.args, err)
			continue
		}
		if !reflect.DeepEqual(actual, cmd.expected) {
			t.Errorf("Do(%v) = %v, want %v", cmd.args, actual, cmd.expected)
		}
	}
}

func TestPipelineCommands(t *testing.T) {
	c, err := redis.DialDefaultServer()
	if err != nil {
		t.Fatalf("error connection to database, %v", err)
	}
	defer c.Close()

	for _, cmd := range testCommands {
		if err := c.Send(cmd.args[0].(string), cmd.args[1:]...); err != nil {
			t.Fatalf("Send(%v) returned error %v", cmd.args, err)
		}
	}
	if err := c.Flush(); err != nil {
		t.Errorf("Flush() returned error %v", err)
	}
	for _, cmd := range testCommands {
		actual, err := c.Receive()
		if err != nil {
			t.Fatalf("Receive(%v) returned error %v", cmd.args, err)
		}
		if !reflect.DeepEqual(actual, cmd.expected) {
			t.Errorf("Receive(%v) = %v, want %v", cmd.args, actual, cmd.expected)
		}
	}
}

func TestBlankCommand(t *testing.T) {
	c, err := redis.DialDefaultServer()
	if err != nil {
		t.Fatalf("error connection to database, %v", err)
	}
	defer c.Close()

	for _, cmd := range testCommands {
		if err := c.Send(cmd.args[0].(string), cmd.args[1:]...); err != nil {
			t.Fatalf("Send(%v) returned error %v", cmd.args, err)
		}
	}
	reply, err := redis.Values(c.Do(""))
	if err != nil {
		t.Fatalf("Do() returned error %v", err)
	}
	if len(reply) != len(testCommands) {
		t.Fatalf("len(reply)=%d, want %d", len(reply), len(testCommands))
	}
	for i, cmd := range testCommands {
		actual := reply[i]
		if !reflect.DeepEqual(actual, cmd.expected) {
			t.Errorf("Receive(%v) = %v, want %v", cmd.args, actual, cmd.expected)
		}
	}
}

func TestRecvBeforeSend(t *testing.T) {
	c, err := redis.DialDefaultServer()
	if err != nil {
		t.Fatalf("error connection to database, %v", err)
	}
	defer c.Close()
	done := make(chan struct{})
	go func() {
		c.Receive() // nolint: errcheck
		close(done)
	}()
	time.Sleep(time.Millisecond)
	require.NoError(t, c.Send("PING"))
	require.NoError(t, c.Flush())
	<-done
	_, err = c.Do("")
	if err != nil {
		t.Fatalf("error=%v", err)
	}
}

func TestError(t *testing.T) {
	c, err := redis.DialDefaultServer()
	if err != nil {
		t.Fatalf("error connection to database, %v", err)
	}
	defer c.Close()

	_, err = c.Do("SET", "key", "val")
	require.NoError(t, err)
	_, err = c.Do("HSET", "key", "fld", "val")
	if err == nil {
		t.Errorf("Expected err for HSET on string key.")
	}
	if c.Err() != nil {
		t.Errorf("Conn has Err()=%v, expect nil", c.Err())
	}
	_, err = c.Do("SET", "key", "val")
	if err != nil {
		t.Errorf("Do(SET, key, val) returned error %v, expected nil.", err)
	}
}

func TestReadTimeout(t *testing.T) {
	done := make(chan struct{})
	errs := make(chan error, 2)
	defer func() {
		close(done)
		for err := range errs {
			require.NoError(t, err)
		}
	}()

	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
		close(errs)
	}()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen returned %v", err)
	}
	defer l.Close()

	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()

				to := time.NewTimer(time.Second)
				defer to.Stop()
				select {
				case <-to.C:
				case <-done:
					return
				}
				_, err := c.Write([]byte("+OK\r\n"))
				errs <- err
				c.Close()
			}()
		}
	}()

	// Do

	c1, err := redis.Dial(l.Addr().Network(), l.Addr().String(), redis.DialReadTimeout(time.Millisecond))
	if err != nil {
		t.Fatalf("redis.Dial returned %v", err)
	}
	defer c1.Close()

	_, err = c1.Do("PING")
	if err == nil {
		t.Fatalf("c1.Do() returned nil, expect error")
	}
	if c1.Err() == nil {
		t.Fatalf("c1.Err() = nil, expect error")
	}

	// Send/Flush/Receive

	c2, err := redis.Dial(l.Addr().Network(), l.Addr().String(), redis.DialReadTimeout(time.Millisecond))
	if err != nil {
		t.Fatalf("redis.Dial returned %v", err)
	}
	defer c2.Close()

	require.NoError(t, c2.Send("PING"))
	require.NoError(t, c2.Flush())
	_, err = c2.Receive()
	if err == nil {
		t.Fatalf("c2.Receive() returned nil, expect error")
	}
	if c2.Err() == nil {
		t.Fatalf("c2.Err() = nil, expect error")
	}
}

func TestDialContextFunc(t *testing.T) {
	var isPassed bool
	f := func(ctx context.Context, network, addr string) (net.Conn, error) {
		isPassed = true
		return &testConn{}, nil
	}

	_, err := redis.DialContext(context.Background(), "", "", redis.DialContextFunc(f))
	if err != nil {
		t.Fatalf("DialContext returned %v", err)
	}

	if !isPassed {
		t.Fatal("DialContextFunc not passed")
	}
}

func TestDialContext_CanceledContext(t *testing.T) {
	addr, err := redis.DefaultServerAddr()
	if err != nil {
		t.Fatalf("redis.DefaultServerAddr returned %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err = redis.DialContext(ctx, "tcp", addr); err == nil {
		t.Fatalf("DialContext returned nil, expect error")
	}
}

var dialErrors = []struct {
	rawurl        string
	expectedError string
}{
	{
		"localhost",
		"invalid redis URL scheme",
	},
	// The error message for invalid hosts is different in different
	// versions of Go, so just check that there is an error message.
	{
		"redis://weird url",
		"",
	},
	{
		"redis://foo:bar:baz",
		"",
	},
	{
		"http://www.google.com",
		"invalid redis URL scheme: http",
	},
	{
		"redis://localhost:6379/abc123",
		"invalid database: abc123",
	},
	{
		"redis:foo//localhost:6379",
		"invalid redis URL, url is opaque: redis:foo//localhost:6379",
	},
}

func TestDialURLErrors(t *testing.T) {
	for _, d := range dialErrors {
		_, err := redis.DialURL(d.rawurl)
		if err == nil || !strings.Contains(err.Error(), d.expectedError) {
			t.Errorf("DialURL did not return expected error (expected %v to contain %s)", err, d.expectedError)
		}
	}
}

func TestDialURLPort(t *testing.T) {
	checkPort := func(network, address string) (net.Conn, error) {
		if address != "localhost:6379" {
			t.Errorf("DialURL did not set port to 6379 by default (got %v)", address)
		}
		return nil, nil
	}
	_, err := redis.DialURL("redis://localhost", redis.DialNetDial(checkPort))
	if err != nil {
		t.Error("dial error:", err)
	}
}

func TestDialURLHost(t *testing.T) {
	checkHost := func(network, address string) (net.Conn, error) {
		if address != "localhost:6379" {
			t.Errorf("DialURL did not set host to localhost by default (got %v)", address)
		}
		return nil, nil
	}
	_, err := redis.DialURL("redis://:6379", redis.DialNetDial(checkHost))
	if err != nil {
		t.Error("dial error:", err)
	}
}

var workingDirectory, _ = os.Getwd()

var dialURLTests = []struct {
	description string
	url         string
	r           string
	w           string
}{
	{"password", "redis://:abc123@localhost", "+OK\r\n", "*2\r\n$4\r\nAUTH\r\n$6\r\nabc123\r\n"},
	{"password redis-cli compat", "redis://abc123@localhost", "+OK\r\n", "*2\r\n$4\r\nAUTH\r\n$6\r\nabc123\r\n"},
	{"password db1", "redis://:abc123@localhost/1", "+OK\r\n+OK\r\n", "*2\r\n$4\r\nAUTH\r\n$6\r\nabc123\r\n*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n"},
	{"password db1 redis-cli compat", "redis://abc123@localhost/1", "+OK\r\n+OK\r\n", "*2\r\n$4\r\nAUTH\r\n$6\r\nabc123\r\n*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n"},
	{"password no host db0", "redis://:abc123@/0", "+OK\r\n+OK\r\n", "*2\r\n$4\r\nAUTH\r\n$6\r\nabc123\r\n"},
	{"password no host db0 redis-cli compat", "redis://abc123@/0", "+OK\r\n+OK\r\n", "*2\r\n$4\r\nAUTH\r\n$6\r\nabc123\r\n"},
	{"password no host db1", "redis://:abc123@/1", "+OK\r\n+OK\r\n", "*2\r\n$4\r\nAUTH\r\n$6\r\nabc123\r\n*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n"},
	{"password no host db1 redis-cli compat", "redis://abc123@/1", "+OK\r\n+OK\r\n", "*2\r\n$4\r\nAUTH\r\n$6\r\nabc123\r\n*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n"},
	{"username and password", "redis://user:password@localhost", "+OK\r\n", "*3\r\n$4\r\nAUTH\r\n$4\r\nuser\r\n$8\r\npassword\r\n"},
	{"username", "redis://x:@localhost", "+OK\r\n", ""},
	{"database 3", "redis://localhost/3", "+OK\r\n", "*2\r\n$6\r\nSELECT\r\n$1\r\n3\r\n"},
	{"database 99", "redis://localhost/99", "+OK\r\n", "*2\r\n$6\r\nSELECT\r\n$2\r\n99\r\n"},
	{"no database", "redis://localhost/", "+OK\r\n", ""},
	{"absolute socket path", "redis+unix://" + filepath.Join(workingDirectory, "server.sock"), "+OK\r\n", ""},
	{"relative socket path", "redis+unix://./server.sock", "+OK\r\n", ""},
	{"unix socket path database 99", "redis+unix://./server.sock?db=99", "+OK\r\n", "*2\r\n$6\r\nSELECT\r\n$2\r\n99\r\n"},
}

func TestDialURL(t *testing.T) {
	for _, tt := range dialURLTests {
		t.Run(tt.description, func(t *testing.T) {
			var buf bytes.Buffer
			// UseTLS should be ignored in all of these tests.
			_, err := redis.DialURL(tt.url, dialTestConn(tt.r, &buf), redis.DialUseTLS(true))
			if err != nil {
				t.Errorf("%s dial error: %v, buf: %v", tt.description, err, buf.String())
				return
			}
			if w := buf.String(); w != tt.w {
				t.Errorf("%s commands = %q, want %q", tt.description, w, tt.w)
			}
		})
	}
}

func checkPingPong(t *testing.T, buf *bytes.Buffer, c redis.Conn) {
	resp, err := c.Do("PING")
	if err != nil {
		t.Fatal("ping error:", err)
	}
	// Close connection to ensure that writes to buf are complete.
	c.Close()
	expected := "*1\r\n$4\r\nPING\r\n"
	actual := buf.String()
	if actual != expected {
		t.Errorf("commands = %q, want %q", actual, expected)
	}
	if resp != "PONG" {
		t.Errorf("resp = %v, want %v", resp, "PONG")
	}
}

const pingResponse = "+PONG\r\n"

func TestDialURLTLS(t *testing.T) {
	var buf bytes.Buffer
	c, err := redis.DialURL("rediss://example.com/",
		redis.DialTLSConfig(&clientTLSConfig),
		dialTestConnTLS(pingResponse, &buf))
	if err != nil {
		t.Fatal("dial error:", err)
	}
	checkPingPong(t, &buf, c)
}

func TestDialUseTLS(t *testing.T) {
	var buf bytes.Buffer
	c, err := redis.Dial("tcp", "example.com:6379",
		redis.DialTLSConfig(&clientTLSConfig),
		dialTestConnTLS(pingResponse, &buf),
		redis.DialUseTLS(true))
	if err != nil {
		t.Fatal("dial error:", err)
	}
	checkPingPong(t, &buf, c)
}

type blockedReader struct {
	ch chan struct{}
}

func (b blockedReader) Read(p []byte) (n int, err error) {
	<-b.ch
	return 0, io.EOF
}

func dialTestBlockedConn(ch chan struct{}, w io.Writer) redis.DialOption {
	return redis.DialNetDial(func(network, addr string) (net.Conn, error) {
		return &testConn{Reader: blockedReader{ch: ch}, Writer: w}, nil
	})
}

func TestDialTLSHandshakeTimeout(t *testing.T) {
	var buf bytes.Buffer
	ch := make(chan struct{})
	var err error
	go func() {
		_, err = redis.Dial("tcp", "example.com:6379",
			redis.DialTLSConfig(&clientTLSConfig),
			redis.DialTLSHandshakeTimeout(time.Millisecond),
			dialTestBlockedConn(ch, &buf),
			redis.DialUseTLS(true))
		close(ch)
	}()
	select {
	case <-time.After(time.Second):
		t.Fatal("dial didn't timeout")
	case <-ch:
		if err == nil {
			t.Fatal("dial didn't error")
		} else if err.Error() != "TLS handshake timeout" {
			t.Fatal("dial unexpected error:", err)
		}
	}
}

func TestDialTLSSKipVerify(t *testing.T) {
	var buf bytes.Buffer
	c, err := redis.Dial("tcp", "example.com:6379",
		dialTestConnTLS(pingResponse, &buf),
		redis.DialTLSSkipVerify(true),
		redis.DialUseTLS(true))
	if err != nil {
		t.Fatal("dial error:", err)
	}
	checkPingPong(t, &buf, c)
}

func TestDialUseACL(t *testing.T) {
	var buf bytes.Buffer
	_, err := redis.Dial("tcp", "localhost:6379",
		redis.DialUsername("user"),
		redis.DialPassword("password"),
		dialTestConn(pingResponse, &buf))
	if err != nil {
		t.Fatal("dial error:", err)
	}
	if err != nil {
		t.Fatal("dial error:", err)
	}
	expected := "*3\r\n$4\r\nAUTH\r\n$4\r\nuser\r\n$8\r\npassword\r\n"
	if w := buf.String(); w != expected {
		t.Errorf("got %q, want %q", w, expected)
	}
}

// Connect to an Redis instance using the Redis ACL system
func ExampleDial_acl() {
	c, err := redis.Dial("tcp", "localhost:6379",
		redis.DialUsername("username"),
		redis.DialPassword("password"),
	)
	if err != nil {
		// handle error
	}
	defer c.Close()
}

func TestDialClientName(t *testing.T) {
	var buf bytes.Buffer
	_, err := redis.Dial("tcp", ":6379",
		dialTestConn(pingResponse, &buf),
		redis.DialClientName("redis-connection"),
	)
	if err != nil {
		t.Fatal("dial error:", err)
	}
	expected := "*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$16\r\nredis-connection\r\n"
	if w := buf.String(); w != expected {
		t.Errorf("got %q, want %q", w, expected)
	}

	// testing against a real server
	connectionName := "test-connection"
	c, err := redis.DialDefaultServer(redis.DialClientName(connectionName))
	if err != nil {
		t.Fatalf("error connection to database, %v", err)
	}
	defer c.Close()

	v, err := c.Do("CLIENT", "GETNAME")
	if err != nil {
		t.Fatalf("CLIENT GETNAME returned error %v", err)
	}

	vs, err := redis.String(v, nil)
	if err != nil {
		t.Fatalf("String(v) returned error %v", err)
	}

	if vs != connectionName {
		t.Fatalf("wrong connection name. Got '%s', expected '%s'", vs, connectionName)
	}
}

// Connect to local instance of Redis running on the default port.
func ExampleDial() {
	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		// handle error
	}
	defer c.Close()
}

// Connect to local instance of Redis running on the default port using the provided context.
func ExampleDialContext() {
	ctx := context.Background()
	c, err := redis.DialContext(ctx, "tcp", ":6379")
	if err != nil {
		// handle error
	}
	defer c.Close()
}

// Connect to remote instance of Redis using a URL.
func ExampleDialURL() {
	c, err := redis.DialURL(os.Getenv("REDIS_URL"))
	if err != nil {
		// handle connection error
	}
	defer c.Close()
}

// TestExecError tests handling of errors in a transaction. See
// http://redis.io/topics/transactions for information on how Redis handles
// errors in a transaction.
func TestExecError(t *testing.T) {
	c, err := redis.DialDefaultServer()
	if err != nil {
		t.Fatalf("error connection to database, %v", err)
	}
	defer c.Close()

	// Execute commands that fail before EXEC is called.

	_, err = c.Do("DEL", "k0")
	require.NoError(t, err)
	_, err = c.Do("ZADD", "k0", 0, 0)
	require.NoError(t, err)
	require.NoError(t, c.Send("MULTI"))
	require.NoError(t, c.Send("NOTACOMMAND", "k0", 0, 0))
	require.NoError(t, c.Send("ZINCRBY", "k0", 0, 0))
	v, err := c.Do("EXEC")
	if err == nil {
		t.Fatalf("EXEC returned values %v, expected error", v)
	}

	// Execute commands that fail after EXEC is called. The first command
	// returns an error.

	_, err = c.Do("DEL", "k1")
	require.NoError(t, err)
	_, err = c.Do("ZADD", "k1", 0, 0)
	require.NoError(t, err)
	require.NoError(t, c.Send("MULTI"))
	require.NoError(t, c.Send("HSET", "k1", 0, 0))
	require.NoError(t, c.Send("ZINCRBY", "k1", 0, 0))
	v, err = c.Do("EXEC")
	if err != nil {
		t.Fatalf("EXEC returned error %v", err)
	}

	vs, err := redis.Values(v, nil)
	if err != nil {
		t.Fatalf("Values(v) returned error %v", err)
	}

	if len(vs) != 2 {
		t.Fatalf("len(vs) == %d, want 2", len(vs))
	}

	if _, ok := vs[0].(error); !ok {
		t.Fatalf("first result is type %T, expected error", vs[0])
	}

	if _, ok := vs[1].([]byte); !ok {
		t.Fatalf("second result is type %T, expected []byte", vs[1])
	}

	// Execute commands that fail after EXEC is called. The second command
	// returns an error.

	_, err = c.Do("ZADD", "k2", 0, 0)
	require.NoError(t, err)
	require.NoError(t, c.Send("MULTI"))
	require.NoError(t, c.Send("ZINCRBY", "k2", 0, 0))
	require.NoError(t, c.Send("HSET", "k2", 0, 0))
	v, err = c.Do("EXEC")
	if err != nil {
		t.Fatalf("EXEC returned error %v", err)
	}

	vs, err = redis.Values(v, nil)
	if err != nil {
		t.Fatalf("Values(v) returned error %v", err)
	}

	if len(vs) != 2 {
		t.Fatalf("len(vs) == %d, want 2", len(vs))
	}

	if _, ok := vs[0].([]byte); !ok {
		t.Fatalf("first result is type %T, expected []byte", vs[0])
	}

	if _, ok := vs[1].(error); !ok {
		t.Fatalf("second result is type %T, expected error", vs[2])
	}
}

func BenchmarkDoEmpty(b *testing.B) {
	b.StopTimer()
	c, err := redis.DialDefaultServer()
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Do(""); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDoPing(b *testing.B) {
	b.StopTimer()
	c, err := redis.DialDefaultServer()
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.Do("PING"); err != nil {
			b.Fatal(err)
		}
	}
}

var clientTLSConfig, serverTLSConfig tls.Config

func init() {
	// The certificate and key for testing TLS dial options was created
	// using the command
	//
	//   go run GOROOT/src/crypto/tls/generate_cert.go  \
	//      --rsa-bits 1024 \
	//      --host 127.0.0.1,::1,example.com --ca \
	//      --start-date "Jan 1 00:00:00 1970" \
	//      --duration=1000000h
	//
	// where GOROOT is the value of GOROOT reported by go env.
	localhostCert := []byte(`
-----BEGIN CERTIFICATE-----
MIICFDCCAX2gAwIBAgIRAJfBL4CUxkXcdlFurb3K+iowDQYJKoZIhvcNAQELBQAw
EjEQMA4GA1UEChMHQWNtZSBDbzAgFw03MDAxMDEwMDAwMDBaGA8yMDg0MDEyOTE2
MDAwMFowEjEQMA4GA1UEChMHQWNtZSBDbzCBnzANBgkqhkiG9w0BAQEFAAOBjQAw
gYkCgYEArizw8WxMUQ3bGHLeuJ4fDrEpy+L2pqrbYRlKk1DasJ/VkB8bImzIpe6+
LGjiYIxvnDCOJ3f3QplcQuiuMyl6f2irJlJsbFT8Lo/3obnuTKAIaqUdJUqBg6y+
JaL8Auk97FvunfKFv8U1AIhgiLzAfQ/3Eaq1yi87Ra6pMjGbTtcCAwEAAaNoMGYw
DgYDVR0PAQH/BAQDAgKkMBMGA1UdJQQMMAoGCCsGAQUFBwMBMA8GA1UdEwEB/wQF
MAMBAf8wLgYDVR0RBCcwJYILZXhhbXBsZS5jb22HBH8AAAGHEAAAAAAAAAAAAAAA
AAAAAAEwDQYJKoZIhvcNAQELBQADgYEAdZ8daIVkyhVwflt5I19m0oq1TycbGO1+
ach7T6cZiBQeNR/SJtxr/wKPEpmvUgbv2BfFrKJ8QoIHYsbNSURTWSEa02pfw4k9
6RQhij3ZkG79Ituj5OYRORV6Z0HUW32r670BtcuHuAhq7YA6Nxy4FtSt7bAlVdRt
rrKgNsltzMk=
-----END CERTIFICATE-----`)

	localhostKey := []byte(`
-----BEGIN RSA PRIVATE KEY-----
MIICXAIBAAKBgQCuLPDxbExRDdsYct64nh8OsSnL4vamqtthGUqTUNqwn9WQHxsi
bMil7r4saOJgjG+cMI4nd/dCmVxC6K4zKXp/aKsmUmxsVPwuj/ehue5MoAhqpR0l
SoGDrL4lovwC6T3sW+6d8oW/xTUAiGCIvMB9D/cRqrXKLztFrqkyMZtO1wIDAQAB
AoGACrc5G6FOEK6JjDeE/Fa+EmlT6PdNtXNNi+vCas3Opo8u1G8VfEi1D4BgstrB
Eq+RLkrOdB8tVyuYQYWPMhabMqF+hhKJN72j0OwfuPlVvTInwb/cKjo/zbH1IA+Y
HenHNK4ywv7/p/9/MvQPJ3I32cQBCgGUW5chVSH5M1sj5gECQQDabQAI1X0uDqCm
KbX9gXVkAgxkFddrt6LBHt57xujFcqEKFE7nwKhDh7DweVs/VEJ+kpid4z+UnLOw
KjtP9JolAkEAzCNBphQ//IsbH5rNs10wIUw3Ks/Oepicvr6kUFbIv+neRzi1iJHa
m6H7EayK3PWgax6BAsR/t0Jc9XV7r2muSwJAVzN09BHnK+ADGtNEKLTqXMbEk6B0
pDhn7ZmZUOkUPN+Kky+QYM11X6Bob1jDqQDGmymDbGUxGO+GfSofC8inUQJAGfci
Eo3g1a6b9JksMPRZeuLG4ZstGErxJRH6tH1Va5PDwitka8qhk8o2tTjNMO3NSdLH
diKoXBcE2/Pll5pJoQJBAIMiiMIzXJhnN4mX8may44J/HvMlMf2xuVH2gNMwmZuc
Bjqn3yoLHaoZVvbWOi0C2TCN4FjXjaLNZGifQPbIcaA=
-----END RSA PRIVATE KEY-----`)

	cert, err := tls.X509KeyPair(localhostCert, localhostKey)
	if err != nil {
		panic(fmt.Sprintf("error creating key pair: %v", err))
	}
	serverTLSConfig.Certificates = []tls.Certificate{cert}

	certificate, err := x509.ParseCertificate(serverTLSConfig.Certificates[0].Certificate[0])
	if err != nil {
		panic(fmt.Sprintf("error parsing x509 certificate: %v", err))
	}

	clientTLSConfig.RootCAs = x509.NewCertPool()
	clientTLSConfig.RootCAs.AddCert(certificate)
}

func TestWithTimeout(t *testing.T) {
	for _, recv := range []bool{true, false} {
		for _, defaultTimout := range []time.Duration{0, time.Minute} {
			var buf bytes.Buffer
			nc := &testConn{Reader: strings.NewReader("+OK\r\n+OK\r\n+OK\r\n+OK\r\n+OK\r\n+OK\r\n+OK\r\n+OK\r\n+OK\r\n+OK\r\n"), Writer: &buf}
			c, _ := redis.Dial("", "", redis.DialReadTimeout(defaultTimout), redis.DialNetDial(func(network, addr string) (net.Conn, error) { return nc, nil }))
			for i := 0; i < 4; i++ {
				var minDeadline, maxDeadline time.Time

				// Alternate between default and specified timeout.
				var err error
				if i%2 == 0 {
					if defaultTimout != 0 {
						minDeadline = time.Now().Add(defaultTimout)
					}
					if recv {
						_, err = c.Receive()
					} else {
						_, err = c.Do("PING")
					}
					require.NoError(t, err)
					if defaultTimout != 0 {
						maxDeadline = time.Now().Add(defaultTimout)
					}
				} else {
					timeout := 10 * time.Minute
					minDeadline = time.Now().Add(timeout)
					if recv {
						_, err = redis.ReceiveWithTimeout(c, timeout)
					} else {
						_, err = redis.DoWithTimeout(c, timeout, "PING")
					}
					require.NoError(t, err)
					maxDeadline = time.Now().Add(timeout)
				}

				// Expect set deadline in expected range.
				if nc.readDeadline.Before(minDeadline) || nc.readDeadline.After(maxDeadline) {
					t.Errorf("recv %v, %d: do deadline error: %v, %v, %v", recv, i, minDeadline, nc.readDeadline, maxDeadline)
				}
			}
		}
	}
}
