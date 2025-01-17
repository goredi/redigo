package redis

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type stateUpdateTest struct {
	state       connectionState
	expected    connectionState
	commandName string
	args        []interface{}
	info        map[string]*commandAction
}

func buildStateUpdateTests(tests map[string]stateUpdateTest, root, infos map[string]*commandAction, args ...string) {
	for cmd, ci := range infos {
		if cmd != strings.ToLower(cmd) {
			// Skip non lower case commands as we only need to test one case per action.
			continue
		}

		cmdArgs := append(args, cmd)
		if ci.Next != nil {
			buildStateUpdateTests(tests, root, ci.Next, cmdArgs...)
			continue
		}

		t := stateUpdateTest{
			commandName: cmdArgs[0],
			args:        make([]interface{}, len(cmdArgs)-1),
			info:        root,
		}
		for i, arg := range cmdArgs[1:] {
			t.args[i] = arg
		}
		var state connectionState
		ci.Action(&state)
		if state != 0 {
			// Action is a Set.
			t.expected = state
		} else {
			// Action is a Clear.
			state = ^connectionState(0)
			ci.Action(&state)
			t.state = ^state
		}

		tests[strings.Join(cmdArgs, "-")] = t
	}
}

func TestConnectionStateUpdate(t *testing.T) {
	tests := make(map[string]stateUpdateTest)
	buildStateUpdateTests(tests, activeConnActions, activeConnActions)
	buildStateUpdateTests(tests, connActions, connActions)

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			tc.state.update(tc.info, true, tc.commandName, tc.args...)
			require.Equal(t, tc.expected, tc.state)
		})
	}
}

func benchmarkStateUpdate(b *testing.B, names ...string) {
	var state connectionState
	for i := 0; i < b.N; i++ {
		for _, cmd := range names {
			state.update(activeConnActions, true, cmd)
		}
	}
}

func BenchmarkConnectionStateUpdateCorrectCase(b *testing.B) {
	benchmarkStateUpdate(b, "watch", "WATCH", "monitor", "MONITOR")
}

func BenchmarkConnectionStateUpdateMixedCase(b *testing.B) {
	benchmarkStateUpdate(b, "wAtch", "WeTCH", "monItor", "MONiTOR")
}

func BenchmarkConnectionStateUpdateNoMatch(b *testing.B) {
	benchmarkStateUpdate(b, "GET", "SET", "HMGET", "HMSET")
}

func TestClientReply(t *testing.T) {
	p := &Pool{
		MaxIdle:   1,
		MaxActive: 2,
		Dial: func() (Conn, error) {
			return DialDefaultServer()
		},
	}
	defer p.Close()

	closeCheck := func(t *testing.T, c Conn) {
		t.Helper()

		done := make(chan error)
		go func() {
			done <- c.Close()
		}()

		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for connection close")
		}
	}

	var defaultState connectionState
	tests := map[string]func(t *testing.T){
		"off": func(t *testing.T) {
			c := p.Get()
			defer closeCheck(t, c)

			err := c.Send("CLIENT", "REPLY", "OFF")
			require.NoError(t, err)
			require.Equal(t, stateClientReplyOff, c.(*activeConn).state)

			err = c.Send("ECHO", "first")
			require.NoError(t, err)
			require.Equal(t, stateClientReplyOff, c.(*activeConn).state)

			_, err = c.Do("CLIENT", "REPLY", "ON")
			require.NoError(t, err)
			require.Equal(t, defaultState, c.(*activeConn).state)

			reply, err := String(c.Do("ECHO", "second"))
			require.NoError(t, err)
			require.Equal(t, "second", reply)
		},
		"off-skip": func(t *testing.T) {
			c := p.Get()
			defer closeCheck(t, c)

			err := c.Send("CLIENT", "REPLY", "OFF")
			require.NoError(t, err)
			require.Equal(t, stateClientReplyOff, c.(*activeConn).state)

			err = c.Send("ECHO", "first")
			require.NoError(t, err)
			require.Equal(t, stateClientReplyOff, c.(*activeConn).state)

			// Skip should be ignored as we are already in a off state.
			err = c.Send("CLIENT", "REPLY", "SKIP")
			require.NoError(t, err)
			require.Equal(t, stateClientReplyOff, c.(*activeConn).state)

			_, err = c.Do("CLIENT", "REPLY", "ON")
			require.NoError(t, err)
			require.Equal(t, defaultState, c.(*activeConn).state)

			reply, err := String(c.Do("ECHO", "second"))
			require.NoError(t, err)
			require.Equal(t, "second", reply)
		},
		"skip": func(t *testing.T) {
			c := p.Get()
			defer closeCheck(t, c)

			err := c.Send("CLIENT", "REPLY", "SKIP")
			require.NoError(t, err)
			require.Equal(t, stateClientReplySkipNext, c.(*activeConn).state)

			err = c.Send("ECHO", "first")
			require.NoError(t, err)
			require.Equal(t, stateClientReplySkip, c.(*activeConn).state)

			err = c.Send("ECHO", "second")
			require.NoError(t, err)
			require.Equal(t, defaultState, c.(*activeConn).state)

			err = c.Flush()
			require.NoError(t, err)

			reply, err := String(c.Receive())
			require.NoError(t, err)
			require.Equal(t, "second", reply)

			reply, err = String(c.Do("ECHO", "third"))
			require.NoError(t, err)
			require.Equal(t, "third", reply)
		},
	}

	for name, tc := range tests {
		t.Run(name, tc)
	}
}

// setAllFlags sets all flags in the state.
func setAllFlags(state *connectionState, infos map[string]*commandAction) {
	for _, ci := range infos {
		if ci.Next != nil {
			setAllFlags(state, ci.Next)
		}

		if ci.Action != nil {
			var check connectionState
			ci.Action(&check)
			if check != 0 {
				*state |= check
			}
		}
	}
}

// isUnsupported returns true if err is an unknown or disabled command error, false otherwise.
func isUnsupported(err error) bool {
	if err != nil {
		errStr := strings.ToLower(err.Error())
		if strings.HasPrefix(errStr, "err unknown command") ||
			strings.HasPrefix(errStr, "err unknown subcommand") ||
			strings.HasPrefix(errStr, "err this instance has cluster support disabled") {
			return true
		}
	}

	return false
}

// checkSupported checks if a command is supported and if it is, it
// sends it on c otherwise it removes flags from the expected state.
func checkSupported(t *testing.T, p *Pool, c Conn, expected *connectionState, flags connectionState, cmd string, args ...interface{}) {
	t.Helper()

	c2 := p.Get()
	defer c2.Close() //nolint: errcheck

	_, err := c2.Do(cmd, args...)
	if isUnsupported(err) {
		*expected &^= flags
		c2.(*activeConn).state &^= flags
		return
	}

	require.NoError(t, err)
	require.NoError(t, c.Send(cmd, args...))
}

type closeTest struct {
	expected    connectionState
	commandName string
	args        []interface{}
}

// buildCloseTests builds a map of close tests for the given command infos.
func buildCloseTests(tests map[string]closeTest, root, infos map[string]*commandAction, args ...string) {
	for cmd, ci := range infos {
		if cmd != strings.ToLower(cmd) {
			// Skip non lower case commands as we only need to test one case per action.
			continue
		}

		if cmd == "monitor" {
			// MONITOR is tested separately.
			continue
		}

		cmdArgs := append(args, cmd)
		if ci.Next != nil {
			buildCloseTests(tests, root, ci.Next, cmdArgs...)
			continue
		}

		var state connectionState
		ci.Action(&state)
		if state == 0 {
			// Action is a Clear so not needed.
			continue
		}

		// Action is a Set.
		t := closeTest{
			commandName: cmdArgs[0],
			args:        make([]interface{}, len(cmdArgs)-1),
			expected:    state,
		}
		for i, arg := range cmdArgs[1:] {
			t.args[i] = arg
		}
		switch t.commandName {
		case "watch", "psubscribe", "subscribe", "ssubscribe":
			t.args = append(t.args, "key")
		}

		tests[strings.Join(cmdArgs, "-")] = t
	}
}

func TestPoolCloseCleanup(t *testing.T) {
	p := &Pool{
		Dial: func() (Conn, error) {
			return DialDefaultServer()
		},
	}
	defer p.Close()

	var expected connectionState
	setAllFlags(&expected, activeConnActions)
	for _, val := range []string{"off", "skip"} {
		t.Run("all-client-reply-"+val, func(t *testing.T) {
			c := p.Get()
			// Apply all state changing commands.
			checkSupported(t, p, c, &expected, stateClientNoEvict, "CLIENT", "NO-EVICT", "ON")
			checkSupported(t, p, c, &expected, stateClientNoTouch, "CLIENT", "NO-TOUCH", "ON")
			// CLIENT REPLY OFF and CLIENT REPLY SKIP / SKIP NEXT are mutually exclusive.
			switch val {
			case "off":
				checkSupported(t, p, c, &expected, stateClientReplyOff, "CLIENT", "REPLY", "OFF")
				checkSupported(t, p, c, &expected, 0, "CLIENT", "REPLY", "SKIP") // Should be ignored.
				// CLIENT REPLY SKIP / SKIP NEXT was never set.
				expected &^= stateClientReplySkip | stateClientReplySkipNext
			case "skip":
				checkSupported(t, p, c, &expected, stateClientReplySkip|stateClientReplySkipNext, "CLIENT", "REPLY", "SKIP")
				// CLIENT REPLY SKIP / SKIP NEXT should be unset by the next command and CLIENT REPLY OFF was never set.
				expected &^= stateClientReplyOff | stateClientReplySkip | stateClientReplySkipNext
			}
			checkSupported(t, p, c, &expected, stateClientTracking, "CLIENT", "TRACKING", "ON")
			require.Zero(t, c.(*activeConn).state&(stateClientReplySkipNext)) // Skip next should have been cleared.
			// MONITOR isn't tested as it can't be cleared during close without using RESET
			// which isn't possible to use as we can't ensure that the dialled settings such
			// as selected database and AUTH are restored.
			expected &^= stateMonitor
			checkSupported(t, p, c, &expected, stateWatch, "WATCH", "key") // Out of order as WATCH isn't allowed in side a subscriptions.
			require.Zero(t, c.(*activeConn).state&(stateClientReplySkip))  // Skip should should have cleared.
			checkSupported(t, p, c, &expected, stateMulti, "MULTI")        // Out of order as MULTI isn't allowed in side a subscriptions.
			checkSupported(t, p, c, &expected, statePsubscribe, "PSUBSCRIBE", "x")
			checkSupported(t, p, c, &expected, stateReadOnly, "READONLY")
			checkSupported(t, p, c, &expected, stateSsubscribe, "SSUBSCRIBE", "x")
			checkSupported(t, p, c, &expected, stateSubscribe, "SUBSCRIBE", "x")
			require.Equal(t, expected, c.(*activeConn).state)
			require.NoError(t, c.Close())
			require.Zero(t, c.(*activeConn).state)
		})
	}

	tests := make(map[string]closeTest)
	buildCloseTests(tests, activeConnActions, activeConnActions)

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			c := p.Get()
			_, err := c.Do(tc.commandName, tc.args...)
			if isUnsupported(err) {
				t.Skipf("Command %q not supported", tc.commandName)
			}
			require.NoError(t, err)

			require.Equal(t, tc.expected, c.(*activeConn).state)
			require.NoError(t, c.Close())
			require.Zero(t, c.(*activeConn).state)
		})
	}

	t.Run("monitor", func(t *testing.T) {
		c := p.Get()
		_, err := c.Do("MONITOR")
		require.NoError(t, err)
		require.ErrorIs(t, c.Close(), errMonitorEnabled)
	})
}
